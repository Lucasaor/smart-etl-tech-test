"""Repair Agent — diagnostica e tenta corrigir falhas do pipeline.

Fluxo: get_error → analyze → propose_fix → apply_fix → retry → validate.

Invocado quando o pipeline_agent ou monitor_agent detecta uma falha.
Usa LLM para analisar o erro e propor correções (reexecução, skip, ajuste de config).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, TypedDict

import structlog
from langgraph.graph import END, START, StateGraph

from agents.llm_provider import LLMProvider, LLMProviderError, BudgetExceededError
from agents.tools import data_tools, pipeline_tools, quality_tools
from core.events import emit_agent_action, get_event_bus
from monitoring.models import AgentAction, Alert, AlertSeverity
from monitoring.store import get_monitoring_store

logger = structlog.get_logger(__name__)

AGENT_NAME = "repair_agent"


# ─── Estado do Grafo ────────────────────────────────────────────────────────


class RepairAgentState(TypedDict, total=False):
    """Estado compartilhado entre nós do repair agent."""

    erro_original: str
    camada_falha: str
    run_id: str | None
    analise_erro: dict[str, Any]
    proposta_fix: dict[str, Any]
    fix_aplicado: bool
    resultado_retry: dict[str, Any] | None
    validacao: dict[str, Any] | None
    reparado: bool
    tentativas: int
    max_tentativas: int


# ─── Estratégias de reparo ──────────────────────────────────────────────────

ESTRATEGIAS = {
    "retry": "Reexecutar a camada que falhou sem alterações",
    "skip_layer": "Pular a camada problemática e continuar",
    "retry_from_start": "Reiniciar o pipeline completo",
    "data_quality_fix": "Verificar e corrigir problemas de qualidade nos dados de entrada",
    "no_action": "Sem ação possível — escalar para operador humano",
}


# ─── Nós do Grafo ───────────────────────────────────────────────────────────


def get_error(state: RepairAgentState) -> dict[str, Any]:
    """Coleta informações sobre o erro."""
    logger.info("repair_agent_get_error")

    erro = state.get("erro_original", "Erro desconhecido")
    camada = state.get("camada_falha", "desconhecida")

    # Coletar contexto adicional das tabelas
    contexto: dict[str, Any] = {"erro": erro, "camada": camada}
    try:
        tabelas = data_tools.listar_tabelas()
        contexto["tabelas"] = {
            k: {"exists": v["exists"], "rows": v["rows"]}
            for k, v in tabelas.items()
        }
    except Exception:
        contexto["tabelas"] = {}

    _registrar_acao("get_error", f"Erro na camada {camada}: {erro[:200]}")
    return {"analise_erro": contexto}


def analyze(state: RepairAgentState) -> dict[str, Any]:
    """Analisa o erro e determina a causa raiz.

    Tenta usar LLM para análise inteligente; se não disponível,
    usa heurísticas baseadas na mensagem de erro.
    """
    logger.info("repair_agent_analyze")

    contexto = state.get("analise_erro", {})
    erro = contexto.get("erro", "")
    camada = contexto.get("camada", "")

    # Heurísticas diretas (não precisa de LLM)
    analise = _analisar_com_heuristicas(erro, camada, contexto)

    # Tentar enriquecer com LLM (opcional — não bloqueia se falhar)
    try:
        llm = LLMProvider()
        llm_analise = _analisar_com_llm(llm, erro, camada, contexto)
        if llm_analise:
            analise.update(llm_analise)
    except (LLMProviderError, BudgetExceededError, Exception) as e:
        logger.warning("repair_llm_indisponivel", erro=str(e))

    _registrar_acao(
        "analyze",
        f"Estratégia: {analise.get('estrategia', 'desconhecida')}",
    )
    return {"analise_erro": {**contexto, **analise}}


def propose_fix(state: RepairAgentState) -> dict[str, Any]:
    """Propõe uma correção baseada na análise."""
    logger.info("repair_agent_propose_fix")

    analise = state.get("analise_erro", {})
    estrategia = analise.get("estrategia", "retry")
    camada = analise.get("camada", state.get("camada_falha", ""))

    proposta: dict[str, Any] = {
        "estrategia": estrategia,
        "descricao": ESTRATEGIAS.get(estrategia, "Ação desconhecida"),
        "camada_alvo": camada,
        "parametros": {},
    }

    if estrategia == "retry":
        proposta["parametros"] = {"layers": [camada]}
    elif estrategia == "retry_from_start":
        proposta["parametros"] = {"layers": ["bronze", "silver", "gold"]}
    elif estrategia == "skip_layer":
        # Determinar próximas camadas
        ordem = ["bronze", "silver", "gold"]
        idx = ordem.index(camada) if camada in ordem else -1
        proximas = ordem[idx + 1:] if idx >= 0 else []
        proposta["parametros"] = {"layers": proximas}
    elif estrategia == "data_quality_fix":
        proposta["parametros"] = {"layers": [camada]}

    _registrar_acao("propose_fix", f"Proposta: {estrategia} para {camada}")
    return {"proposta_fix": proposta}


def apply_fix(state: RepairAgentState) -> dict[str, Any]:
    """Aplica a correção proposta."""
    logger.info("repair_agent_apply_fix")

    proposta = state.get("proposta_fix", {})
    estrategia = proposta.get("estrategia", "no_action")
    params = proposta.get("parametros", {})

    if estrategia == "no_action":
        _registrar_acao("apply_fix", "Sem ação possível — escalar")
        _criar_alerta(
            "Reparo impossível",
            f"Erro requer intervenção manual: {state.get('erro_original', '')}",
            AlertSeverity.CRITICAL,
        )
        return {"fix_aplicado": False}

    if estrategia == "skip_layer":
        layers = params.get("layers", [])
        if not layers:
            _registrar_acao("apply_fix", "Nenhuma camada para executar após skip")
            return {"fix_aplicado": True, "resultado_retry": {"status": "completed", "skipped": True}}

    _registrar_acao("apply_fix", f"Aplicando {estrategia}")
    return {"fix_aplicado": True}


def retry(state: RepairAgentState) -> dict[str, Any]:
    """Reexecuta o pipeline com os parâmetros da correção."""
    logger.info("repair_agent_retry")

    if not state.get("fix_aplicado", False):
        return {"resultado_retry": None}

    proposta = state.get("proposta_fix", {})
    params = proposta.get("parametros", {})
    layers = params.get("layers", [])

    if not layers:
        return {"resultado_retry": {"status": "completed", "msg": "sem camadas para executar"}}

    try:
        resultado = pipeline_tools.executar_pipeline(
            layers=layers,
            trigger=AGENT_NAME,
        )
        _registrar_acao("retry", f"Retry status: {resultado['status']}")
        return {"resultado_retry": resultado}
    except Exception as e:
        logger.error("repair_retry_erro", erro=str(e))
        _registrar_acao("retry", f"Retry falhou: {e}")
        return {"resultado_retry": {"status": "failed", "error": str(e)}}


def validate_repair(state: RepairAgentState) -> dict[str, Any]:
    """Valida se o reparo foi bem-sucedido."""
    logger.info("repair_agent_validate")

    resultado = state.get("resultado_retry")
    tentativas = state.get("tentativas", 0) + 1

    if resultado is None:
        return {
            "reparado": False,
            "tentativas": tentativas,
            "validacao": {"sucesso": False, "motivo": "Nenhum resultado de retry"},
        }

    status = resultado.get("status", "unknown")
    reparado = status == "completed"

    if reparado:
        _registrar_acao("validate", f"Reparo bem-sucedido após {tentativas} tentativa(s)")
    else:
        _registrar_acao("validate", f"Reparo falhou (tentativa {tentativas})")
        if tentativas >= state.get("max_tentativas", 2):
            _criar_alerta(
                "Reparo esgotou tentativas",
                f"Após {tentativas} tentativas, a camada {state.get('camada_falha', '?')} "
                f"não foi reparada. Erro: {state.get('erro_original', '')}",
                AlertSeverity.CRITICAL,
            )

    return {
        "reparado": reparado,
        "tentativas": tentativas,
        "validacao": {"sucesso": reparado, "status_retry": status},
    }


# ─── Roteamento ─────────────────────────────────────────────────────────────


def _route_after_validate(state: RepairAgentState) -> str:
    """Roteador: se reparado ou tentativas esgotadas, finaliza."""
    if state.get("reparado", False):
        return "end"
    tentativas = state.get("tentativas", 0)
    max_tent = state.get("max_tentativas", 2)
    if tentativas >= max_tent:
        return "end"
    # Tentar novamente
    return "analyze"


# ─── Construção do Grafo ────────────────────────────────────────────────────


def build_repair_graph() -> StateGraph:
    """Constrói o grafo LangGraph do repair agent.

    Returns:
        StateGraph compilável com .compile()
    """
    graph = StateGraph(RepairAgentState)

    # Nós
    graph.add_node("get_error", get_error)
    graph.add_node("analyze", analyze)
    graph.add_node("propose_fix", propose_fix)
    graph.add_node("apply_fix", apply_fix)
    graph.add_node("retry", retry)
    graph.add_node("validate", validate_repair)

    # Arestas
    graph.add_edge(START, "get_error")
    graph.add_edge("get_error", "analyze")
    graph.add_edge("analyze", "propose_fix")
    graph.add_edge("propose_fix", "apply_fix")
    graph.add_edge("apply_fix", "retry")
    graph.add_edge("retry", "validate")

    graph.add_conditional_edges("validate", _route_after_validate, {
        "end": END,
        "analyze": "analyze",
    })

    return graph


def create_repair_agent():
    """Cria e compila o repair agent.

    Returns:
        Grafo compilado pronto para invocar via .invoke(state).
    """
    graph = build_repair_graph()
    return graph.compile()


def run_repair_agent(
    erro: str,
    camada_falha: str,
    run_id: str | None = None,
    max_tentativas: int = 2,
) -> RepairAgentState:
    """Executa o repair agent para tentar corrigir uma falha.

    Args:
        erro: Mensagem de erro original.
        camada_falha: Camada onde ocorreu a falha ('bronze', 'silver', 'gold').
        run_id: ID do run que falhou (opcional).
        max_tentativas: Máximo de tentativas de reparo.

    Returns:
        Estado final do agente.
    """
    agent = create_repair_agent()

    initial_state: RepairAgentState = {
        "erro_original": erro,
        "camada_falha": camada_falha,
        "run_id": run_id,
        "analise_erro": {},
        "proposta_fix": {},
        "fix_aplicado": False,
        "resultado_retry": None,
        "validacao": None,
        "reparado": False,
        "tentativas": 0,
        "max_tentativas": max_tentativas,
    }

    result = agent.invoke(initial_state)
    return result


# ─── Heurísticas ────────────────────────────────────────────────────────────


def _analisar_com_heuristicas(
    erro: str,
    camada: str,
    contexto: dict[str, Any],
) -> dict[str, Any]:
    """Análise heurística do erro sem depender de LLM."""
    erro_lower = erro.lower()

    # Erro de arquivo não encontrado → dados de entrada ausentes
    if "not found" in erro_lower or "no such file" in erro_lower or "filenotfound" in erro_lower:
        return {
            "causa_raiz": "Arquivo de entrada não encontrado",
            "estrategia": "no_action",
            "confianca": 0.9,
        }

    # Erro de schema → incompatibilidade de colunas
    if "schema" in erro_lower or "column" in erro_lower or "coluna" in erro_lower:
        return {
            "causa_raiz": "Incompatibilidade de schema",
            "estrategia": "retry_from_start",
            "confianca": 0.7,
        }

    # Erro de permissão
    if "permission" in erro_lower or "denied" in erro_lower:
        return {
            "causa_raiz": "Erro de permissão de arquivo",
            "estrategia": "no_action",
            "confianca": 0.9,
        }

    # Tabela de entrada vazia ou ausente
    tabelas = contexto.get("tabelas", {})
    if camada == "silver" and not tabelas.get("bronze", {}).get("exists"):
        return {
            "causa_raiz": "Tabela Bronze não existe",
            "estrategia": "retry_from_start",
            "confianca": 0.9,
        }
    if camada == "gold" and not tabelas.get("silver_conversations", {}).get("exists"):
        return {
            "causa_raiz": "Tabela Silver não existe",
            "estrategia": "retry_from_start",
            "confianca": 0.8,
        }

    # Erro transitório / genérico → retry simples
    return {
        "causa_raiz": "Erro desconhecido — tentando retry",
        "estrategia": "retry",
        "confianca": 0.5,
    }


def _analisar_com_llm(
    llm: LLMProvider,
    erro: str,
    camada: str,
    contexto: dict[str, Any],
) -> dict[str, Any] | None:
    """Tentativa de análise via LLM. Retorna None se falhar."""
    prompt_sistema = (
        "Você é um engenheiro de dados diagnosticando falhas em um pipeline de dados. "
        "Analise o erro e sugira uma estratégia de reparo. "
        "Estratégias disponíveis: retry, skip_layer, retry_from_start, data_quality_fix, no_action. "
        "Responda em JSON com os campos: "
        "causa_raiz (string), estrategia (uma das disponíveis), confianca (float 0-1). "
        "Responda APENAS o JSON."
    )

    prompt_usuario = (
        f"Erro: {erro}\n"
        f"Camada: {camada}\n"
        f"Tabelas existentes: {[k for k, v in contexto.get('tabelas', {}).items() if v.get('exists')]}"
    )

    try:
        resposta = llm.complete_json(
            messages=[
                {"role": "system", "content": prompt_sistema},
                {"role": "user", "content": prompt_usuario},
            ]
        )
        if isinstance(resposta, dict):
            estrategia = resposta.get("estrategia", "retry")
            if estrategia in ESTRATEGIAS:
                return {
                    "causa_raiz_llm": resposta.get("causa_raiz", ""),
                    "estrategia": estrategia,
                    "confianca": resposta.get("confianca", 0.5),
                }
    except Exception:
        pass
    return None


# ─── Helpers ────────────────────────────────────────────────────────────────


def _registrar_acao(acao: str, resumo: str) -> None:
    """Registra uma ação do agente no monitoring store e event bus."""
    store = get_monitoring_store()
    action = AgentAction(
        agent_name=AGENT_NAME,
        action=acao,
        output_summary=resumo,
        success=True,
    )
    store.save_agent_action(action)

    bus = get_event_bus()
    emit_agent_action(bus, AGENT_NAME, acao, {"summary": resumo})


def _criar_alerta(titulo: str, mensagem: str, severidade: AlertSeverity) -> None:
    """Cria um alerta no monitoring store."""
    store = get_monitoring_store()
    alerta = Alert(
        severity=severidade,
        title=titulo,
        message=mensagem,
        source=AGENT_NAME,
    )
    store.save_alert(alerta)
