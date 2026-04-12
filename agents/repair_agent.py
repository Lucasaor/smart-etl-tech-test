"""Repair Agent — diagnostica e tenta corrigir falhas do pipeline.

Fluxo: get_error → analyze → propose_fix → apply_fix → retry → validate.

Invocado quando o pipeline_agent ou monitor_agent detecta uma falha.
Utiliza DUAS estratégias principais:
  1. Reexecutar o pipeline (retry, retry_from_start)
  2. REGENERAR O CÓDIGO da camada que falhou via CodeGenAgent (regenerate_code)

A estratégia regenerate_code usa o LLM para corrigir o código gerado
com base na mensagem de erro, sem precisar que o humano intervenha.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

import structlog
from langgraph.graph import END, START, StateGraph

from agents.llm_provider import LLMProvider, LLMProviderError, BudgetExceededError
from config.llm_config import get_llm_config_for_role
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
    insights_acumulados: list[str]  # Insights de falhas para regeneração do zero


# ─── Estratégias de reparo ──────────────────────────────────────────────────

ESTRATEGIAS = {
    "regenerate_code": "Regenerar o código da camada via LLM com contexto do erro",
    "retry": "Reexecutar a camada que falhou sem alterações",
    "skip_layer": "Pular a camada problemática e continuar",
    "retry_from_start": "Reiniciar o pipeline completo (regenerando código)",
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
        llm = LLMProvider(config=get_llm_config_for_role("repair"))
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
    elif estrategia == "regenerate_code":
        proposta["parametros"] = {"layers": [camada], "regenerar": True}
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
    """Aplica a correção proposta.

    Para a estratégia 'regenerate_code', usa o CodeGenAgent para
    regenerar o código da camada que falhou com contexto do erro.
    """
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

    if estrategia == "regenerate_code":
        # Regenerar o código da camada que falhou
        camada = state.get("camada_falha", "")
        erro = state.get("erro_original", "")

        # Se o erro é de schema, limpar a tabela Delta antiga para evitar conflitos
        if "schema" in erro.lower() or "schemamismatch" in erro.lower():
            try:
                from config.settings import get_settings
                settings = get_settings()
                import shutil
                path_map = {
                    "bronze": settings.bronze_path,
                    "silver": settings.silver_messages_path,
                }
                if camada in path_map:
                    delta_log = Path(path_map[camada]) / "_delta_log"
                    if delta_log.exists():
                        shutil.rmtree(delta_log)
                        logger.info("repair_cleared_delta_log", camada=camada, path=str(delta_log))
            except Exception as e:
                logger.warning("repair_clear_delta_log_failed", erro=str(e))

        try:
            from agents.codegen_agent import (
                CodeGenAgent,
                carregar_pipeline_gerado,
                salvar_pipeline_gerado,
            )
            from config.settings import get_settings
            from pipeline.specs import carregar_spec, spec_existe

            settings = get_settings()
            generated_dir = str(Path(settings.spec_path) / "generated")

            # Carregar spec e código existente
            spec = None
            if spec_existe(settings.spec_path):
                spec = carregar_spec(settings.spec_path)

            pipeline_gerado = carregar_pipeline_gerado(generated_dir)

            if spec and pipeline_gerado:
                agent = CodeGenAgent(llm=LLMProvider(config=get_llm_config_for_role("repair")))
                analise = pipeline_gerado.analise

                # Encontrar o código antigo que falhou
                codigo_anterior = ""
                if camada == "bronze" and pipeline_gerado.bronze:
                    codigo_anterior = pipeline_gerado.bronze.codigo
                elif camada == "silver" and pipeline_gerado.silver:
                    codigo_anterior = pipeline_gerado.silver.codigo
                elif camada.startswith("gold"):
                    for g in pipeline_gerado.gold:
                        if g.camada == camada:
                            codigo_anterior = g.codigo
                            break

                # Regenerar com contexto do erro
                novo_codigo = agent.regenerar_camada(
                    spec, analise, camada, erro, codigo_anterior
                )

                # Substituir no pipeline gerado
                if camada == "bronze" and novo_codigo.codigo:
                    pipeline_gerado.bronze = novo_codigo
                elif camada == "silver" and novo_codigo.codigo:
                    pipeline_gerado.silver = novo_codigo
                elif camada.startswith("gold") and novo_codigo.codigo:
                    pipeline_gerado.gold = [
                        novo_codigo if g.camada == camada else g
                        for g in pipeline_gerado.gold
                    ]

                # Salvar código regenerado
                salvar_pipeline_gerado(pipeline_gerado, generated_dir)
                _registrar_acao("apply_fix", f"Código regenerado para {camada}")
            else:
                _registrar_acao("apply_fix", "Spec ou pipeline gerado não encontrado para regeneração")
                return {"fix_aplicado": False}

        except Exception as e:
            logger.error("apply_fix_regenerate_failed", erro=str(e))
            _registrar_acao("apply_fix", f"Falha na regeneração: {e}")
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
    """Valida se o reparo foi bem-sucedido.

    Acumula insights de cada tentativa para uso na regeneração do zero.
    Após esgotar max_tentativas, tenta regenerar o código completamente
    do zero usando os insights acumulados antes de desistir.
    """
    logger.info("repair_agent_validate")

    resultado = state.get("resultado_retry")
    tentativas = state.get("tentativas", 0) + 1
    insights = list(state.get("insights_acumulados", []))

    if resultado is None:
        insights.append("Tentativa sem resultado — retry não produziu output")
        return {
            "reparado": False,
            "tentativas": tentativas,
            "insights_acumulados": insights,
            "validacao": {"sucesso": False, "motivo": "Nenhum resultado de retry"},
        }

    status = resultado.get("status", "unknown")
    erro_retry = resultado.get("error", "")
    reparado = status == "completed"

    # Acumular insight desta tentativa
    if not reparado:
        analise = state.get("analise_erro", {})
        causa = analise.get("causa_raiz", analise.get("causa_raiz_llm", ""))
        insight = f"Tentativa {tentativas}: estratégia={analise.get('estrategia', '?')}"
        if causa:
            insight += f", causa={causa}"
        if erro_retry:
            insight += f", erro={erro_retry[:200]}"
        insights.append(insight)

    if reparado:
        _registrar_acao("validate", f"Reparo bem-sucedido após {tentativas} tentativa(s)")
    else:
        _registrar_acao("validate", f"Reparo falhou (tentativa {tentativas})")
        if tentativas >= state.get("max_tentativas", 2):
            # Tentar regeneração COMPLETA do zero com insights acumulados
            regeneracao_ok = _tentar_regeneracao_do_zero(state, insights)

            if regeneracao_ok:
                _registrar_acao(
                    "validate",
                    "Regeneração do zero bem-sucedida após repair esgotar tentativas",
                )
                return {
                    "reparado": True,
                    "tentativas": tentativas,
                    "insights_acumulados": insights,
                    "validacao": {
                        "sucesso": True,
                        "metodo": "regeneracao_do_zero",
                        "status_retry": "completed",
                    },
                }

            # Regeneração do zero também falhou — escalar
            analise = state.get("analise_erro", {})
            causa = analise.get("causa_raiz", analise.get("causa_raiz_llm", "Não determinada"))
            estrategia = analise.get("estrategia", "N/A")
            _criar_alerta(
                "Reparo esgotou tentativas",
                f"Após {tentativas} tentativas + regeneração do zero, "
                f"a camada {state.get('camada_falha', '?')} não foi reparada.\n"
                f"Causa raiz: {causa}\n"
                f"Estratégia tentada: {estrategia}\n"
                f"Erro original: {state.get('erro_original', '')}\n"
                f"Insights acumulados: {'; '.join(insights)}\n\n"
                f"Recomendação: Verifique o código gerado em data/specs/generated/, "
                f"corrija manualmente ou ajuste a especificação (KPIs/Dicionário) e regenere o código.",
                AlertSeverity.CRITICAL,
            )

    return {
        "reparado": reparado,
        "tentativas": tentativas,
        "insights_acumulados": insights,
        "validacao": {"sucesso": reparado, "status_retry": status},
    }


def _tentar_regeneracao_do_zero(
    state: RepairAgentState,
    insights: list[str],
) -> bool:
    """Tenta regenerar o código da camada completamente do zero.

    Chamado quando o repair agent esgota suas tentativas de correção.
    Usa CodeGenAgent.regenerar_camada_do_zero() para gerar código novo
    incorporando os insights das falhas anteriores.

    Returns:
        True se a regeneração e execução foram bem-sucedidas.
    """
    camada = state.get("camada_falha", "")
    erro_original = state.get("erro_original", "")

    logger.info(
        "tentando_regeneracao_do_zero",
        camada=camada,
        total_insights=len(insights),
    )

    # Adicionar o erro original aos insights
    insights_completos = [f"Erro original: {erro_original}"] + insights

    try:
        from agents.codegen_agent import (
            CodeGenAgent,
            carregar_pipeline_gerado,
            salvar_pipeline_gerado,
        )
        from agents.llm_provider import LLMProvider
        from config.llm_config import get_llm_config_for_role
        from config.settings import get_settings
        from pipeline.specs import carregar_spec, spec_existe

        settings = get_settings()
        generated_dir = str(Path(settings.spec_path) / "generated")

        spec = None
        if spec_existe(settings.spec_path):
            spec = carregar_spec(settings.spec_path)

        pipeline_gerado = carregar_pipeline_gerado(generated_dir)

        if not spec or not pipeline_gerado:
            logger.warning("regeneracao_do_zero_sem_spec_ou_pipeline")
            return False

        agent = CodeGenAgent(llm=LLMProvider(config=get_llm_config_for_role("codegen")))
        analise = pipeline_gerado.analise

        # Regenerar completamente do zero com insights
        novo_codigo = agent.regenerar_camada_do_zero(
            spec, analise, camada, insights_completos
        )

        if novo_codigo.erro:
            logger.warning("regeneracao_do_zero_gerou_erro", erro=novo_codigo.erro)
            return False

        # Substituir no pipeline gerado
        if camada == "bronze" and novo_codigo.codigo:
            pipeline_gerado.bronze = novo_codigo
        elif camada == "silver" and novo_codigo.codigo:
            pipeline_gerado.silver = novo_codigo
        elif camada.startswith("gold") and novo_codigo.codigo:
            pipeline_gerado.gold = [
                novo_codigo if g.camada == camada else g
                for g in pipeline_gerado.gold
            ]

        # Salvar código regenerado
        salvar_pipeline_gerado(pipeline_gerado, generated_dir)

        # Tentar executar o código regenerado
        resultado = pipeline_tools.executar_pipeline(
            layers=[camada.split("_")[0]],  # "gold_xxx" → "gold"
            trigger="repair_agent_regeneracao_do_zero",
        )

        sucesso = resultado.get("status") == "completed"
        logger.info(
            "regeneracao_do_zero_resultado",
            camada=camada,
            sucesso=sucesso,
            status=resultado.get("status"),
        )
        return sucesso

    except Exception as e:
        logger.error("regeneracao_do_zero_falhou", camada=camada, erro=str(e))
        return False


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
        "insights_acumulados": [],
    }

    result = agent.invoke(initial_state)
    return result


# ─── Heurísticas ────────────────────────────────────────────────────────────


def _analisar_com_heuristicas(
    erro: str,
    camada: str,
    contexto: dict[str, Any],
) -> dict[str, Any]:
    """Análise heurística do erro sem depender de LLM.

    Prioriza regenerate_code para erros de execução do código gerado,
    pois o código pode ter bugs que o LLM pode corrigir.

    Inclui diagnósticos detalhados para os erros mais comuns em código Gold:
    - Referência a colunas que não existem na Silver
    - Uso de colunas de mensagem individual na tabela de conversas
    - Tipos de dados Polars incompatíveis (Int8 vs Int32)
    - Funções auxiliares não definidas
    - Variáveis não definidas (gold_path, imports faltando)
    - round(None) em métricas agregadas
    """
    erro_lower = erro.lower()

    # ── Erros de código gerado: syntax / execution ──

    if "codeexecutionerror" in erro_lower or "syntaxerror" in erro_lower:
        return {
            "causa_raiz": "Erro no código gerado — regenerar com contexto do erro",
            "estrategia": "regenerate_code",
            "confianca": 0.85,
        }

    # ── NameError: função ou variável não definida ──

    if "nameerror" in erro_lower:
        causa = "Código gerado referencia nome não definido."
        # Diagnóstico específico para erros comuns
        if "gold_path" in erro:
            causa += (
                " O código não define gold_path = settings['gold_NOME_path']. "
                "Corrija adicionando essa atribuição logo após ler a Silver."
            )
        elif "json" in erro and "import" not in erro:
            causa += (
                " O código usa json.dumps() mas não importa o módulo json. "
                "Adicione 'import json' no topo do arquivo."
            )
        else:
            causa += (
                " Provavelmente uma função auxiliar usada em map_elements() "
                "não foi definida no arquivo. Garanta que TODAS as funções "
                "referenciadas estejam definidas antes de serem chamadas."
            )
        return {
            "causa_raiz": causa,
            "estrategia": "regenerate_code",
            "confianca": 0.9,
        }

    # ── ColumnNotFoundError: coluna inexistente ──

    if "columnnotfounderror" in erro_lower or (
        "column" in erro_lower and "not found" in erro_lower
    ):
        causa = "Código gerado referencia coluna inexistente na tabela Silver."

        # Diagnóstico de colunas proibidas comuns
        colunas_proibidas = {
            "metadata": "A coluna 'metadata' não existe; subcampos já foram extraídos como colunas individuais.",
            "timestamp": "Use 'primeira_mensagem_at' ou 'ultima_mensagem_at' em vez de 'timestamp'.",
            "message_body": "A Silver é por conversa, não tem texto de mensagens. Use colunas pré-computadas.",
            "full_body": "A Silver é por conversa. Use pii_cpf_hash, pii_email_hash, lead_phone.",
            "direction": "Use total_mensagens_inbound / total_mensagens_outbound em vez de 'direction'.",
            "message_type": "Use total_audio, total_image, total_document em vez de 'message_type'.",
            "message_id": "A Silver é por CONVERSA, não por mensagem. Não existe message_id.",
            "sender_phone": "Use 'lead_phone' em vez de 'sender_phone'.",
            "state": "Use 'lead_state' em vez de 'state'.",
            "city": "Use 'lead_city' em vez de 'city'.",
            "device": "Use 'lead_device' em vez de 'device'.",
        }
        for col, dica in colunas_proibidas.items():
            if col in erro_lower:
                causa += f" {dica}"
                break
        else:
            causa += (
                " Verifique quais colunas existem na tabela Silver e use "
                "os nomes corretos. A tabela é por CONVERSA, não por mensagem."
            )

        return {
            "causa_raiz": causa,
            "estrategia": "regenerate_code",
            "confianca": 0.95,
        }

    # ── SchemaError / SchemaMismatch: tipos incompatíveis ──

    if "schema" in erro_lower or "schemamismatch" in erro_lower:
        causa = "Incompatibilidade de schema no código gerado."
        if "int8" in erro_lower or "int32" in erro_lower:
            causa += (
                " Polars dt.weekday()/week()/hour() retornam Int8, NÃO Int32. "
                "Use pl.lit(None).cast(pl.Int8) para literais nulos de colunas temporais."
            )
        elif "concat" in erro_lower or "diagonal" in erro_lower:
            causa += (
                " Ao usar pl.concat(), garanta que os dtypes de todas as colunas "
                "correspondam exatamente entre os DataFrames."
            )
        return {
            "causa_raiz": causa,
            "estrategia": "regenerate_code",
            "confianca": 0.9,
        }

    # ── TypeError: round(None), operações com None ──

    if "typeerror" in erro_lower:
        causa = "Erro de tipo no código gerado."
        if "round" in erro_lower or "__round__" in erro_lower:
            causa += (
                " round() do Python não aceita None. Métricas agregadas (.mean(), "
                ".median()) podem retornar None para colunas nulas. "
                "Proteja com: round(val, 2) if val is not None else 0.0"
            )
        return {
            "causa_raiz": causa,
            "estrategia": "regenerate_code",
            "confianca": 0.85,
        }

    # ── ValueError / KeyError ──

    if "valueerror" in erro_lower or "keyerror" in erro_lower:
        return {
            "causa_raiz": "Erro de valor/chave no código gerado",
            "estrategia": "regenerate_code",
            "confianca": 0.8,
        }

    # ── Arquivo não encontrado / permissão ──
    # Deve vir DEPOIS dos checks de coluna/schema para não capturar ColumnNotFoundError
    if "filenotfound" in erro_lower or "no such file" in erro_lower or ("not found" in erro_lower and "file" in erro_lower) or "permission denied" in erro_lower or "permissionerror" in erro_lower:
        return {
            "causa_raiz": "Arquivo de entrada não encontrado ou sem permissão",
            "estrategia": "no_action",
            "confianca": 0.9,
        }

    # ── Tabela de entrada vazia ou ausente ──
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

    # ── Fallback: tentar regenerar ──
    return {
        "causa_raiz": "Erro desconhecido — tentando regenerar código",
        "estrategia": "regenerate_code",
        "confianca": 0.6,
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
        "Estratégias disponíveis: regenerate_code, retry, skip_layer, retry_from_start, data_quality_fix, no_action. "
        "PREFIRA regenerate_code quando o erro parece ser de código gerado incorreto (schema, tipo, lógica). "
        "\n\nCONTEXTO IMPORTANTE sobre erros comuns na camada Gold:\n"
        "- ColumnNotFoundError: o código usa colunas que não existem na Silver. "
        "A Silver é por CONVERSA. Colunas proibidas: metadata, timestamp, message_body, "
        "full_body, direction, message_type, message_id, sender_phone, state, city, device. "
        "Colunas corretas: primeira_mensagem_at, total_mensagens_inbound, lead_state, lead_city, etc.\n"
        "- NameError (gold_path): código não define gold_path = settings['gold_NOME_path'].\n"
        "- NameError (json): código usa json.dumps() sem 'import json'.\n"
        "- NameError (função): funções auxiliares usadas em map_elements() não foram definidas.\n"
        "- SchemaError Int8/Int32: Polars dt.weekday()/week()/hour() retornam Int8.\n"
        "- TypeError __round__: round(None) — proteger com 'if val is not None'.\n"
        "\nResponda em JSON com os campos: "
        "causa_raiz (string detalhada), estrategia (uma das disponíveis), confianca (float 0-1). "
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
