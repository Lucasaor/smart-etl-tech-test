"""Pipeline Agent — LangGraph state machine para execução end-to-end.

Fluxo: load_spec → analyze_data → generate_code → run_tests → plan →
execute_bronze → execute_silver → execute_gold → validate → complete.

O passo CRÍTICO é generate_code: sem ele, não há pipeline para executar.
Todo código é gerado dinamicamente pelo CodeGenAgent baseado na spec do usuário.
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, TypedDict

import structlog
from langgraph.graph import END, START, StateGraph

from agents.llm_provider import LLMProvider, LLMProviderError, BudgetExceededError
from agents.tools import data_tools, pipeline_tools, quality_tools
from core.events import emit_agent_action, get_event_bus
from monitoring.models import AgentAction, Alert, AlertSeverity
from monitoring.store import get_monitoring_store
from pipeline.specs import ProjectSpec, carregar_spec, spec_existe

logger = structlog.get_logger(__name__)

AGENT_NAME = "pipeline_agent"


# ─── Estado do Grafo ────────────────────────────────────────────────────────


class PipelineAgentState(TypedDict, total=False):
    """Estado compartilhado entre nós do pipeline agent."""

    spec: ProjectSpec | None
    spec_path: str
    tabelas_status: dict[str, Any]
    plano: dict[str, Any]
    # Geração de código
    pipeline_gerado: Any  # PipelineGerado
    codegen_status: dict[str, Any]
    testes_status: dict[str, Any]
    # Resultados de execução
    resultado_bronze: dict[str, Any] | None
    resultado_silver: dict[str, Any] | None
    resultado_gold: dict[str, Any] | None
    validacao: dict[str, Any] | None
    erro: str | None
    etapa_atual: str
    completo: bool
    layers_a_executar: list[str]


# ─── Nós do Grafo ───────────────────────────────────────────────────────────


def load_spec(state: PipelineAgentState) -> dict[str, Any]:
    """Carrega a especificação do projeto."""
    logger.info("pipeline_agent_load_spec")
    spec_path = state.get("spec_path", "")

    if state.get("spec"):
        return {"etapa_atual": "analyze_data"}

    if spec_path and spec_existe(spec_path):
        spec = carregar_spec(spec_path)
        _registrar_acao("load_spec", f"Spec carregada: {spec.nome}")
        return {"spec": spec, "etapa_atual": "analyze_data"}

    from config.settings import get_settings
    default_path = get_settings().spec_path
    if spec_existe(default_path):
        spec = carregar_spec(default_path)
        _registrar_acao("load_spec", f"Spec carregada (default): {spec.nome}")
        return {"spec": spec, "etapa_atual": "analyze_data"}

    _registrar_acao("load_spec", "Nenhuma spec encontrada — impossível gerar pipeline")
    return {"spec": None, "etapa_atual": "analyze_data", "erro": "Spec não encontrada. Configure o projeto primeiro."}


def analyze_data(state: PipelineAgentState) -> dict[str, Any]:
    """Analisa o estado atual das tabelas."""
    logger.info("pipeline_agent_analyze_data")

    if state.get("erro"):
        return {"etapa_atual": "complete"}

    try:
        tabelas = data_tools.listar_tabelas()
        _registrar_acao(
            "analyze_data",
            f"Tabelas verificadas: {sum(1 for t in tabelas.values() if t['exists'])} existentes",
        )
        return {"tabelas_status": tabelas, "etapa_atual": "generate_code"}
    except Exception as e:
        return {"erro": f"Falha ao analisar dados: {e}", "etapa_atual": "generate_code"}


def generate_code(state: PipelineAgentState) -> dict[str, Any]:
    """Gera o código do pipeline usando o CodeGenAgent.

    Este é o passo CENTRAL: usa LLM para gerar o código Python
    de cada camada baseado nos inputs do usuário (spec).
    """
    logger.info("pipeline_agent_generate_code")

    spec = state.get("spec")
    if not spec:
        return {"erro": "Spec não carregada — impossível gerar código", "etapa_atual": "complete"}

    from agents.codegen_agent import (
        CodeGenAgent,
        PipelineGerado,
        carregar_pipeline_gerado,
        pipeline_gerado_existe,
        salvar_pipeline_gerado,
    )
    from config.settings import get_settings

    settings = get_settings()
    generated_dir = str(Path(settings.spec_path) / "generated")

    # Verificar se já existe código gerado
    if pipeline_gerado_existe(generated_dir):
        pipeline_gerado = carregar_pipeline_gerado(generated_dir)
        if pipeline_gerado and pipeline_gerado.completo:
            _registrar_acao("generate_code", "Código gerado já existe — reutilizando")
            return {
                "pipeline_gerado": pipeline_gerado,
                "codegen_status": {"status": "reused", "modulos_gold": len(pipeline_gerado.gold)},
                "etapa_atual": "run_tests",
            }

    # Gerar código via LLM
    try:
        _registrar_acao("generate_code", "Iniciando geração de código via LLM...")
        agent = CodeGenAgent()
        pipeline_gerado = agent.gerar_pipeline_completo(spec)

        # Salvar código gerado
        salvar_pipeline_gerado(pipeline_gerado, generated_dir)

        status = {
            "status": "generated",
            "bronze": bool(pipeline_gerado.bronze and pipeline_gerado.bronze.codigo),
            "silver": bool(pipeline_gerado.silver and pipeline_gerado.silver.codigo),
            "gold_modules": len([g for g in pipeline_gerado.gold if g.codigo]),
            "completo": pipeline_gerado.completo,
        }

        _registrar_acao(
            "generate_code",
            f"Código gerado: bronze={status['bronze']}, silver={status['silver']}, "
            f"gold={status['gold_modules']} módulos",
        )

        if not pipeline_gerado.gold:
            logger.warning(
                "generate_code_sem_gold",
                motivo="Nenhum módulo Gold foi gerado. O pipeline prosseguirá sem a camada Gold.",
            )

        if not pipeline_gerado.completo:
            erros = []
            if pipeline_gerado.bronze and pipeline_gerado.bronze.erro:
                erros.append(f"Bronze: {pipeline_gerado.bronze.erro}")
            if pipeline_gerado.silver and pipeline_gerado.silver.erro:
                erros.append(f"Silver: {pipeline_gerado.silver.erro}")
            for g in pipeline_gerado.gold:
                if g.erro:
                    erros.append(f"Gold({g.camada}): {g.erro}")
            return {
                "pipeline_gerado": pipeline_gerado,
                "codegen_status": status,
                "erro": f"Geração incompleta: {'; '.join(erros)}",
                "etapa_atual": "complete",
            }

        return {
            "pipeline_gerado": pipeline_gerado,
            "codegen_status": status,
            "etapa_atual": "run_tests",
        }

    except (LLMProviderError, BudgetExceededError) as e:
        _registrar_acao("generate_code", f"Falha LLM: {e}")
        return {
            "erro": f"Falha na geração de código (LLM): {e}",
            "codegen_status": {"status": "failed", "error": str(e)},
            "etapa_atual": "complete",
        }
    except Exception as e:
        _registrar_acao("generate_code", f"Erro inesperado: {e}")
        return {
            "erro": f"Erro na geração de código: {e}",
            "codegen_status": {"status": "failed", "error": str(e)},
            "etapa_atual": "complete",
        }


def run_tests(state: PipelineAgentState) -> dict[str, Any]:
    """Executa os testes gerados para validar o código antes de rodar o pipeline."""
    logger.info("pipeline_agent_run_tests")

    pipeline_gerado = state.get("pipeline_gerado")
    if not pipeline_gerado:
        return {"testes_status": {"status": "skipped", "reason": "sem código gerado"}, "etapa_atual": "plan"}

    from pipeline.executor import execute_generated_tests

    resultados: dict[str, Any] = {}

    # Testar Bronze
    if pipeline_gerado.bronze and pipeline_gerado.bronze.testes:
        resultado = execute_generated_tests(
            pipeline_gerado.bronze.testes,
            pipeline_gerado.bronze.codigo,
            "bronze",
        )
        resultados["bronze"] = resultado

    # Testar Silver
    if pipeline_gerado.silver and pipeline_gerado.silver.testes:
        resultado = execute_generated_tests(
            pipeline_gerado.silver.testes,
            pipeline_gerado.silver.codigo,
            "silver",
        )
        resultados["silver"] = resultado

    # Testar Gold modules
    for gold in pipeline_gerado.gold:
        if gold.testes:
            resultado = execute_generated_tests(gold.testes, gold.codigo, gold.camada)
            resultados[gold.camada] = resultado

    testes_falhos = [k for k, v in resultados.items() if v.get("status") == "failed"]

    status = {
        "status": "passed" if not testes_falhos else "some_failed",
        "resultados": resultados,
        "falhos": testes_falhos,
        "total": len(resultados),
    }

    _registrar_acao(
        "run_tests",
        f"Testes: {len(resultados)} executados, {len(testes_falhos)} falhas",
    )

    # Testes falhando NÃO bloqueiam execução — são informativos
    return {"testes_status": status, "etapa_atual": "plan"}


def plan(state: PipelineAgentState) -> dict[str, Any]:
    """Define o plano de execução baseado no código gerado."""
    logger.info("pipeline_agent_plan")
    tabelas = state.get("tabelas_status", {})
    layers_solicitadas = state.get("layers_a_executar", ["bronze", "silver", "gold"])

    pipeline_gerado = state.get("pipeline_gerado")
    layers: list[str] = []

    for layer in layers_solicitadas:
        if layer == "bronze" and pipeline_gerado and pipeline_gerado.bronze:
            layers.append("bronze")
        elif layer == "silver" and pipeline_gerado and pipeline_gerado.silver:
            bronze_existe = tabelas.get("bronze", {}).get("exists", False)
            if bronze_existe or "bronze" in layers:
                layers.append("silver")
        elif layer == "gold" and pipeline_gerado and pipeline_gerado.gold:
            silver_existe = tabelas.get("silver_conversations", {}).get("exists", False)
            if silver_existe or "silver" in layers:
                layers.append("gold")
        elif layer == "gold" and pipeline_gerado and not pipeline_gerado.gold:
            logger.warning(
                "plan_gold_sem_modulos",
                motivo="Gold solicitada mas nenhum módulo Gold foi gerado. "
                       "Camada Gold será pulada nesta execução.",
                modulos_gold=len(pipeline_gerado.gold) if pipeline_gerado else 0,
            )

    plano = {
        "layers": layers,
        "tem_spec": state.get("spec") is not None,
        "tem_codigo_gerado": pipeline_gerado is not None and pipeline_gerado.completo,
        "tabelas_existentes": [k for k, v in tabelas.items() if v.get("exists")],
    }

    _registrar_acao("plan", f"Plano: executar {layers} (código gerado)")
    return {"plano": plano, "etapa_atual": "execute"}


def execute_bronze(state: PipelineAgentState) -> dict[str, Any]:
    """Executa a camada Bronze usando código gerado."""
    logger.info("pipeline_agent_execute_bronze")
    plano = state.get("plano", {})
    if "bronze" not in plano.get("layers", []):
        return {"etapa_atual": "execute_silver"}

    try:
        resultado = pipeline_tools.executar_camada("bronze", trigger=AGENT_NAME)
        _registrar_acao("execute_bronze", f"Status: {resultado['status']}")
        if resultado["status"] == "failed":
            erro_step = resultado["steps"][0]["error_message"] if resultado["steps"] else "desconhecido"
            return {"resultado_bronze": resultado, "erro": f"Bronze falhou: {erro_step}"}
        return {"resultado_bronze": resultado, "etapa_atual": "execute_silver"}
    except Exception as e:
        return {"erro": f"Exceção Bronze: {e}", "resultado_bronze": None}


def execute_silver(state: PipelineAgentState) -> dict[str, Any]:
    """Executa a camada Silver usando código gerado."""
    logger.info("pipeline_agent_execute_silver")
    plano = state.get("plano", {})
    if "silver" not in plano.get("layers", []):
        return {"etapa_atual": "execute_gold"}

    if state.get("erro"):
        return {"etapa_atual": "complete"}

    try:
        resultado = pipeline_tools.executar_camada("silver", trigger=AGENT_NAME)
        _registrar_acao("execute_silver", f"Status: {resultado['status']}")
        if resultado["status"] == "failed":
            erro_step = resultado["steps"][0]["error_message"] if resultado["steps"] else "desconhecido"
            return {"resultado_silver": resultado, "erro": f"Silver falhou: {erro_step}"}
        return {"resultado_silver": resultado, "etapa_atual": "execute_gold"}
    except Exception as e:
        return {"erro": f"Exceção Silver: {e}", "resultado_silver": None}


def execute_gold(state: PipelineAgentState) -> dict[str, Any]:
    """Executa a camada Gold usando código gerado."""
    logger.info("pipeline_agent_execute_gold")
    plano = state.get("plano", {})
    if "gold" not in plano.get("layers", []):
        return {"etapa_atual": "validate"}

    if state.get("erro"):
        return {"etapa_atual": "complete"}

    try:
        resultado = pipeline_tools.executar_camada("gold", trigger=AGENT_NAME)
        _registrar_acao("execute_gold", f"Status: {resultado['status']}")
        if resultado["status"] == "failed":
            erro_step = resultado["steps"][0]["error_message"] if resultado["steps"] else "desconhecido"
            return {"resultado_gold": resultado, "erro": f"Gold falhou: {erro_step}"}
        return {"resultado_gold": resultado, "etapa_atual": "validate"}
    except Exception as e:
        return {"erro": f"Exceção Gold: {e}", "resultado_gold": None}


def validate(state: PipelineAgentState) -> dict[str, Any]:
    """Valida que as tabelas foram criadas corretamente."""
    logger.info("pipeline_agent_validate")

    if state.get("erro"):
        return {"etapa_atual": "complete"}

    tabelas = data_tools.listar_tabelas()
    plano = state.get("plano", {})
    layers = plano.get("layers", [])

    problemas: list[str] = []

    # Verificar as tabelas esperadas baseado nas layers executadas
    if "bronze" in layers:
        info = tabelas.get("bronze", {})
        if not info.get("exists"):
            problemas.append("Tabela bronze não encontrada")
        elif info.get("rows", 0) == 0:
            problemas.append("Tabela bronze está vazia")

    if "silver" in layers:
        for nome in ["silver_messages", "silver_conversations"]:
            info = tabelas.get(nome, {})
            if not info.get("exists"):
                problemas.append(f"Tabela {nome} não encontrada")
            elif info.get("rows", 0) == 0:
                problemas.append(f"Tabela {nome} está vazia")

    if "gold" in layers:
        # Para Gold, verificar ao menos uma tabela Gold foi criada
        gold_tables = [k for k in tabelas if k.startswith("gold_")]
        if not gold_tables:
            # Verificar diretamente se algum diretório gold foi criado
            from config.settings import get_settings
            gold_path = Path(get_settings().gold_path)
            if gold_path.exists():
                gold_dirs = [d for d in gold_path.iterdir() if d.is_dir()]
                if not gold_dirs:
                    problemas.append("Nenhuma tabela Gold foi criada")

    validacao = {
        "problemas": problemas,
        "todas_ok": len(problemas) == 0,
    }

    _registrar_acao(
        "validate",
        f"{'Validação OK' if not problemas else f'{len(problemas)} problemas encontrados'}",
    )
    return {"validacao": validacao, "etapa_atual": "complete"}


def complete(state: PipelineAgentState) -> dict[str, Any]:
    """Marca a execução como completa."""
    logger.info("pipeline_agent_complete")
    erro = state.get("erro")
    if erro:
        _registrar_acao("complete", f"Finalizado com erro: {erro}")
        _criar_alerta(
            "Pipeline falhou",
            erro,
            AlertSeverity.ERROR,
        )
    else:
        _registrar_acao("complete", "Pipeline finalizado com sucesso")
    return {"completo": True}


def try_repair(state: PipelineAgentState) -> dict[str, Any]:
    """Tenta reparar automaticamente o pipeline via Repair Agent.

    Invocado quando qualquer camada falha. Identifica a camada que falhou
    e delega ao repair agent para análise + correção + retry.
    """
    erro = state.get("erro", "")
    if not erro:
        return {"etapa_atual": "complete"}

    # Identificar a camada que falhou a partir da mensagem de erro
    camada_falha = "desconhecida"
    for camada in ["bronze", "silver", "gold"]:
        if camada.lower() in erro.lower():
            camada_falha = camada
            break

    logger.info(
        "pipeline_agent_try_repair",
        camada_falha=camada_falha,
        erro=erro[:300],
    )
    _registrar_acao("try_repair", f"Invocando repair agent para camada {camada_falha}")

    try:
        from agents.repair_agent import run_repair_agent

        resultado = run_repair_agent(
            erro=erro,
            camada_falha=camada_falha,
            max_tentativas=2,
        )

        reparado = resultado.get("reparado", False)
        tentativas = resultado.get("tentativas", 0)

        if reparado:
            logger.info(
                "pipeline_agent_repair_sucesso",
                camada=camada_falha,
                tentativas=tentativas,
            )
            _registrar_acao(
                "try_repair",
                f"Reparo bem-sucedido para {camada_falha} após {tentativas} tentativa(s)",
            )
            # Limpar o erro para que 'complete' não registre falha
            return {"erro": None, "etapa_atual": "complete"}
        else:
            logger.warning(
                "pipeline_agent_repair_falhou",
                camada=camada_falha,
                tentativas=tentativas,
            )
            _registrar_acao(
                "try_repair",
                f"Reparo falhou para {camada_falha} após {tentativas} tentativa(s)",
            )
            return {"etapa_atual": "complete"}

    except Exception as e:
        logger.error("pipeline_agent_repair_erro", erro=str(e))
        _registrar_acao("try_repair", f"Erro ao invocar repair agent: {e}")
        return {"etapa_atual": "complete"}


# ─── Roteamento ─────────────────────────────────────────────────────────────


def _should_stop_on_error(state: PipelineAgentState) -> str:
    """Roteador: se erro existe, vai direto para complete."""
    if state.get("erro"):
        return "try_repair"
    return "continue"


def _route_after_plan(state: PipelineAgentState) -> str:
    plano = state.get("plano", {})
    layers = plano.get("layers", [])
    if "bronze" in layers:
        return "execute_bronze"
    if "silver" in layers:
        return "execute_silver"
    if "gold" in layers:
        return "execute_gold"
    return "validate"


def _route_after_bronze(state: PipelineAgentState) -> str:
    if state.get("erro"):
        return "try_repair"
    plano = state.get("plano", {})
    if "silver" in plano.get("layers", []):
        return "execute_silver"
    return "validate"


def _route_after_silver(state: PipelineAgentState) -> str:
    if state.get("erro"):
        return "try_repair"
    plano = state.get("plano", {})
    if "gold" in plano.get("layers", []):
        return "execute_gold"
    return "validate"


def _route_after_gold(state: PipelineAgentState) -> str:
    if state.get("erro"):
        return "try_repair"
    return "validate"


# ─── Construção do Grafo ────────────────────────────────────────────────────


def build_pipeline_graph() -> StateGraph:
    """Constrói o grafo LangGraph do pipeline agent.

    Fluxo: load_spec → analyze_data → generate_code → run_tests →
           plan → execute_* → validate → complete

    Returns:
        StateGraph compilável com .compile()
    """
    graph = StateGraph(PipelineAgentState)

    # Nós
    graph.add_node("load_spec", load_spec)
    graph.add_node("analyze_data", analyze_data)
    graph.add_node("generate_code", generate_code)
    graph.add_node("run_tests", run_tests)
    graph.add_node("plan", plan)
    graph.add_node("execute_bronze", execute_bronze)
    graph.add_node("execute_silver", execute_silver)
    graph.add_node("execute_gold", execute_gold)
    graph.add_node("validate", validate)
    graph.add_node("try_repair", try_repair)
    graph.add_node("complete", complete)

    # Arestas lineares: spec → analyze → generate_code → run_tests → plan
    graph.add_edge(START, "load_spec")
    graph.add_edge("load_spec", "analyze_data")
    graph.add_edge("analyze_data", "generate_code")
    graph.add_edge("generate_code", "run_tests")
    graph.add_edge("run_tests", "plan")

    # Roteamento condicional após plan
    graph.add_conditional_edges("plan", _route_after_plan, {
        "execute_bronze": "execute_bronze",
        "execute_silver": "execute_silver",
        "execute_gold": "execute_gold",
        "validate": "validate",
    })

    graph.add_conditional_edges("execute_bronze", _route_after_bronze, {
        "execute_silver": "execute_silver",
        "try_repair": "try_repair",
        "validate": "validate",
    })

    graph.add_conditional_edges("execute_silver", _route_after_silver, {
        "execute_gold": "execute_gold",
        "try_repair": "try_repair",
        "validate": "validate",
    })

    graph.add_conditional_edges("execute_gold", _route_after_gold, {
        "validate": "validate",
        "try_repair": "try_repair",
    })

    graph.add_edge("validate", "complete")
    graph.add_edge("try_repair", "complete")
    graph.add_edge("complete", END)

    return graph


def create_pipeline_agent():
    """Cria e compila o pipeline agent.

    Returns:
        Grafo compilado pronto para invocar via .invoke(state).
    """
    graph = build_pipeline_graph()
    return graph.compile()


def run_pipeline_agent(
    spec_path: str | None = None,
    spec: ProjectSpec | None = None,
    layers: list[str] | None = None,
) -> PipelineAgentState:
    """Executa o pipeline agent completo.

    Args:
        spec_path: Caminho para a spec do projeto.
        spec: Spec pré-carregada (prioridade sobre spec_path).
        layers: Camadas a executar. Padrão: ["bronze", "silver", "gold"].

    Returns:
        Estado final do agente.
    """
    agent = create_pipeline_agent()

    initial_state: PipelineAgentState = {
        "spec": spec,
        "spec_path": spec_path or "",
        "tabelas_status": {},
        "plano": {},
        "pipeline_gerado": None,
        "codegen_status": {},
        "testes_status": {},
        "resultado_bronze": None,
        "resultado_silver": None,
        "resultado_gold": None,
        "validacao": None,
        "erro": None,
        "etapa_atual": "load_spec",
        "completo": False,
        "layers_a_executar": layers or ["bronze", "silver", "gold"],
    }

    result = agent.invoke(initial_state)
    return result


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
