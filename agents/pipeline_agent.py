"""Pipeline Agent — LangGraph state machine para execução end-to-end.

Fluxo: load_spec → analyze_data → plan → execute_bronze → execute_silver
→ execute_gold → validate → complete.

Cada nó do grafo é uma função pura que recebe e retorna o estado.
O agente pode ser invocado programaticamente ou pelo monitor_agent.
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
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

    # Tenta carregar do path padrão das settings
    from config.settings import get_settings
    default_path = get_settings().spec_path
    if spec_existe(default_path):
        spec = carregar_spec(default_path)
        _registrar_acao("load_spec", f"Spec carregada (default): {spec.nome}")
        return {"spec": spec, "etapa_atual": "analyze_data"}

    _registrar_acao("load_spec", "Nenhuma spec encontrada — executando sem spec")
    return {"spec": None, "etapa_atual": "analyze_data"}


def analyze_data(state: PipelineAgentState) -> dict[str, Any]:
    """Analisa o estado atual das tabelas."""
    logger.info("pipeline_agent_analyze_data")
    try:
        tabelas = data_tools.listar_tabelas()
        _registrar_acao(
            "analyze_data",
            f"Tabelas verificadas: {sum(1 for t in tabelas.values() if t['exists'])} existentes",
        )
        return {"tabelas_status": tabelas, "etapa_atual": "plan"}
    except Exception as e:
        return {"erro": f"Falha ao analisar dados: {e}", "etapa_atual": "plan"}


def plan(state: PipelineAgentState) -> dict[str, Any]:
    """Define o plano de execução baseado no estado das tabelas."""
    logger.info("pipeline_agent_plan")
    tabelas = state.get("tabelas_status", {})
    layers_solicitadas = state.get("layers_a_executar", ["bronze", "silver", "gold"])

    layers: list[str] = []
    for layer in layers_solicitadas:
        if layer == "bronze":
            layers.append("bronze")
        elif layer == "silver":
            # Silver só se bronze existe ou vai ser executada
            bronze_existe = tabelas.get("bronze", {}).get("exists", False)
            if bronze_existe or "bronze" in layers:
                layers.append("silver")
        elif layer == "gold":
            # Gold só se silver existe ou vai ser executada
            silver_existe = tabelas.get("silver_conversations", {}).get("exists", False)
            if silver_existe or "silver" in layers:
                layers.append("gold")

    plano = {
        "layers": layers,
        "tem_spec": state.get("spec") is not None,
        "tabelas_existentes": [k for k, v in tabelas.items() if v.get("exists")],
    }

    _registrar_acao("plan", f"Plano: executar {layers}")
    return {"plano": plano, "etapa_atual": "execute"}


def execute_bronze(state: PipelineAgentState) -> dict[str, Any]:
    """Executa a camada Bronze."""
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
    """Executa a camada Silver."""
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
    """Executa a camada Gold."""
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
    tabelas_esperadas: dict[str, str] = {}
    if "bronze" in layers:
        tabelas_esperadas["bronze"] = "bronze"
    if "silver" in layers:
        tabelas_esperadas["silver_messages"] = "silver_messages"
        tabelas_esperadas["silver_conversations"] = "silver_conversations"
    if "gold" in layers:
        for nome in ["gold_sentiment", "gold_personas", "gold_segmentation",
                      "gold_analytics", "gold_vendor"]:
            tabelas_esperadas[nome] = nome

    for nome_tabela in tabelas_esperadas:
        info = tabelas.get(nome_tabela, {})
        if not info.get("exists"):
            problemas.append(f"Tabela {nome_tabela} não encontrada")
        elif info.get("rows", 0) == 0:
            problemas.append(f"Tabela {nome_tabela} está vazia")

    validacao = {
        "problemas": problemas,
        "tabelas_verificadas": list(tabelas_esperadas.keys()),
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


# ─── Roteamento ─────────────────────────────────────────────────────────────


def _should_stop_on_error(state: PipelineAgentState) -> str:
    """Roteador: se erro existe, vai direto para complete."""
    if state.get("erro"):
        return "complete"
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
        return "complete"
    plano = state.get("plano", {})
    if "silver" in plano.get("layers", []):
        return "execute_silver"
    return "validate"


def _route_after_silver(state: PipelineAgentState) -> str:
    if state.get("erro"):
        return "complete"
    plano = state.get("plano", {})
    if "gold" in plano.get("layers", []):
        return "execute_gold"
    return "validate"


def _route_after_gold(state: PipelineAgentState) -> str:
    if state.get("erro"):
        return "complete"
    return "validate"


# ─── Construção do Grafo ────────────────────────────────────────────────────


def build_pipeline_graph() -> StateGraph:
    """Constrói o grafo LangGraph do pipeline agent.

    Returns:
        StateGraph compilável com .compile()
    """
    graph = StateGraph(PipelineAgentState)

    # Nós
    graph.add_node("load_spec", load_spec)
    graph.add_node("analyze_data", analyze_data)
    graph.add_node("plan", plan)
    graph.add_node("execute_bronze", execute_bronze)
    graph.add_node("execute_silver", execute_silver)
    graph.add_node("execute_gold", execute_gold)
    graph.add_node("validate", validate)
    graph.add_node("complete", complete)

    # Arestas
    graph.add_edge(START, "load_spec")
    graph.add_edge("load_spec", "analyze_data")
    graph.add_edge("analyze_data", "plan")

    graph.add_conditional_edges("plan", _route_after_plan, {
        "execute_bronze": "execute_bronze",
        "execute_silver": "execute_silver",
        "execute_gold": "execute_gold",
        "validate": "validate",
    })

    graph.add_conditional_edges("execute_bronze", _route_after_bronze, {
        "execute_silver": "execute_silver",
        "complete": "complete",
        "validate": "validate",
    })

    graph.add_conditional_edges("execute_silver", _route_after_silver, {
        "execute_gold": "execute_gold",
        "complete": "complete",
        "validate": "validate",
    })

    graph.add_conditional_edges("execute_gold", _route_after_gold, {
        "validate": "validate",
        "complete": "complete",
    })

    graph.add_edge("validate", "complete")
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
