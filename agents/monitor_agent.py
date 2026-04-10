"""Monitor Agent — LangGraph loop para monitoramento contínuo do pipeline.

Fluxo cíclico: check_new_data → check_health → decide → (trigger_pipeline | wait) → loop.

Detecta novos dados na Bronze, verifica saúde das tabelas,
dispara execução do pipeline quando necessário, e gera alertas.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, TypedDict

import structlog
from langgraph.graph import END, START, StateGraph

from agents.tools import data_tools, quality_tools
from core.events import emit_agent_action, get_event_bus
from monitoring.models import AgentAction, Alert, AlertSeverity
from monitoring.store import get_monitoring_store

logger = structlog.get_logger(__name__)

AGENT_NAME = "monitor_agent"


# ─── Estado do Grafo ────────────────────────────────────────────────────────


class MonitorAgentState(TypedDict, total=False):
    """Estado compartilhado entre nós do monitor agent."""

    tabelas_status: dict[str, Any]
    saude_tabelas: dict[str, Any]
    novos_dados_detectados: bool
    problemas_saude: list[str]
    deve_executar_pipeline: bool
    pipeline_disparado: bool
    resultado_pipeline: dict[str, Any] | None
    ciclos_executados: int
    max_ciclos: int
    erro: str | None
    parar: bool


# ─── Nós do Grafo ───────────────────────────────────────────────────────────


def check_new_data(state: MonitorAgentState) -> dict[str, Any]:
    """Verifica se existem novos dados para processar."""
    logger.info("monitor_agent_check_new_data")

    try:
        tabelas = data_tools.listar_tabelas()
        bronze = tabelas.get("bronze", {})
        silver_conv = tabelas.get("silver_conversations", {})

        novos_dados = False
        if bronze.get("exists") and not silver_conv.get("exists"):
            novos_dados = True
        elif bronze.get("exists") and silver_conv.get("exists"):
            # Se bronze tem mais linhas que silver processou, pode haver novos dados
            bronze_rows = bronze.get("rows", 0)
            silver_rows = silver_conv.get("rows", 0)
            if bronze_rows > 0 and silver_rows == 0:
                novos_dados = True

        _registrar_acao(
            "check_new_data",
            f"Novos dados: {novos_dados}. Bronze: {bronze.get('rows', 0)} rows",
        )
        return {"tabelas_status": tabelas, "novos_dados_detectados": novos_dados}
    except Exception as e:
        logger.warning("monitor_check_new_data_erro", erro=str(e))
        return {"tabelas_status": {}, "novos_dados_detectados": False}


def check_health(state: MonitorAgentState) -> dict[str, Any]:
    """Verifica a saúde das tabelas existentes."""
    logger.info("monitor_agent_check_health")

    tabelas = state.get("tabelas_status", {})
    problemas: list[str] = []
    saude: dict[str, Any] = {}

    for nome, info in tabelas.items():
        if not info.get("exists"):
            continue
        try:
            integridade = quality_tools.verificar_integridade(nome)
            saude[nome] = integridade
            if integridade.get("status") == "critico":
                problemas.append(f"{nome}: score {integridade.get('score_saude')}")
        except Exception as e:
            problemas.append(f"{nome}: erro ao verificar ({e})")

    if problemas:
        _registrar_acao("check_health", f"Problemas: {'; '.join(problemas)}")
    else:
        _registrar_acao("check_health", "Todas tabelas saudáveis")

    return {"saude_tabelas": saude, "problemas_saude": problemas}


def decide(state: MonitorAgentState) -> dict[str, Any]:
    """Decide se deve disparar o pipeline ou alertas."""
    logger.info("monitor_agent_decide")

    novos_dados = state.get("novos_dados_detectados", False)
    problemas = state.get("problemas_saude", [])

    # Gerar alertas para problemas de saúde
    if problemas:
        _criar_alerta(
            "Problemas de saúde detectados",
            "\n".join(problemas),
            AlertSeverity.WARNING,
        )

    deve_executar = novos_dados
    _registrar_acao(
        "decide",
        f"Novos dados: {novos_dados}, Problemas: {len(problemas)}, Executar: {deve_executar}",
    )

    return {"deve_executar_pipeline": deve_executar}


def trigger_pipeline(state: MonitorAgentState) -> dict[str, Any]:
    """Dispara o pipeline agent se necessário."""
    logger.info("monitor_agent_trigger_pipeline")

    if not state.get("deve_executar_pipeline", False):
        return {"pipeline_disparado": False, "resultado_pipeline": None}

    try:
        from agents.pipeline_agent import run_pipeline_agent

        resultado = run_pipeline_agent()
        _registrar_acao(
            "trigger_pipeline",
            f"Pipeline disparado. Completo: {resultado.get('completo')}",
        )
        return {"pipeline_disparado": True, "resultado_pipeline": resultado}
    except Exception as e:
        logger.error("monitor_trigger_pipeline_erro", erro=str(e))
        _criar_alerta(
            "Falha ao disparar pipeline",
            str(e),
            AlertSeverity.ERROR,
        )
        return {"pipeline_disparado": False, "erro": str(e)}


def check_continue(state: MonitorAgentState) -> dict[str, Any]:
    """Verifica se deve continuar o loop de monitoramento."""
    ciclos = state.get("ciclos_executados", 0) + 1
    max_ciclos = state.get("max_ciclos", 1)
    parar = ciclos >= max_ciclos

    logger.info("monitor_agent_check_continue", ciclo=ciclos, max=max_ciclos, parar=parar)
    return {"ciclos_executados": ciclos, "parar": parar}


# ─── Roteamento ─────────────────────────────────────────────────────────────


def _route_continue(state: MonitorAgentState) -> str:
    """Roteador: continuar loop ou finalizar."""
    if state.get("parar", True):
        return "end"
    return "check_new_data"


# ─── Construção do Grafo ────────────────────────────────────────────────────


def build_monitor_graph() -> StateGraph:
    """Constrói o grafo LangGraph do monitor agent.

    Returns:
        StateGraph compilável com .compile()
    """
    graph = StateGraph(MonitorAgentState)

    # Nós
    graph.add_node("check_new_data", check_new_data)
    graph.add_node("check_health", check_health)
    graph.add_node("decide", decide)
    graph.add_node("trigger_pipeline", trigger_pipeline)
    graph.add_node("check_continue", check_continue)

    # Arestas
    graph.add_edge(START, "check_new_data")
    graph.add_edge("check_new_data", "check_health")
    graph.add_edge("check_health", "decide")
    graph.add_edge("decide", "trigger_pipeline")
    graph.add_edge("trigger_pipeline", "check_continue")

    graph.add_conditional_edges("check_continue", _route_continue, {
        "check_new_data": "check_new_data",
        "end": END,
    })

    return graph


def create_monitor_agent():
    """Cria e compila o monitor agent.

    Returns:
        Grafo compilado pronto para invocar via .invoke(state).
    """
    graph = build_monitor_graph()
    return graph.compile()


def run_monitor_agent(max_ciclos: int = 1) -> MonitorAgentState:
    """Executa o monitor agent por N ciclos.

    Args:
        max_ciclos: Número máximo de ciclos de monitoramento.

    Returns:
        Estado final do agente.
    """
    agent = create_monitor_agent()

    initial_state: MonitorAgentState = {
        "tabelas_status": {},
        "saude_tabelas": {},
        "novos_dados_detectados": False,
        "problemas_saude": [],
        "deve_executar_pipeline": False,
        "pipeline_disparado": False,
        "resultado_pipeline": None,
        "ciclos_executados": 0,
        "max_ciclos": max_ciclos,
        "erro": None,
        "parar": False,
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
