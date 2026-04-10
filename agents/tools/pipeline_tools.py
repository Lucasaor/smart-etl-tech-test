"""Ferramentas de pipeline — disparar runs, consultar status e histórico.

Utilizadas pelos agentes para interagir com o PipelineOrchestrator
e o MonitoringStore de forma programática.
"""

from __future__ import annotations

from typing import Any

import structlog

from monitoring.models import PipelineRun, RunStatus
from monitoring.store import get_monitoring_store
from pipeline.orchestrator import PipelineOrchestrator

logger = structlog.get_logger(__name__)


def executar_pipeline(
    layers: list[str] | None = None,
    trigger: str = "agent",
) -> dict[str, Any]:
    """Executa o pipeline completo (ou camadas específicas).

    Args:
        layers: Camadas a executar. Padrão: ["bronze", "silver", "gold"].
        trigger: Quem disparou a execução.

    Returns:
        Dicionário com run_id, status, duração e steps.
    """
    orch = PipelineOrchestrator()
    run = orch.run_pipeline(layers=layers, trigger=trigger)
    return _serializar_run(run)


def executar_camada(layer: str, trigger: str = "agent") -> dict[str, Any]:
    """Executa uma única camada do pipeline.

    Args:
        layer: Nome da camada ('bronze', 'silver' ou 'gold').
        trigger: Quem disparou a execução.

    Returns:
        Dicionário com run_id, status, duração e steps.
    """
    orch = PipelineOrchestrator()
    run = orch.run_single_step(layer=layer, trigger=trigger)
    return _serializar_run(run)


def obter_status_ultimo_run() -> dict[str, Any] | None:
    """Retorna o status da última execução do pipeline.

    Returns:
        Dicionário com detalhes do último run, ou None se não houver.
    """
    store = get_monitoring_store()
    runs = store.get_pipeline_runs(limit=1)
    if not runs:
        return None
    return _serializar_run(runs[0])


def obter_historico_runs(limite: int = 10) -> list[dict[str, Any]]:
    """Retorna o histórico de execuções do pipeline.

    Args:
        limite: Máximo de runs a retornar.

    Returns:
        Lista de dicionários com detalhes de cada run.
    """
    store = get_monitoring_store()
    runs = store.get_pipeline_runs(limit=limite)
    return [_serializar_run(r) for r in runs]


def obter_metricas_llm() -> dict[str, Any]:
    """Retorna métricas agregadas de uso de LLM.

    Returns:
        Dicionário com custo total e tokens consumidos.
    """
    store = get_monitoring_store()
    custo = store.get_total_llm_cost()
    tokens = store.get_total_tokens()
    return {
        "custo_total_usd": custo,
        "input_tokens": tokens["input_tokens"],
        "output_tokens": tokens["output_tokens"],
    }


def obter_alertas(apenas_abertos: bool = True, limite: int = 20) -> list[dict[str, Any]]:
    """Retorna alertas do pipeline.

    Args:
        apenas_abertos: Se True, retorna apenas alertas não resolvidos.
        limite: Máximo de alertas a retornar.

    Returns:
        Lista de dicionários com detalhes dos alertas.
    """
    store = get_monitoring_store()
    alertas = store.get_alerts(unresolved_only=apenas_abertos, limit=limite)
    return [
        {
            "alert_id": a.alert_id,
            "severity": a.severity.value,
            "title": a.title,
            "message": a.message,
            "timestamp": a.timestamp.isoformat(),
            "resolved": a.resolved,
            "source": a.source,
        }
        for a in alertas
    ]


# ─── Helpers ────────────────────────────────────────────────────────────────


def _serializar_run(run: PipelineRun) -> dict[str, Any]:
    """Converte PipelineRun para dict serializável."""
    return {
        "run_id": run.run_id,
        "status": run.status.value,
        "started_at": run.started_at.isoformat(),
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "duration_sec": run.duration_sec,
        "layers": run.layers,
        "trigger": run.trigger,
        "steps": [
            {
                "step_name": s.step_name,
                "status": s.status.value,
                "rows_output": s.rows_output,
                "duration_sec": s.duration_sec,
                "error_message": s.error_message,
            }
            for s in run.steps
        ],
    }
