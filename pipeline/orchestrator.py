"""Pipeline orchestrator — sequences step execution with status tracking.

Runs pipeline steps (Bronze → Silver → Gold) in order, emits events,
tracks status via the monitoring store, and provides a unified entry
point for both manual execution and agent-triggered runs.
"""

from __future__ import annotations

import time
import traceback
from datetime import datetime, timezone

import structlog

from config.settings import get_settings
from core.events import (
    EventBus,
    emit_agent_action,
    emit_pipeline_started,
    emit_step_completed,
    emit_step_failed,
    get_event_bus,
)
from monitoring.models import PipelineRun, RunStatus, StepRun
from monitoring.store import get_monitoring_store

logger = structlog.get_logger(__name__)


class PipelineOrchestrator:
    """Orchestrates the execution of pipeline steps."""

    def __init__(
        self,
        event_bus: EventBus | None = None,
    ) -> None:
        self.settings = get_settings()
        self.bus = event_bus or get_event_bus()
        self.store = get_monitoring_store()

    def run_pipeline(
        self,
        layers: list[str] | None = None,
        trigger: str = "manual",
    ) -> PipelineRun:
        """Execute the pipeline for the specified layers.

        Args:
            layers: List of layers to run. Defaults to ["bronze", "silver", "gold"].
            trigger: What triggered this run ("manual", "monitor_agent", "schedule").

        Returns:
            PipelineRun with status and step details.
        """
        if layers is None:
            layers = ["bronze", "silver", "gold"]

        run = PipelineRun(layers=layers, trigger=trigger, status=RunStatus.RUNNING)
        emit_pipeline_started(self.bus, run.run_id, layers)
        logger.info("pipeline_run_start", run_id=run.run_id, layers=layers, trigger=trigger)

        step_registry = self._get_step_registry()

        for layer in layers:
            if layer not in step_registry:
                logger.warning("unknown_layer", layer=layer)
                continue

            step = StepRun(step_name=layer, status=RunStatus.RUNNING)
            step.started_at = datetime.now(timezone.utc)
            run.steps.append(step)

            try:
                result = step_registry[layer]()
                step.status = RunStatus.COMPLETED
                step.completed_at = datetime.now(timezone.utc)
                step.rows_output = result.get("rows_written", 0) if isinstance(result, dict) else 0

                emit_step_completed(
                    self.bus,
                    run.run_id,
                    layer,
                    rows_processed=step.rows_output,
                    duration_sec=step.duration_sec or 0.0,
                )
                logger.info(
                    "step_completed",
                    run_id=run.run_id,
                    step=layer,
                    rows=step.rows_output,
                    duration=step.duration_sec,
                )

            except Exception as e:
                step.status = RunStatus.FAILED
                step.completed_at = datetime.now(timezone.utc)
                step.error_message = f"{type(e).__name__}: {e}"

                emit_step_failed(self.bus, run.run_id, layer, str(e))
                logger.error(
                    "step_failed",
                    run_id=run.run_id,
                    step=layer,
                    error=str(e),
                    traceback=traceback.format_exc(),
                )

                # Stop pipeline on first failure
                run.status = RunStatus.FAILED
                run.completed_at = datetime.now(timezone.utc)
                self.store.save_pipeline_run(run)
                return run

        # All steps completed
        run.status = RunStatus.COMPLETED
        run.completed_at = datetime.now(timezone.utc)
        self.store.save_pipeline_run(run)

        logger.info(
            "pipeline_run_complete",
            run_id=run.run_id,
            duration=run.duration_sec,
            steps=len(run.steps),
        )
        return run

    def run_single_step(self, layer: str, trigger: str = "manual") -> PipelineRun:
        """Run a single pipeline layer."""
        return self.run_pipeline(layers=[layer], trigger=trigger)

    def _get_step_registry(self) -> dict[str, callable]:
        """Registry mapping layer names to their execution functions."""
        return {
            "bronze": self._run_bronze,
            # "silver" and "gold" will be added in Phases 3 and 4
        }

    def _run_bronze(self) -> dict:
        """Execute the Bronze ingestion step."""
        from pipeline.bronze.ingestion import ingest_bronze

        return ingest_bronze(
            source_path=self.settings.bronze_source_path,
            bronze_path=self.settings.bronze_path,
            mode="auto",
        )
