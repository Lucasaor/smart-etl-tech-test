"""Orquestrador do pipeline — sequencia execução dos steps com rastreio de status.

Executa os steps do pipeline (Bronze → Silver → Gold) em ordem, emite eventos,
rastreia status via monitoring store, e fornece um ponto de entrada unificado
tanto para execução manual quanto para execução disparada por agentes.

Aceita opcionalmente uma ProjectSpec para configurar paths e parâmetros
dinamicamente a partir das especificações do projeto.
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
from pipeline.specs import ProjectSpec, carregar_spec, spec_existe

logger = structlog.get_logger(__name__)


class PipelineOrchestrator:
    """Orquestra a execução dos steps do pipeline."""

    def __init__(
        self,
        event_bus: EventBus | None = None,
        spec: ProjectSpec | None = None,
    ) -> None:
        self.settings = get_settings()
        self.bus = event_bus or get_event_bus()
        self.store = get_monitoring_store()
        self.spec = spec

    def carregar_spec_se_disponivel(self) -> ProjectSpec | None:
        """Carrega a spec do projeto se existir no diretório configurado."""
        if self.spec:
            return self.spec
        spec_dir = self.settings.spec_path
        if spec_existe(spec_dir):
            self.spec = carregar_spec(spec_dir)
            logger.info("spec_carregada_pelo_orchestrator", nome=self.spec.nome)
        return self.spec

    def run_pipeline(
        self,
        layers: list[str] | None = None,
        trigger: str = "manual",
    ) -> PipelineRun:
        """Executa o pipeline para as camadas especificadas.

        Se uma ProjectSpec estiver disponível (via construtor ou diretório),
        ela é usada para configurar o source_path da camada Bronze.

        Args:
            layers: Lista de camadas a executar. Padrão: ["bronze", "silver", "gold"].
            trigger: O que disparou esta execução ("manual", "monitor_agent", "schedule").

        Returns:
            PipelineRun com status e detalhes dos steps.
        """
        if layers is None:
            layers = ["bronze", "silver", "gold"]

        # Tenta carregar a spec do projeto
        self.carregar_spec_se_disponivel()

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
        """Executa uma única camada do pipeline."""
        return self.run_pipeline(layers=[layer], trigger=trigger)

    def _get_step_registry(self) -> dict[str, callable]:
        """Registro mapeando nomes de camadas às funções de execução."""
        return {
            "bronze": self._run_bronze,
            "silver": self._run_silver,
            # "gold" será adicionado na Fase 4 (gerado pelo CodeGen Agent)
        }

    def _run_bronze(self) -> dict:
        """Executa o step de ingestão Bronze.

        Usa o path da spec se disponível, senão usa o path das settings.
        """
        from pipeline.bronze.ingestion import ingest_bronze

        source_path = self.settings.bronze_source_path
        if self.spec and self.spec.dados_brutos_path:
            source_path = self.spec.dados_brutos_path

        return ingest_bronze(
            source_path=source_path,
            bronze_path=self.settings.bronze_path,
            mode="auto",
        )

    def _run_silver(self) -> dict:
        """Executa os steps Silver: limpeza, extração e agregação."""
        from pipeline.silver.cleaning import clean_silver
        from pipeline.silver.conversations import aggregate_conversations
        from pipeline.silver.extraction import extract_entities

        silver_stats = clean_silver(
            bronze_path=self.settings.bronze_path,
            silver_messages_path=self.settings.silver_messages_path,
        )
        extract_entities(silver_messages_path=self.settings.silver_messages_path)
        agg_stats = aggregate_conversations(
            silver_messages_path=self.settings.silver_messages_path,
            silver_conversations_path=self.settings.silver_conversations_path,
        )
        return {
            **silver_stats,
            "conversations_written": agg_stats["total_conversations"],
            "rows_written": silver_stats["rows_written"],
        }
