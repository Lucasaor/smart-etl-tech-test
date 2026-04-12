"""Orquestrador do pipeline — executa código GERADO DINAMICAMENTE pelos agentes.

Não executa código pré-codificado. O fluxo é:
  1. CodeGenAgent gera o código de cada camada (Bronze/Silver/Gold)
  2. O código é salvo em pipeline/generated/
  3. Este orquestrador carrega e executa o código gerado via executor

Aceita uma ProjectSpec + PipelineGerado para saber o que executar.
Se não houver código gerado, dispara a geração automaticamente.
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from config.settings import get_settings
from core.events import (
    EventBus,
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
    """Orquestra a execução dos steps do pipeline usando código gerado."""

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
        progress_callback=None,
    ) -> PipelineRun:
        """Executa o pipeline para as camadas especificadas.

        Carrega o código gerado pelo CodeGenAgent e executa via executor.
        Se não houver código gerado, dispara a geração automaticamente.

        Args:
            layers: Lista de camadas a executar. Padrão: ["bronze", "silver", "gold"].
            trigger: O que disparou esta execução.
            progress_callback: Callback opcional (step: str, pct: float) -> None
                para reportar progresso ao frontend.

        Returns:
            PipelineRun com status e detalhes dos steps.
        """
        from agents.codegen_agent import (
            PipelineGerado,
            carregar_pipeline_gerado,
            pipeline_gerado_existe,
            salvar_pipeline_gerado,
            CodeGenAgent,
        )
        from pipeline.executor import (
            CodeExecutionError,
            build_settings_dict,
            execute_generated_code,
        )

        if layers is None:
            layers = ["bronze", "silver", "gold"]

        self.carregar_spec_se_disponivel()

        run = PipelineRun(layers=layers, trigger=trigger, status=RunStatus.RUNNING)
        emit_pipeline_started(self.bus, run.run_id, layers)
        logger.info("pipeline_run_start", run_id=run.run_id, layers=layers, trigger=trigger)

        # Carregar código gerado
        generated_dir = str(Path(self.settings.spec_path) / "generated")
        pipeline_gerado: PipelineGerado | None = None

        if pipeline_gerado_existe(generated_dir):
            pipeline_gerado = carregar_pipeline_gerado(generated_dir)
        else:
            # Gerar código automaticamente se spec existe
            if self.spec:
                logger.info("gerando_pipeline_automaticamente")
                try:
                    agent = CodeGenAgent()
                    pipeline_gerado = agent.gerar_pipeline_completo(self.spec)
                    salvar_pipeline_gerado(pipeline_gerado, generated_dir)
                except Exception as e:
                    logger.error("falha_geracao_automatica", erro=str(e))
                    run.status = RunStatus.FAILED
                    step = StepRun(
                        step_name="codegen",
                        status=RunStatus.FAILED,
                        error_message=f"Falha na geração de código: {e}",
                    )
                    step.started_at = datetime.now(timezone.utc)
                    step.completed_at = datetime.now(timezone.utc)
                    run.steps.append(step)
                    run.completed_at = datetime.now(timezone.utc)
                    self.store.save_pipeline_run(run)
                    return run

        if not pipeline_gerado:
            run.status = RunStatus.FAILED
            step = StepRun(
                step_name="codegen",
                status=RunStatus.FAILED,
                error_message="Nenhum código gerado encontrado e nenhuma spec disponível para gerar.",
            )
            step.started_at = datetime.now(timezone.utc)
            step.completed_at = datetime.now(timezone.utc)
            run.steps.append(step)
            run.completed_at = datetime.now(timezone.utc)
            self.store.save_pipeline_run(run)
            return run

        # Preparar settings para injeção no código gerado
        source_path = self.settings.bronze_source_path
        if self.spec and self.spec.dados_brutos_path:
            source_path = self.spec.dados_brutos_path

        settings_dict = build_settings_dict(spec_dados_path=source_path)

        # Executar camadas
        total_layers = len(layers)
        for idx, layer in enumerate(layers):
            if progress_callback:
                progress_callback(layer, idx / total_layers)

            step = StepRun(step_name=layer, status=RunStatus.RUNNING)
            step.started_at = datetime.now(timezone.utc)
            run.steps.append(step)

            try:
                result = self._execute_layer(
                    layer, pipeline_gerado, settings_dict
                )
                step.status = RunStatus.COMPLETED
                step.completed_at = datetime.now(timezone.utc)
                step.rows_output = result.get("rows_written", 0)

                emit_step_completed(
                    self.bus, run.run_id, layer,
                    rows_processed=step.rows_output,
                    duration_sec=step.duration_sec or 0.0,
                )
                logger.info(
                    "step_completed", run_id=run.run_id, step=layer,
                    rows=step.rows_output, duration=step.duration_sec,
                )

                # Se a camada produziu 0 linhas, disparar repair agent
                if step.rows_output == 0:
                    logger.warning(
                        "step_zero_rows_triggering_repair",
                        run_id=run.run_id, step=layer,
                    )
                    repair_result = self._trigger_repair(
                        erro=f"Camada {layer} produziu 0 linhas — nenhum dado processado",
                        camada=layer,
                        run_id=run.run_id,
                    )
                    if repair_result and repair_result.get("reparado"):
                        # Repair bem-sucedido — re-executar a camada
                        logger.info(
                            "repair_success_rerunning_layer",
                            run_id=run.run_id, step=layer,
                        )
                        retry_result = self._execute_layer(
                            layer, pipeline_gerado, settings_dict
                        )
                        step.rows_output = retry_result.get("rows_written", 0)
                        logger.info(
                            "step_rerun_after_repair",
                            run_id=run.run_id, step=layer,
                            rows=step.rows_output,
                        )
                    else:
                        logger.warning(
                            "repair_failed_zero_rows",
                            run_id=run.run_id, step=layer,
                        )

            except (CodeExecutionError, Exception) as e:
                step.status = RunStatus.FAILED
                step.completed_at = datetime.now(timezone.utc)
                step.error_message = f"{type(e).__name__}: {e}"

                emit_step_failed(self.bus, run.run_id, layer, str(e))
                logger.error(
                    "step_failed", run_id=run.run_id, step=layer,
                    error=str(e), traceback=traceback.format_exc(),
                )

                # Tentar reparar automaticamente
                repair_result = self._trigger_repair(
                    erro=step.error_message,
                    camada=layer,
                    run_id=run.run_id,
                )
                if repair_result and repair_result.get("reparado"):
                    logger.info(
                        "repair_success_after_failure",
                        run_id=run.run_id, step=layer,
                    )
                    step.status = RunStatus.COMPLETED
                    step.error_message = None
                    step.completed_at = datetime.now(timezone.utc)
                    continue

                # Reparo falhou — tentar regeneração completa do zero
                # (o repair agent já tentou internamente, mas tentamos
                #  também pelo orchestrator com re-execução direta)
                regen_ok = self._try_fresh_regeneration(
                    layer=layer,
                    erro_original=step.error_message,
                    repair_result=repair_result,
                    pipeline_gerado=pipeline_gerado,
                    settings_dict=settings_dict,
                )
                if regen_ok:
                    logger.info(
                        "fresh_regeneration_success",
                        run_id=run.run_id, step=layer,
                    )
                    # Recarregar pipeline gerado após regeneração
                    pipeline_gerado = carregar_pipeline_gerado(generated_dir)
                    step.status = RunStatus.COMPLETED
                    step.error_message = None
                    step.completed_at = datetime.now(timezone.utc)
                    continue

                # Tudo falhou — registrar recomendação e parar
                recomendacao = ""
                if repair_result:
                    analise = repair_result.get("analise_erro", {})
                    causa = analise.get("causa_raiz", analise.get("causa_raiz_llm", ""))
                    estrategia = analise.get("estrategia", "")
                    tentativas = repair_result.get("tentativas", 0)
                    insights = repair_result.get("insights_acumulados", [])
                    recomendacao = (
                        f"\n\n🔧 Repair Agent: {tentativas} tentativa(s) de reparo + "
                        f"regeneração do zero falharam."
                        f"\nCausa raiz identificada: {causa}"
                        f"\nEstratégia tentada: {estrategia}"
                        f"\nInsights: {'; '.join(insights) if insights else 'N/A'}"
                        f"\nRecomendação: Verifique o código gerado e corrija manualmente, "
                        f"ou ajuste a especificação e regenere o código."
                    )
                step.error_message += recomendacao

                run.status = RunStatus.FAILED
                run.completed_at = datetime.now(timezone.utc)
                self.store.save_pipeline_run(run)
                return run

        run.status = RunStatus.COMPLETED
        run.completed_at = datetime.now(timezone.utc)
        self.store.save_pipeline_run(run)

        if progress_callback:
            progress_callback("concluído", 1.0)

        logger.info(
            "pipeline_run_complete", run_id=run.run_id,
            duration=run.duration_sec, steps=len(run.steps),
        )
        return run

    def run_single_step(self, layer: str, trigger: str = "manual") -> PipelineRun:
        """Executa uma única camada do pipeline."""
        return self.run_pipeline(layers=[layer], trigger=trigger)

    def _try_fresh_regeneration(
        self,
        layer: str,
        erro_original: str,
        repair_result: dict[str, Any] | None,
        pipeline_gerado: Any,
        settings_dict: dict[str, Any],
    ) -> bool:
        """Tenta regenerar o código da camada do zero com insights acumulados.

        Chamado quando o repair agent esgota tentativas. Usa a função
        regenerar_camada_do_zero do CodeGenAgent para gerar código novo
        incorporando os erros encontrados no prompt.

        Returns:
            True se a regeneração e re-execução foram bem-sucedidas.
        """
        if not self.spec:
            return False

        try:
            from agents.codegen_agent import (
                CodeGenAgent,
                salvar_pipeline_gerado,
            )

            # Coletar insights
            insights: list[str] = [f"Erro original: {erro_original}"]
            if repair_result:
                for ins in repair_result.get("insights_acumulados", []):
                    insights.append(ins)
                analise = repair_result.get("analise_erro", {})
                if analise.get("causa_raiz"):
                    insights.append(f"Causa raiz: {analise['causa_raiz']}")

            logger.info(
                "orchestrator_fresh_regeneration",
                layer=layer,
                insights_count=len(insights),
            )

            agent = CodeGenAgent()
            analise_spec = pipeline_gerado.analise

            # Para gold, precisamos mapear o layer para o nome do módulo
            camada = layer
            if layer == "gold":
                # Regenerar todos os módulos gold que falharam
                for gold_mod in pipeline_gerado.gold:
                    if not gold_mod.codigo:
                        continue
                    novo_codigo = agent.regenerar_camada_do_zero(
                        self.spec, analise_spec, gold_mod.camada, insights
                    )
                    if novo_codigo.codigo and not novo_codigo.erro:
                        pipeline_gerado.gold = [
                            novo_codigo if g.camada == gold_mod.camada else g
                            for g in pipeline_gerado.gold
                        ]
            else:
                novo_codigo = agent.regenerar_camada_do_zero(
                    self.spec, analise_spec, camada, insights
                )
                if novo_codigo.erro:
                    logger.warning("fresh_regeneration_code_error", erro=novo_codigo.erro)
                    return False

                if camada == "bronze":
                    pipeline_gerado.bronze = novo_codigo
                elif camada == "silver":
                    pipeline_gerado.silver = novo_codigo

            # Salvar código regenerado
            generated_dir = str(Path(self.settings.spec_path) / "generated")
            salvar_pipeline_gerado(pipeline_gerado, generated_dir)

            # Tentar executar a camada
            result = self._execute_layer(layer, pipeline_gerado, settings_dict)
            rows = result.get("rows_written", 0)
            if rows > 0:
                logger.info("fresh_regeneration_execution_ok", layer=layer, rows=rows)
                return True

            logger.warning("fresh_regeneration_zero_rows", layer=layer)
            return False

        except Exception as e:
            logger.error("fresh_regeneration_failed", layer=layer, erro=str(e))
            return False

    def _trigger_repair(
        self,
        erro: str,
        camada: str,
        run_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Dispara o repair agent para tentar corrigir uma falha ou resultado vazio.

        Returns:
            Estado final do repair agent, ou None se falhar.
        """
        try:
            from agents.repair_agent import run_repair_agent

            logger.info(
                "triggering_repair_agent",
                camada=camada,
                erro=erro[:200],
                run_id=run_id,
            )
            result = run_repair_agent(
                erro=erro,
                camada_falha=camada,
                run_id=run_id,
                max_tentativas=2,
            )
            return result
        except Exception as e:
            logger.error("repair_agent_invocation_failed", erro=str(e))
            return None

    def _execute_layer(
        self,
        layer: str,
        pipeline_gerado: Any,
        settings_dict: dict[str, Any],
    ) -> dict[str, Any]:
        """Executa o código gerado para uma camada específica."""
        from pipeline.executor import execute_generated_code

        if layer == "bronze":
            if not pipeline_gerado.bronze or not pipeline_gerado.bronze.codigo:
                raise RuntimeError("Código Bronze não foi gerado")
            return execute_generated_code(
                pipeline_gerado.bronze.codigo, settings_dict, label="bronze"
            )

        elif layer == "silver":
            if not pipeline_gerado.silver or not pipeline_gerado.silver.codigo:
                raise RuntimeError("Código Silver não foi gerado")
            return execute_generated_code(
                pipeline_gerado.silver.codigo, settings_dict, label="silver"
            )

        elif layer == "gold":
            if not pipeline_gerado.gold:
                logger.error(
                    "execute_layer_gold_sem_modulos",
                    motivo="Nenhum módulo Gold foi gerado pelo CodeGenAgent. "
                           "Verifique se modulos_gold_recomendados foi retornado pela análise LLM.",
                )
                raise RuntimeError("Nenhum módulo Gold foi gerado")

            total_rows = 0
            gold_results = {}

            for gold_mod in pipeline_gerado.gold:
                if not gold_mod.codigo:
                    continue

                nome = gold_mod.camada.replace("gold_", "")
                # Adicionar path do Gold module ao settings
                gold_settings = {
                    **settings_dict,
                    f"gold_{nome}_path": str(
                        Path(settings_dict["gold_base_path"]) / nome
                    ),
                }

                result = execute_generated_code(
                    gold_mod.codigo, gold_settings, label=gold_mod.camada,
                )
                gold_results[nome] = result
                total_rows += result.get("rows_written", 0)

            return {"rows_written": total_rows, "gold_modules": gold_results, "status": "ok"}

        else:
            raise ValueError(f"Camada desconhecida: {layer}")
