"""Tests for Phase 5 — Agent System (tools + LangGraph agents)."""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from core.storage import LocalDeltaBackend


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _isolate_settings(tmp_path, monkeypatch):
    """Isola settings para usar diretórios temporários."""
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("RUNTIME_ENV", "local")
    monkeypatch.setenv("SPEC_DIR", str(tmp_path / "specs"))

    # Limpar caches de singletons
    from config.settings import get_settings
    from core.storage import get_storage_backend
    from monitoring.store import get_monitoring_store
    from core.events import get_event_bus

    get_settings.cache_clear()
    get_storage_backend.cache_clear()
    get_monitoring_store.cache_clear()
    get_event_bus.cache_clear()

    # Limpar cache do data_tools
    import agents.tools.data_tools as dt
    dt._TABLE_NAMES = None

    yield

    get_settings.cache_clear()
    get_storage_backend.cache_clear()
    get_monitoring_store.cache_clear()
    get_event_bus.cache_clear()
    dt._TABLE_NAMES = None


@pytest.fixture
def storage():
    return LocalDeltaBackend()


@pytest.fixture
def sample_bronze_df() -> pl.DataFrame:
    return pl.DataFrame({
        "conversation_id": ["conv_001", "conv_002", "conv_003"],
        "message_body": ["Olá, bom dia", "Quero contratar seguro", "Obrigado!"],
        "sender_name": ["João", "Maria", "João"],
        "timestamp": pl.Series([
            "2026-01-01 09:00:00",
            "2026-01-01 10:00:00",
            "2026-01-01 11:00:00",
        ], dtype=pl.Datetime),
        "_ingested_at": pl.Series([
            "2026-01-01 12:00:00",
            "2026-01-01 12:00:00",
            "2026-01-01 12:00:00",
        ], dtype=pl.Datetime),
    })


@pytest.fixture
def sample_silver_df() -> pl.DataFrame:
    return pl.DataFrame({
        "conversation_id": ["conv_001", "conv_002"],
        "total_messages": [10, 5],
        "inbound_messages": [5, 2],
        "outbound_messages": [5, 3],
        "avg_response_time_sec": [120.0, 300.0],
        "has_cpf": [True, False],
        "has_email": [False, True],
    })


@pytest.fixture
def populated_bronze(tmp_path, storage, sample_bronze_df):
    """Cria tabela Bronze populada."""
    from config.settings import get_settings
    settings = get_settings()
    path = settings.bronze_path
    storage.write_table(sample_bronze_df, path)
    return path


@pytest.fixture
def populated_silver_conv(tmp_path, storage, sample_silver_df):
    """Cria tabela Silver conversations populada."""
    from config.settings import get_settings
    settings = get_settings()
    path = settings.silver_conversations_path
    storage.write_table(sample_silver_df, path)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# data_tools
# ═══════════════════════════════════════════════════════════════════════════


class TestDataTools:

    def test_listar_tabelas_vazio(self):
        from agents.tools.data_tools import listar_tabelas
        resultado = listar_tabelas()
        assert isinstance(resultado, dict)
        assert "bronze" in resultado
        assert "silver_messages" in resultado
        assert resultado["bronze"]["exists"] is False

    def test_listar_tabelas_com_bronze(self, populated_bronze):
        from agents.tools.data_tools import listar_tabelas
        resultado = listar_tabelas()
        assert resultado["bronze"]["exists"] is True
        assert resultado["bronze"]["rows"] == 3

    def test_tabela_existe(self, populated_bronze):
        from agents.tools.data_tools import tabela_existe
        assert tabela_existe("bronze") is True
        assert tabela_existe("silver_messages") is False

    def test_contar_linhas(self, populated_bronze):
        from agents.tools.data_tools import contar_linhas
        assert contar_linhas("bronze") == 3

    def test_ler_tabela(self, populated_bronze):
        from agents.tools.data_tools import ler_tabela
        df = ler_tabela("bronze", limite=2)
        assert len(df) == 2
        assert "conversation_id" in df.columns

    def test_obter_schema(self, populated_bronze):
        from agents.tools.data_tools import obter_schema
        schema = obter_schema("bronze")
        assert "conversation_id" in schema
        assert "timestamp" in schema

    def test_amostrar_tabela(self, populated_bronze):
        from agents.tools.data_tools import amostrar_tabela
        amostra = amostrar_tabela("bronze", n=2)
        assert len(amostra) == 2
        assert isinstance(amostra[0], dict)

    def test_resolver_path_direto(self):
        from agents.tools.data_tools import _resolver_path
        assert _resolver_path("/algum/path") == "/algum/path"

    def test_resolver_path_logico(self):
        from agents.tools.data_tools import _resolver_path
        from config.settings import get_settings
        settings = get_settings()
        assert _resolver_path("bronze") == settings.bronze_path


# ═══════════════════════════════════════════════════════════════════════════
# quality_tools
# ═══════════════════════════════════════════════════════════════════════════


class TestQualityTools:

    def test_verificar_nulos(self, populated_bronze):
        from agents.tools.quality_tools import verificar_nulos
        resultado = verificar_nulos("bronze")
        assert resultado["total_linhas"] == 3
        assert "colunas" in resultado
        assert "conversation_id" in resultado["colunas"]

    def test_verificar_duplicatas_sem_duplicatas(self, populated_bronze):
        from agents.tools.quality_tools import verificar_duplicatas
        resultado = verificar_duplicatas("bronze", colunas_chave=["conversation_id"])
        assert resultado["duplicatas"] == 0

    def test_verificar_duplicatas_total(self, tmp_path, storage):
        from agents.tools.quality_tools import verificar_duplicatas
        from config.settings import get_settings
        settings = get_settings()
        df = pl.DataFrame({
            "id": ["a", "a", "b"],
            "val": [1, 1, 2],
        })
        storage.write_table(df, settings.bronze_path)
        resultado = verificar_duplicatas("bronze", colunas_chave=["id"])
        assert resultado["duplicatas"] == 1

    def test_verificar_duplicatas_colunas_invalidas(self, populated_bronze):
        from agents.tools.quality_tools import verificar_duplicatas
        resultado = verificar_duplicatas("bronze", colunas_chave=["coluna_inexistente"])
        assert "erro" in resultado

    def test_comparar_schemas(self, populated_bronze, populated_silver_conv):
        from agents.tools.quality_tools import comparar_schemas
        resultado = comparar_schemas("bronze", "silver_conversations")
        assert "colunas_em_comum" in resultado
        assert "apenas_tabela_a" in resultado
        assert "apenas_tabela_b" in resultado
        assert resultado["schemas_identicos"] is False

    def test_verificar_integridade(self, populated_bronze):
        from agents.tools.quality_tools import verificar_integridade
        resultado = verificar_integridade("bronze")
        assert "score_saude" in resultado
        assert resultado["score_saude"] >= 0
        assert resultado["total_linhas"] == 3
        assert resultado["status"] in ("saudavel", "atencao", "critico")

    def test_verificar_integridade_score_alto(self, populated_bronze):
        from agents.tools.quality_tools import verificar_integridade
        resultado = verificar_integridade("bronze")
        # Dados sem nulos devem ter score alto
        assert resultado["score_saude"] >= 80

    def test_validar_valores(self, populated_bronze):
        from agents.tools.quality_tools import validar_valores
        resultado = validar_valores(
            "bronze",
            "sender_name",
            ["João", "Maria", "Pedro"],
        )
        assert resultado["valido"] is True
        assert resultado["valores_inesperados"] == []

    def test_validar_valores_inesperados(self, populated_bronze):
        from agents.tools.quality_tools import validar_valores
        resultado = validar_valores(
            "bronze",
            "sender_name",
            ["João"],  # Maria está ausente dos esperados
        )
        assert resultado["valido"] is False
        assert "Maria" in resultado["valores_inesperados"]

    def test_validar_valores_coluna_inexistente(self, populated_bronze):
        from agents.tools.quality_tools import validar_valores
        resultado = validar_valores("bronze", "col_fake", [])
        assert "erro" in resultado


# ═══════════════════════════════════════════════════════════════════════════
# pipeline_tools
# ═══════════════════════════════════════════════════════════════════════════


class TestPipelineTools:

    def test_obter_status_ultimo_run_vazio(self):
        from agents.tools.pipeline_tools import obter_status_ultimo_run
        resultado = obter_status_ultimo_run()
        assert resultado is None

    def test_obter_historico_runs_vazio(self):
        from agents.tools.pipeline_tools import obter_historico_runs
        resultado = obter_historico_runs()
        assert resultado == []

    def test_obter_metricas_llm(self):
        from agents.tools.pipeline_tools import obter_metricas_llm
        resultado = obter_metricas_llm()
        assert resultado["custo_total_usd"] == 0.0
        assert resultado["input_tokens"] == 0
        assert resultado["output_tokens"] == 0

    def test_obter_alertas_vazio(self):
        from agents.tools.pipeline_tools import obter_alertas
        resultado = obter_alertas()
        assert resultado == []

    def test_serializar_run(self):
        from agents.tools.pipeline_tools import _serializar_run
        from monitoring.models import PipelineRun, RunStatus
        run = PipelineRun(layers=["bronze"], trigger="test", status=RunStatus.COMPLETED)
        serializado = _serializar_run(run)
        assert serializado["status"] == "completed"
        assert serializado["layers"] == ["bronze"]
        assert serializado["trigger"] == "test"

    def test_executar_camada_mock(self):
        """Testa executar_camada com orchestrator mockado."""
        from agents.tools.pipeline_tools import executar_camada
        from monitoring.models import PipelineRun, RunStatus, StepRun

        mock_run = PipelineRun(
            layers=["bronze"],
            trigger="agent",
            status=RunStatus.COMPLETED,
        )
        mock_run.steps = [StepRun(step_name="bronze", status=RunStatus.COMPLETED, rows_output=10)]

        with patch("agents.tools.pipeline_tools.PipelineOrchestrator") as MockOrch:
            mock_instance = MagicMock()
            mock_instance.run_single_step.return_value = mock_run
            MockOrch.return_value = mock_instance

            resultado = executar_camada("bronze", trigger="test")
            assert resultado["status"] == "completed"

    def test_executar_pipeline_mock(self):
        from agents.tools.pipeline_tools import executar_pipeline
        from monitoring.models import PipelineRun, RunStatus

        mock_run = PipelineRun(
            layers=["bronze", "silver"],
            trigger="agent",
            status=RunStatus.COMPLETED,
        )

        with patch("agents.tools.pipeline_tools.PipelineOrchestrator") as MockOrch:
            mock_instance = MagicMock()
            mock_instance.run_pipeline.return_value = mock_run
            MockOrch.return_value = mock_instance

            resultado = executar_pipeline(layers=["bronze", "silver"])
            assert resultado["status"] == "completed"


# ═══════════════════════════════════════════════════════════════════════════
# pipeline_agent (LangGraph)
# ═══════════════════════════════════════════════════════════════════════════


class TestPipelineAgent:

    def test_build_graph_compiles(self):
        from agents.pipeline_agent import build_pipeline_graph
        graph = build_pipeline_graph()
        compiled = graph.compile()
        assert compiled is not None

    def test_load_spec_sem_spec(self):
        from agents.pipeline_agent import load_spec
        state = {"spec_path": "/inexistente", "spec": None}
        result = load_spec(state)
        assert result.get("etapa_atual") == "analyze_data"

    def test_load_spec_com_spec(self):
        from agents.pipeline_agent import load_spec
        from pipeline.specs import ProjectSpec
        spec = ProjectSpec(nome="test")
        state = {"spec": spec, "spec_path": ""}
        result = load_spec(state)
        assert result["etapa_atual"] == "analyze_data"

    def test_analyze_data(self):
        from agents.pipeline_agent import analyze_data
        state = {}
        result = analyze_data(state)
        assert "tabelas_status" in result

    def test_plan_com_bronze(self):
        from agents.pipeline_agent import plan
        from agents.codegen_agent import GeneratedCode, PipelineGerado

        pg = PipelineGerado(
            bronze=GeneratedCode(camada="bronze", codigo="def run(r,w,s): ...", nome_funcao="run", descricao="test"),
        )
        state = {
            "tabelas_status": {"bronze": {"exists": False}},
            "layers_a_executar": ["bronze"],
            "spec": None,
            "pipeline_gerado": pg,
        }
        result = plan(state)
        assert "bronze" in result["plano"]["layers"]

    def test_plan_silver_sem_bronze(self):
        from agents.pipeline_agent import plan
        state = {
            "tabelas_status": {"bronze": {"exists": False}},
            "layers_a_executar": ["silver"],
            "spec": None,
        }
        result = plan(state)
        # Silver não deve ser planejado se bronze não existe
        assert "silver" not in result["plano"]["layers"]

    def test_plan_silver_com_bronze_existente(self):
        from agents.pipeline_agent import plan
        from agents.codegen_agent import GeneratedCode, PipelineGerado

        pg = PipelineGerado(
            silver=GeneratedCode(camada="silver", codigo="def run(r,w,s): ...", nome_funcao="run", descricao="test"),
        )
        state = {
            "tabelas_status": {"bronze": {"exists": True}},
            "layers_a_executar": ["silver"],
            "spec": None,
            "pipeline_gerado": pg,
        }
        result = plan(state)
        assert "silver" in result["plano"]["layers"]

    def test_execute_bronze_skip(self):
        from agents.pipeline_agent import execute_bronze
        state = {"plano": {"layers": ["silver"]}}
        result = execute_bronze(state)
        assert result["etapa_atual"] == "execute_silver"

    def test_execute_silver_skip(self):
        from agents.pipeline_agent import execute_silver
        state = {"plano": {"layers": ["bronze"]}, "erro": None}
        result = execute_silver(state)
        assert result["etapa_atual"] == "execute_gold"

    def test_execute_silver_skip_on_error(self):
        from agents.pipeline_agent import execute_silver
        state = {"plano": {"layers": ["bronze", "silver"]}, "erro": "algo deu errado"}
        result = execute_silver(state)
        assert result["etapa_atual"] == "complete"

    def test_execute_gold_skip(self):
        from agents.pipeline_agent import execute_gold
        state = {"plano": {"layers": ["bronze"]}, "erro": None}
        result = execute_gold(state)
        assert result["etapa_atual"] == "validate"

    def test_execute_gold_skip_on_error(self):
        from agents.pipeline_agent import execute_gold
        state = {"plano": {"layers": ["gold"]}, "erro": "falha prévia"}
        result = execute_gold(state)
        assert result["etapa_atual"] == "complete"

    def test_validate_sem_erro(self):
        from agents.pipeline_agent import validate
        state = {"plano": {"layers": []}, "erro": None}
        result = validate(state)
        assert result["validacao"]["todas_ok"] is True

    def test_validate_com_erro(self):
        from agents.pipeline_agent import validate
        state = {"plano": {"layers": ["bronze"]}, "erro": "algo"}
        result = validate(state)
        assert result["etapa_atual"] == "complete"

    def test_complete_com_erro(self):
        from agents.pipeline_agent import complete
        state = {"erro": "test error"}
        result = complete(state)
        assert result["completo"] is True

    def test_complete_sem_erro(self):
        from agents.pipeline_agent import complete
        state = {"erro": None}
        result = complete(state)
        assert result["completo"] is True

    def test_route_after_plan_bronze(self):
        from agents.pipeline_agent import _route_after_plan
        state = {"plano": {"layers": ["bronze", "silver", "gold"]}}
        assert _route_after_plan(state) == "execute_bronze"

    def test_route_after_plan_validate(self):
        from agents.pipeline_agent import _route_after_plan
        state = {"plano": {"layers": []}}
        assert _route_after_plan(state) == "validate"

    def test_route_after_bronze_silver(self):
        from agents.pipeline_agent import _route_after_bronze
        state = {"plano": {"layers": ["bronze", "silver"]}, "erro": None}
        assert _route_after_bronze(state) == "execute_silver"

    def test_route_after_bronze_error(self):
        from agents.pipeline_agent import _route_after_bronze
        state = {"plano": {"layers": ["bronze"]}, "erro": "falha"}
        assert _route_after_bronze(state) == "try_repair"

    def test_route_after_silver_gold(self):
        from agents.pipeline_agent import _route_after_silver
        state = {"plano": {"layers": ["silver", "gold"]}, "erro": None}
        assert _route_after_silver(state) == "execute_gold"

    def test_route_after_gold_validate(self):
        from agents.pipeline_agent import _route_after_gold
        state = {"erro": None}
        assert _route_after_gold(state) == "validate"

    def test_route_after_gold_error(self):
        from agents.pipeline_agent import _route_after_gold
        state = {"erro": "falhou"}
        assert _route_after_gold(state) == "try_repair"

    def test_create_pipeline_agent(self):
        from agents.pipeline_agent import create_pipeline_agent
        agent = create_pipeline_agent()
        assert agent is not None

    def test_run_pipeline_agent_no_data(self):
        """Pipeline agent completo sem dados — deve concluir sem erros fatais."""
        from agents.pipeline_agent import run_pipeline_agent
        # Mock executar_camada para não depender de dados reais
        with patch("agents.tools.pipeline_tools.PipelineOrchestrator") as MockOrch:
            from monitoring.models import PipelineRun, RunStatus, StepRun
            mock_run = PipelineRun(layers=["bronze"], trigger="pipeline_agent", status=RunStatus.COMPLETED)
            mock_run.steps = [StepRun(step_name="bronze", status=RunStatus.COMPLETED, rows_output=5)]
            mock_instance = MagicMock()
            mock_instance.run_single_step.return_value = mock_run
            mock_instance.run_pipeline.return_value = mock_run
            MockOrch.return_value = mock_instance

            result = run_pipeline_agent(layers=["bronze"])
            assert result["completo"] is True


# ═══════════════════════════════════════════════════════════════════════════
# monitor_agent (LangGraph)
# ═══════════════════════════════════════════════════════════════════════════


class TestMonitorAgent:

    def test_build_graph_compiles(self):
        from agents.monitor_agent import build_monitor_graph
        graph = build_monitor_graph()
        compiled = graph.compile()
        assert compiled is not None

    def test_check_new_data_sem_tabelas(self):
        from agents.monitor_agent import check_new_data
        state = {}
        result = check_new_data(state)
        assert result["novos_dados_detectados"] is False

    def test_check_new_data_com_bronze(self, populated_bronze):
        from agents.monitor_agent import check_new_data
        state = {}
        result = check_new_data(state)
        # Bronze existe mas silver não → novos dados
        assert result["novos_dados_detectados"] is True

    def test_check_new_data_completo(self, populated_bronze, populated_silver_conv):
        from agents.monitor_agent import check_new_data
        state = {}
        result = check_new_data(state)
        # Ambas existem com dados
        assert "tabelas_status" in result

    def test_check_health_vazio(self):
        from agents.monitor_agent import check_health
        state = {"tabelas_status": {}}
        result = check_health(state)
        assert result["problemas_saude"] == []

    def test_check_health_com_tabela(self, populated_bronze):
        from agents.monitor_agent import check_health
        state = {"tabelas_status": {"bronze": {"exists": True, "rows": 3}}}
        result = check_health(state)
        assert "bronze" in result["saude_tabelas"]

    def test_decide_sem_novos_dados(self):
        from agents.monitor_agent import decide
        state = {"novos_dados_detectados": False, "problemas_saude": []}
        result = decide(state)
        assert result["deve_executar_pipeline"] is False

    def test_decide_com_novos_dados(self):
        from agents.monitor_agent import decide
        state = {"novos_dados_detectados": True, "problemas_saude": []}
        result = decide(state)
        assert result["deve_executar_pipeline"] is True

    def test_trigger_pipeline_nao_executa(self):
        from agents.monitor_agent import trigger_pipeline
        state = {"deve_executar_pipeline": False}
        result = trigger_pipeline(state)
        assert result["pipeline_disparado"] is False

    def test_check_continue_para(self):
        from agents.monitor_agent import check_continue
        state = {"ciclos_executados": 0, "max_ciclos": 1}
        result = check_continue(state)
        assert result["parar"] is True

    def test_check_continue_continua(self):
        from agents.monitor_agent import check_continue
        state = {"ciclos_executados": 0, "max_ciclos": 3}
        result = check_continue(state)
        assert result["parar"] is False

    def test_route_continue_end(self):
        from agents.monitor_agent import _route_continue
        state = {"parar": True}
        assert _route_continue(state) == "end"

    def test_route_continue_loop(self):
        from agents.monitor_agent import _route_continue
        state = {"parar": False}
        assert _route_continue(state) == "check_new_data"

    def test_create_monitor_agent(self):
        from agents.monitor_agent import create_monitor_agent
        agent = create_monitor_agent()
        assert agent is not None

    def test_run_monitor_agent_1_ciclo(self):
        from agents.monitor_agent import run_monitor_agent
        result = run_monitor_agent(max_ciclos=1)
        assert result["ciclos_executados"] == 1
        assert result["parar"] is True


# ═══════════════════════════════════════════════════════════════════════════
# repair_agent (LangGraph)
# ═══════════════════════════════════════════════════════════════════════════


class TestRepairAgent:

    def test_build_graph_compiles(self):
        from agents.repair_agent import build_repair_graph
        graph = build_repair_graph()
        compiled = graph.compile()
        assert compiled is not None

    def test_get_error(self):
        from agents.repair_agent import get_error
        state = {"erro_original": "Test error", "camada_falha": "bronze"}
        result = get_error(state)
        assert result["analise_erro"]["erro"] == "Test error"
        assert result["analise_erro"]["camada"] == "bronze"

    def test_heuristicas_file_not_found(self):
        from agents.repair_agent import _analisar_com_heuristicas
        result = _analisar_com_heuristicas(
            "FileNotFoundError: no such file", "bronze", {}
        )
        assert result["estrategia"] == "no_action"

    def test_heuristicas_schema(self):
        from agents.repair_agent import _analisar_com_heuristicas
        result = _analisar_com_heuristicas(
            "Schema mismatch on column X", "silver", {}
        )
        assert result["estrategia"] == "regenerate_code"

    def test_heuristicas_permission(self):
        from agents.repair_agent import _analisar_com_heuristicas
        result = _analisar_com_heuristicas(
            "Permission denied", "bronze", {}
        )
        assert result["estrategia"] == "no_action"

    def test_heuristicas_silver_sem_bronze(self):
        from agents.repair_agent import _analisar_com_heuristicas
        result = _analisar_com_heuristicas(
            "tabela vazia", "silver",
            {"tabelas": {"bronze": {"exists": False}}},
        )
        assert result["estrategia"] == "retry_from_start"

    def test_heuristicas_gold_sem_silver(self):
        from agents.repair_agent import _analisar_com_heuristicas
        result = _analisar_com_heuristicas(
            "tabela vazia", "gold",
            {"tabelas": {"silver_conversations": {"exists": False}}},
        )
        assert result["estrategia"] == "retry_from_start"

    def test_heuristicas_erro_generico(self):
        from agents.repair_agent import _analisar_com_heuristicas
        result = _analisar_com_heuristicas("algo inesperado", "bronze", {})
        assert result["estrategia"] == "regenerate_code"

    def test_analyze_sem_llm(self):
        from agents.repair_agent import analyze
        state = {
            "analise_erro": {
                "erro": "FileNotFoundError: test",
                "camada": "bronze",
                "tabelas": {},
            },
        }
        result = analyze(state)
        assert "estrategia" in result["analise_erro"]

    def test_propose_fix_retry(self):
        from agents.repair_agent import propose_fix
        state = {
            "analise_erro": {"estrategia": "retry", "camada": "bronze"},
            "camada_falha": "bronze",
        }
        result = propose_fix(state)
        assert result["proposta_fix"]["estrategia"] == "retry"
        assert result["proposta_fix"]["parametros"]["layers"] == ["bronze"]

    def test_propose_fix_skip(self):
        from agents.repair_agent import propose_fix
        state = {
            "analise_erro": {"estrategia": "skip_layer", "camada": "silver"},
            "camada_falha": "silver",
        }
        result = propose_fix(state)
        assert result["proposta_fix"]["parametros"]["layers"] == ["gold"]

    def test_propose_fix_retry_from_start(self):
        from agents.repair_agent import propose_fix
        state = {
            "analise_erro": {"estrategia": "retry_from_start", "camada": "silver"},
            "camada_falha": "silver",
        }
        result = propose_fix(state)
        assert result["proposta_fix"]["parametros"]["layers"] == ["bronze", "silver", "gold"]

    def test_apply_fix_no_action(self):
        from agents.repair_agent import apply_fix
        state = {"proposta_fix": {"estrategia": "no_action", "parametros": {}}}
        result = apply_fix(state)
        assert result["fix_aplicado"] is False

    def test_apply_fix_retry(self):
        from agents.repair_agent import apply_fix
        state = {
            "proposta_fix": {
                "estrategia": "retry",
                "parametros": {"layers": ["bronze"]},
            }
        }
        result = apply_fix(state)
        assert result["fix_aplicado"] is True

    def test_retry_sem_fix(self):
        from agents.repair_agent import retry
        state = {"fix_aplicado": False, "proposta_fix": {}}
        result = retry(state)
        assert result["resultado_retry"] is None

    def test_retry_sem_layers(self):
        from agents.repair_agent import retry
        state = {
            "fix_aplicado": True,
            "proposta_fix": {"parametros": {"layers": []}},
        }
        result = retry(state)
        assert result["resultado_retry"]["status"] == "completed"

    def test_retry_com_mock(self):
        from agents.repair_agent import retry
        from monitoring.models import PipelineRun, RunStatus

        mock_run = PipelineRun(layers=["bronze"], trigger="repair_agent", status=RunStatus.COMPLETED)

        with patch("agents.tools.pipeline_tools.PipelineOrchestrator") as MockOrch:
            mock_instance = MagicMock()
            mock_instance.run_pipeline.return_value = mock_run
            MockOrch.return_value = mock_instance

            state = {
                "fix_aplicado": True,
                "proposta_fix": {"parametros": {"layers": ["bronze"]}},
            }
            result = retry(state)
            assert result["resultado_retry"]["status"] == "completed"

    def test_validate_repair_sem_resultado(self):
        from agents.repair_agent import validate_repair
        state = {"resultado_retry": None, "tentativas": 0, "max_tentativas": 2}
        result = validate_repair(state)
        assert result["reparado"] is False

    def test_validate_repair_sucesso(self):
        from agents.repair_agent import validate_repair
        state = {
            "resultado_retry": {"status": "completed"},
            "tentativas": 0,
            "max_tentativas": 2,
        }
        result = validate_repair(state)
        assert result["reparado"] is True

    def test_validate_repair_falha(self):
        from agents.repair_agent import validate_repair
        state = {
            "resultado_retry": {"status": "failed"},
            "tentativas": 0,
            "max_tentativas": 2,
        }
        result = validate_repair(state)
        assert result["reparado"] is False

    def test_route_after_validate_reparado(self):
        from agents.repair_agent import _route_after_validate
        state = {"reparado": True, "tentativas": 1, "max_tentativas": 2}
        assert _route_after_validate(state) == "end"

    def test_route_after_validate_esgotou(self):
        from agents.repair_agent import _route_after_validate
        state = {"reparado": False, "tentativas": 2, "max_tentativas": 2}
        assert _route_after_validate(state) == "end"

    def test_route_after_validate_retry(self):
        from agents.repair_agent import _route_after_validate
        state = {"reparado": False, "tentativas": 1, "max_tentativas": 3}
        assert _route_after_validate(state) == "analyze"

    def test_create_repair_agent(self):
        from agents.repair_agent import create_repair_agent
        agent = create_repair_agent()
        assert agent is not None

    def test_run_repair_agent_no_action(self):
        """Repair agent com erro sem ação → deve finalizar sem reparar."""
        from agents.repair_agent import run_repair_agent
        result = run_repair_agent(
            erro="PermissionError: permission denied",
            camada_falha="bronze",
            max_tentativas=1,
        )
        assert result["reparado"] is False

    def test_run_repair_agent_retry_mock(self):
        """Repair agent com regenerate_code mockado bem-sucedido."""
        from agents.repair_agent import run_repair_agent
        from monitoring.models import PipelineRun, RunStatus

        mock_run = PipelineRun(layers=["bronze"], trigger="repair_agent", status=RunStatus.COMPLETED)

        with patch("agents.tools.pipeline_tools.PipelineOrchestrator") as MockOrch, \
             patch("agents.codegen_agent.CodeGenAgent") as MockCodeGen, \
             patch("pipeline.specs.carregar_spec") as mock_spec, \
             patch("pipeline.specs.spec_existe", return_value=True), \
             patch("agents.codegen_agent.carregar_pipeline_gerado") as mock_pg, \
             patch("agents.codegen_agent.salvar_pipeline_gerado"):
            mock_instance = MagicMock()
            mock_instance.run_pipeline.return_value = mock_run
            MockOrch.return_value = mock_instance

            # Mock codegen agent regeneração
            from agents.codegen_agent import GeneratedCode
            mock_agent = MagicMock()
            mock_agent.regenerar_camada.return_value = GeneratedCode(
                camada="bronze", codigo="def run(r,w,s): return {'rows_written': 1}",
                nome_funcao="run", descricao="regenerated",
            )
            MockCodeGen.return_value = mock_agent

            # Mock spec e pipeline_gerado
            mock_spec.return_value = MagicMock()
            from agents.codegen_agent import PipelineGerado
            mock_pg.return_value = PipelineGerado()

            result = run_repair_agent(
                erro="timeout na conexão",
                camada_falha="bronze",
                max_tentativas=2,
            )
            assert result["reparado"] is True


# ═══════════════════════════════════════════════════════════════════════════
# Integration: Agents register actions in MonitoringStore
# ═══════════════════════════════════════════════════════════════════════════


class TestAgentMonitoringIntegration:

    def test_pipeline_agent_registra_acoes(self):
        """Pipeline agent deve registrar ações no MonitoringStore."""
        from agents.pipeline_agent import run_pipeline_agent
        from monitoring.store import get_monitoring_store

        with patch("agents.tools.pipeline_tools.PipelineOrchestrator") as MockOrch:
            from monitoring.models import PipelineRun, RunStatus, StepRun
            mock_run = PipelineRun(layers=["bronze"], trigger="pipeline_agent", status=RunStatus.COMPLETED)
            mock_run.steps = [StepRun(step_name="bronze", status=RunStatus.COMPLETED, rows_output=5)]
            mock_instance = MagicMock()
            mock_instance.run_single_step.return_value = mock_run
            MockOrch.return_value = mock_instance

            run_pipeline_agent(layers=["bronze"])

        store = get_monitoring_store()
        acoes = store.get_agent_actions(agent_name="pipeline_agent")
        assert len(acoes) > 0

    def test_monitor_agent_registra_acoes(self):
        """Monitor agent deve registrar ações no MonitoringStore."""
        from agents.monitor_agent import run_monitor_agent
        from monitoring.store import get_monitoring_store

        run_monitor_agent(max_ciclos=1)

        store = get_monitoring_store()
        acoes = store.get_agent_actions(agent_name="monitor_agent")
        assert len(acoes) > 0

    def test_repair_agent_registra_acoes(self):
        """Repair agent deve registrar ações no MonitoringStore."""
        from agents.repair_agent import run_repair_agent
        from monitoring.store import get_monitoring_store

        run_repair_agent(
            erro="PermissionError: denied",
            camada_falha="bronze",
            max_tentativas=1,
        )

        store = get_monitoring_store()
        acoes = store.get_agent_actions(agent_name="repair_agent")
        assert len(acoes) > 0

    def test_repair_agent_cria_alerta(self):
        """Repair agent deve criar alertas para erros sem ação."""
        from agents.repair_agent import run_repair_agent
        from monitoring.store import get_monitoring_store

        run_repair_agent(
            erro="PermissionError: denied",
            camada_falha="bronze",
            max_tentativas=1,
        )

        store = get_monitoring_store()
        alertas = store.get_alerts(unresolved_only=False)
        assert len(alertas) > 0
        assert any("repair_agent" in a.source for a in alertas)
