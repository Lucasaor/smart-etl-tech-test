"""Tests for Phase 1 — Foundation (config, core, agents/llm_provider, specs)."""

import json
import os
import tempfile
from pathlib import Path

import polars as pl
import pytest


class TestSettings:
    """Tests for config/settings.py."""

    def test_default_settings(self):
        from config.settings import Settings

        s = Settings()
        assert s.runtime_env.value == "local"
        assert s.is_local is True
        assert s.is_databricks is False

    def test_derived_paths_local(self):
        from config.settings import Settings

        s = Settings(data_root="/tmp/test_data")
        assert s.bronze_path == "/tmp/test_data/bronze"
        assert s.silver_path == "/tmp/test_data/silver"
        assert s.gold_path == "/tmp/test_data/gold"

    def test_derived_paths_databricks(self):
        from config.settings import Settings

        s = Settings(runtime_env="databricks")
        assert s.bronze_path == "/mnt/delta/bronze"
        assert s.is_databricks is True


class TestLLMConfig:
    """Tests for config/llm_config.py."""

    def test_default_config(self):
        from config.llm_config import get_llm_config

        cfg = get_llm_config()
        assert cfg.primary.model is not None
        assert len(cfg.fallback_chain) >= 1

    def test_cost_tracking(self):
        from config.llm_config import LLMConfig, ModelSpec

        spec = ModelSpec(
            model="gpt-4o-mini",
            cost_per_1k_input=0.00015,
            cost_per_1k_output=0.0006,
        )
        cfg = LLMConfig(primary=spec, max_cost_per_run=1.0)

        cost = cfg.track_cost(input_tokens=1000, output_tokens=500, model="gpt-4o-mini")
        assert cost > 0
        assert cfg.accumulated_cost == cost
        assert cfg.budget_exceeded is False

    def test_budget_exceeded(self):
        from config.llm_config import LLMConfig, ModelSpec

        spec = ModelSpec(model="test", cost_per_1k_input=1.0, cost_per_1k_output=1.0)
        cfg = LLMConfig(primary=spec, max_cost_per_run=0.001)
        cfg.track_cost(input_tokens=1000, output_tokens=1000, model="test")
        assert cfg.budget_exceeded is True


class TestLocalDeltaBackend:
    """Tests for core/storage.py — LocalDeltaBackend."""

    def test_write_and_read(self, tmp_path):
        from core.storage import LocalDeltaBackend

        backend = LocalDeltaBackend()
        table_path = str(tmp_path / "test_table")

        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        backend.write_table(df, table_path)

        assert backend.table_exists(table_path) is True

        result = backend.read_table(table_path)
        assert len(result) == 3
        assert result.columns == ["id", "name"]

    def test_append_mode(self, tmp_path):
        from core.storage import LocalDeltaBackend

        backend = LocalDeltaBackend()
        table_path = str(tmp_path / "append_table")

        df1 = pl.DataFrame({"x": [1, 2]})
        backend.write_table(df1, table_path, mode="overwrite")

        df2 = pl.DataFrame({"x": [3, 4]})
        backend.write_table(df2, table_path, mode="append")

        result = backend.read_table(table_path)
        assert len(result) == 4

    def test_table_version(self, tmp_path):
        from core.storage import LocalDeltaBackend

        backend = LocalDeltaBackend()
        table_path = str(tmp_path / "version_table")

        df = pl.DataFrame({"v": [1]})
        backend.write_table(df, table_path)
        v0 = backend.get_table_version(table_path)

        backend.write_table(df, table_path, mode="append")
        v1 = backend.get_table_version(table_path)

        assert v1 == v0 + 1

    def test_table_not_exists(self, tmp_path):
        from core.storage import LocalDeltaBackend

        backend = LocalDeltaBackend()
        assert backend.table_exists(str(tmp_path / "nonexistent")) is False

    def test_row_count(self, tmp_path):
        from core.storage import LocalDeltaBackend

        backend = LocalDeltaBackend()
        table_path = str(tmp_path / "count_table")
        df = pl.DataFrame({"n": list(range(100))})
        backend.write_table(df, table_path)
        assert backend.get_table_row_count(table_path) == 100


class TestPolarsCompute:
    """Tests for core/compute.py — PolarsCompute."""

    def test_sql_query(self):
        from core.compute import PolarsCompute

        compute = PolarsCompute()
        df = pl.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        result = compute.sql("SELECT a, b, a + b AS c FROM data", {"data": df})
        assert "c" in result.columns
        assert result["c"].to_list() == [11, 22, 33]

    def test_read_parquet(self, tmp_path):
        from core.compute import PolarsCompute

        compute = PolarsCompute()
        path = str(tmp_path / "test.parquet")
        pl.DataFrame({"x": [1, 2]}).write_parquet(path)

        result = compute.read_parquet(path)
        assert len(result) == 2


class TestEventBus:
    """Tests for core/events.py."""

    def test_emit_and_subscribe(self):
        from core.events import Event, EventBus, EventType

        bus = EventBus()
        received = []

        bus.subscribe(EventType.STEP_COMPLETED, lambda e: received.append(e))
        bus.emit(Event(event_type=EventType.STEP_COMPLETED, data={"step": "bronze"}))

        assert len(received) == 1
        assert received[0].data["step"] == "bronze"

    def test_wildcard_subscriber(self):
        from core.events import Event, EventBus, EventType

        bus = EventBus()
        received = []

        bus.subscribe(None, lambda e: received.append(e))
        bus.emit(Event(event_type=EventType.PIPELINE_STARTED))
        bus.emit(Event(event_type=EventType.STEP_COMPLETED))

        assert len(received) == 2

    def test_subscriber_error_does_not_propagate(self):
        from core.events import Event, EventBus, EventType

        bus = EventBus()
        bus.subscribe(EventType.STEP_FAILED, lambda e: 1 / 0)  # Will raise

        # Should not raise
        bus.emit(Event(event_type=EventType.STEP_FAILED))


class TestMonitoringStore:
    """Tests for monitoring/store.py."""

    def test_save_and_get_pipeline_run(self, tmp_path):
        from monitoring.models import PipelineRun, RunStatus, StepRun
        from monitoring.store import MonitoringStore

        store = MonitoringStore(str(tmp_path / "test.db"))

        run = PipelineRun(
            run_id="test_run_001",
            status=RunStatus.COMPLETED,
            layers=["bronze", "silver"],
            steps=[StepRun(step_name="bronze", status=RunStatus.COMPLETED, rows_output=100)],
        )
        store.save_pipeline_run(run)

        runs = store.get_pipeline_runs()
        assert len(runs) == 1
        assert runs[0].run_id == "test_run_001"
        assert len(runs[0].steps) == 1

    def test_save_and_get_agent_action(self, tmp_path):
        from monitoring.models import AgentAction
        from monitoring.store import MonitoringStore

        store = MonitoringStore(str(tmp_path / "test.db"))

        action = AgentAction(
            agent_name="pipeline_agent",
            action="execute_bronze",
            model_used="gpt-4o-mini",
            input_tokens=500,
            output_tokens=200,
            cost=0.001,
        )
        store.save_agent_action(action)

        actions = store.get_agent_actions()
        assert len(actions) == 1
        assert actions[0].agent_name == "pipeline_agent"

    def test_save_and_get_alert(self, tmp_path):
        from monitoring.models import Alert, AlertSeverity
        from monitoring.store import MonitoringStore

        store = MonitoringStore(str(tmp_path / "test.db"))

        alert = Alert(
            severity=AlertSeverity.WARNING,
            title="Schema drift detected",
            source="monitor_agent",
        )
        store.save_alert(alert)

        alerts = store.get_alerts()
        assert len(alerts) == 1
        assert alerts[0].severity == AlertSeverity.WARNING


class TestSettings_SpecPath:
    """Tests for spec_path in config/settings.py."""

    def test_spec_path_default_local(self):
        from config.settings import Settings

        s = Settings(data_root="/tmp/test_data")
        assert s.spec_path == "/tmp/test_data/specs"

    def test_spec_path_custom(self):
        from config.settings import Settings

        s = Settings(data_root="/tmp/test_data", spec_dir="/custom/specs")
        assert s.spec_path == "/custom/specs"

    def test_spec_path_databricks(self):
        from config.settings import Settings

        s = Settings(runtime_env="databricks")
        assert s.spec_path == "/mnt/delta/specs"


class TestProjectSpec:
    """Tests for pipeline/specs.py — ProjectSpec model."""

    def test_spec_valida_com_tudo(self, tmp_path):
        from pipeline.specs import ProjectSpec

        # Criar arquivo de dados simulado
        dados_path = str(tmp_path / "dados.parquet")
        pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_parquet(dados_path)

        spec = ProjectSpec(
            nome="teste",
            dados_brutos_path=dados_path,
            dicionario_dados="## Coluna a\nTipo: int",
            descricao_kpis="## KPI 1\nContagem total",
        )
        assert spec.is_valida
        assert spec.validar() == []

    def test_spec_invalida_sem_dados(self):
        from pipeline.specs import ProjectSpec

        spec = ProjectSpec(
            nome="teste",
            dados_brutos_path="",
            dicionario_dados="dicionário",
            descricao_kpis="kpis",
        )
        problemas = spec.validar()
        assert len(problemas) >= 1
        assert any("dados brutos" in p.lower() for p in problemas)

    def test_spec_invalida_sem_dicionario(self, tmp_path):
        from pipeline.specs import ProjectSpec

        dados_path = str(tmp_path / "d.parquet")
        pl.DataFrame({"x": [1]}).write_parquet(dados_path)

        spec = ProjectSpec(
            dados_brutos_path=dados_path,
            dicionario_dados="",
            descricao_kpis="kpis",
        )
        problemas = spec.validar()
        assert any("dicionário" in p.lower() for p in problemas)

    def test_spec_invalida_sem_kpis(self, tmp_path):
        from pipeline.specs import ProjectSpec

        dados_path = str(tmp_path / "d.parquet")
        pl.DataFrame({"x": [1]}).write_parquet(dados_path)

        spec = ProjectSpec(
            dados_brutos_path=dados_path,
            dicionario_dados="dict",
            descricao_kpis="",
        )
        problemas = spec.validar()
        assert any("kpi" in p.lower() for p in problemas)

    def test_spec_invalida_arquivo_inexistente(self):
        from pipeline.specs import ProjectSpec

        spec = ProjectSpec(
            dados_brutos_path="/nao/existe.parquet",
            dicionario_dados="dict",
            descricao_kpis="kpis",
        )
        problemas = spec.validar()
        assert any("não encontrado" in p.lower() for p in problemas)


class TestAnalisarAmostra:
    """Tests for pipeline/specs.py — analisar_amostra."""

    def test_analisar_parquet(self, tmp_path):
        from pipeline.specs import analisar_amostra

        dados_path = str(tmp_path / "dados.parquet")
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "nome": ["Ana", "Bob", None, "Carlos", "Diana"],
            "valor": [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        df.write_parquet(dados_path)

        analise = analisar_amostra(dados_path)
        assert analise.num_linhas == 5
        assert analise.num_colunas == 3
        assert len(analise.colunas) == 3

        # Verificar detalhes de coluna
        col_nome = next(c for c in analise.colunas if c.nome == "nome")
        assert col_nome.nulos == 1
        assert col_nome.nulos_pct == 20.0

    def test_analisar_csv(self, tmp_path):
        from pipeline.specs import analisar_amostra

        dados_path = str(tmp_path / "dados.csv")
        pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dados_path)

        analise = analisar_amostra(dados_path)
        assert analise.num_linhas == 2
        assert analise.num_colunas == 2

    def test_analisar_arquivo_inexistente(self):
        from pipeline.specs import analisar_amostra

        with pytest.raises(FileNotFoundError):
            analisar_amostra("/nao/existe.parquet")

    def test_resumo_nao_vazio(self, tmp_path):
        from pipeline.specs import analisar_amostra

        dados_path = str(tmp_path / "dados.parquet")
        pl.DataFrame({"a": [1]}).write_parquet(dados_path)

        analise = analisar_amostra(dados_path)
        resumo = analise.resumo()
        assert "Linhas:" in resumo
        assert "Colunas:" in resumo


class TestSalvarCarregarSpec:
    """Tests for pipeline/specs.py — salvar_spec e carregar_spec."""

    def test_salvar_e_carregar_spec(self, tmp_path):
        from pipeline.specs import (
            AnaliseAmostra,
            ColunaInfo,
            ProjectSpec,
            carregar_spec,
            salvar_spec,
        )

        spec_dir = str(tmp_path / "specs")
        spec = ProjectSpec(
            nome="meu_projeto",
            dados_brutos_path="/dados/amostra.parquet",
            dicionario_dados="## Coluna A\nDescrição da coluna A",
            descricao_kpis="## KPI 1\nTotal de vendas",
            analise=AnaliseAmostra(
                num_linhas=100,
                num_colunas=3,
                colunas=[
                    ColunaInfo(nome="id", tipo="Int64", valores_unicos=100),
                    ColunaInfo(nome="nome", tipo="Utf8", nulos=5, nulos_pct=5.0),
                ],
            ),
        )

        salvar_spec(spec, spec_dir)

        # Verificar arquivos criados
        assert (Path(spec_dir) / "spec_meta.json").exists()
        assert (Path(spec_dir) / "dicionario_dados.md").exists()
        assert (Path(spec_dir) / "descricao_kpis.md").exists()

        # Carregar e comparar
        loaded = carregar_spec(spec_dir)
        assert loaded.nome == "meu_projeto"
        assert loaded.dicionario_dados == "## Coluna A\nDescrição da coluna A"
        assert loaded.descricao_kpis == "## KPI 1\nTotal de vendas"
        assert loaded.analise is not None
        assert loaded.analise.num_linhas == 100
        assert len(loaded.analise.colunas) == 2

    def test_carregar_spec_diretorio_inexistente(self):
        from pipeline.specs import carregar_spec

        with pytest.raises(FileNotFoundError):
            carregar_spec("/nao/existe")

    def test_spec_existe(self, tmp_path):
        from pipeline.specs import ProjectSpec, salvar_spec, spec_existe

        spec_dir = str(tmp_path / "specs")
        assert spec_existe(spec_dir) is False

        spec = ProjectSpec(
            dados_brutos_path="/x",
            dicionario_dados="d",
            descricao_kpis="k",
        )
        salvar_spec(spec, spec_dir)
        assert spec_existe(spec_dir) is True
