"""Tests for Phase 1 — Foundation (config, core, agents/llm_provider)."""

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
