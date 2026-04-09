"""Tests for Phase 2 — Bronze Layer (ingestion, simulator, orchestrator)."""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest


# --- Fixtures ---

@pytest.fixture
def sample_parquet(tmp_path) -> str:
    """Create a minimal parquet file matching the source schema."""
    df = pl.DataFrame({
        "message_id": ["msg001", "msg002", "msg003", "msg004"],
        "conversation_id": ["conv_001", "conv_001", "conv_002", "conv_002"],
        "timestamp": [
            "2026-02-01 09:00:00",
            "2026-02-01 09:01:30",
            "2026-02-01 10:00:00",
            "2026-02-01 10:05:00",
        ],
        "direction": ["outbound", "inbound", "outbound", "inbound"],
        "sender_phone": ["+5511999990001", "+5511888880001", "+5511999990001", "+5511888880002"],
        "sender_name": ["Agent Carlos", "Ana Paula", "Agent Carlos", "joao silva"],
        "message_type": ["text", "text", "text", "audio"],
        "message_body": [
            "Olá, tudo bem? Vi que você tem interesse em seguro.",
            "Oi! Sim, tenho um Gol 2019. Quanto fica?",
            "Bom dia! Vamos fazer uma cotação?",
            "tenho um hb20 2022, placa ABC1D23",
        ],
        "status": ["delivered", "delivered", "sent", "delivered"],
        "channel": ["whatsapp", "whatsapp", "whatsapp", "whatsapp"],
        "campaign_id": ["camp_landing_fev2026", "camp_landing_fev2026", "camp_google_fev2026", "camp_google_fev2026"],
        "agent_id": ["agent_carlos_01", "agent_carlos_01", "agent_carlos_01", "agent_carlos_01"],
        "conversation_outcome": ["perdido_preco", "perdido_preco", "em_negociacao", "em_negociacao"],
        "metadata": [
            '{"device": "desktop", "city": "Sao Paulo", "state": "SP", "response_time_sec": null, "is_business_hours": true, "lead_source": "google_ads"}',
            '{"device": "android", "city": "Sao Paulo", "state": "SP", "response_time_sec": 90, "is_business_hours": true, "lead_source": "google_ads"}',
            '{"device": "desktop", "city": "Rio de Janeiro", "state": "RJ", "response_time_sec": null, "is_business_hours": true, "lead_source": "facebook"}',
            '{"device": "iphone", "city": "Rio de Janeiro", "state": "RJ", "response_time_sec": 300, "is_business_hours": true, "lead_source": "facebook"}',
        ],
    })
    path = str(tmp_path / "test_source.parquet")
    df.write_parquet(path)
    return path


@pytest.fixture
def sample_parquet_batch2(tmp_path) -> str:
    """Create a second batch of data (later timestamps) for incremental test."""
    df = pl.DataFrame({
        "message_id": ["msg005", "msg006"],
        "conversation_id": ["conv_003", "conv_003"],
        "timestamp": [
            "2026-02-02 14:00:00",
            "2026-02-02 14:03:00",
        ],
        "direction": ["outbound", "inbound"],
        "sender_phone": ["+5511999990002", "+5511888880003"],
        "sender_name": ["Agent Maria", "Pedro Santos"],
        "message_type": ["text", "text"],
        "message_body": [
            "Boa tarde! Gostaria de fazer uma cotação?",
            "Quero cotar um Corolla 2021",
        ],
        "status": ["delivered", "read"],
        "channel": ["whatsapp", "whatsapp"],
        "campaign_id": ["camp_sms_fev2026", "camp_sms_fev2026"],
        "agent_id": ["agent_maria_03", "agent_maria_03"],
        "conversation_outcome": ["venda_fechada", "venda_fechada"],
        "metadata": [
            '{"device": "desktop", "city": "Curitiba", "state": "PR", "response_time_sec": null, "is_business_hours": true, "lead_source": "referral"}',
            '{"device": "android", "city": "Curitiba", "state": "PR", "response_time_sec": 180, "is_business_hours": true, "lead_source": "referral"}',
        ],
    })
    path = str(tmp_path / "test_batch2.parquet")
    df.write_parquet(path)
    return path


class TestSchemaValidation:
    def test_valid_schema(self, sample_parquet):
        from pipeline.bronze.ingestion import validate_schema

        df = pl.read_parquet(sample_parquet)
        issues = validate_schema(df)
        assert issues == []

    def test_missing_column(self, sample_parquet):
        from pipeline.bronze.ingestion import validate_schema

        df = pl.read_parquet(sample_parquet).drop("message_body")
        issues = validate_schema(df)
        assert len(issues) == 1
        assert "message_body" in issues[0]

    def test_empty_dataframe(self, sample_parquet):
        from pipeline.bronze.ingestion import validate_schema

        df = pl.read_parquet(sample_parquet).head(0)
        issues = validate_schema(df)
        assert any("empty" in i.lower() for i in issues)


class TestParseMetadata:
    def test_metadata_expansion(self, sample_parquet):
        from pipeline.bronze.ingestion import parse_metadata

        df = pl.read_parquet(sample_parquet)
        result = parse_metadata(df)

        # Original metadata column preserved
        assert "metadata" in result.columns

        # New typed columns created
        assert "meta_device" in result.columns
        assert "meta_city" in result.columns
        assert "meta_state" in result.columns
        assert "meta_response_time_sec" in result.columns
        assert "meta_is_business_hours" in result.columns
        assert "meta_lead_source" in result.columns

        # Check values
        assert result["meta_city"][0] == "Sao Paulo"
        assert result["meta_state"][2] == "RJ"
        assert result["meta_response_time_sec"][0] is None  # null in source
        assert result["meta_response_time_sec"][1] == 90


class TestCastTypes:
    def test_timestamp_cast(self, sample_parquet):
        from pipeline.bronze.ingestion import cast_types

        df = pl.read_parquet(sample_parquet)
        result = cast_types(df)
        assert result["timestamp"].dtype == pl.Datetime


class TestBronzeIngestion:
    def test_full_ingestion(self, sample_parquet, tmp_path):
        from pipeline.bronze.ingestion import ingest_bronze

        bronze_path = str(tmp_path / "bronze_table")
        result = ingest_bronze(source_path=sample_parquet, bronze_path=bronze_path, mode="full")

        assert result["status"] == "success"
        assert result["rows_written"] == 4
        assert result["total_rows"] == 4
        assert result["mode"] == "full"
        assert result["delta_version"] == 0

    def test_auto_mode_first_run(self, sample_parquet, tmp_path):
        from pipeline.bronze.ingestion import ingest_bronze

        bronze_path = str(tmp_path / "auto_table")
        result = ingest_bronze(source_path=sample_parquet, bronze_path=bronze_path, mode="auto")

        assert result["mode"] == "full"
        assert result["rows_written"] == 4

    def test_incremental_ingestion(self, sample_parquet, sample_parquet_batch2, tmp_path):
        from pipeline.bronze.ingestion import ingest_bronze

        bronze_path = str(tmp_path / "incr_table")

        # First load
        r1 = ingest_bronze(source_path=sample_parquet, bronze_path=bronze_path, mode="full")
        assert r1["total_rows"] == 4

        # Incremental with new data
        r2 = ingest_bronze(source_path=sample_parquet_batch2, bronze_path=bronze_path, mode="incremental")
        assert r2["mode"] == "incremental"
        assert r2["rows_written"] == 2
        assert r2["total_rows"] == 6

    def test_incremental_no_new_data(self, sample_parquet, tmp_path):
        from pipeline.bronze.ingestion import ingest_bronze

        bronze_path = str(tmp_path / "nodup_table")

        # First load
        ingest_bronze(source_path=sample_parquet, bronze_path=bronze_path, mode="full")

        # Re-ingest same data
        r = ingest_bronze(source_path=sample_parquet, bronze_path=bronze_path, mode="incremental")
        assert r["rows_written"] == 0
        assert r["status"] == "no_new_data"

    def test_output_has_metadata_columns(self, sample_parquet, tmp_path):
        from core.storage import LocalDeltaBackend
        from pipeline.bronze.ingestion import ingest_bronze

        bronze_path = str(tmp_path / "meta_table")
        ingest_bronze(source_path=sample_parquet, bronze_path=bronze_path)

        backend = LocalDeltaBackend()
        df = backend.read_table(bronze_path)

        # Metadata columns expanded
        assert "meta_device" in df.columns
        assert "meta_city" in df.columns
        assert "meta_response_time_sec" in df.columns

        # Ingestion metadata
        assert "_ingested_at" in df.columns
        assert "_source_file" in df.columns

        # Timestamp properly typed
        assert df["timestamp"].dtype == pl.Datetime

    def test_schema_validation_error(self, tmp_path):
        from pipeline.bronze.ingestion import SchemaValidationError, ingest_bronze

        # Create a parquet with wrong schema
        bad_df = pl.DataFrame({"wrong_col": [1, 2]})
        bad_path = str(tmp_path / "bad.parquet")
        bad_df.write_parquet(bad_path)

        bronze_path = str(tmp_path / "bad_bronze")
        with pytest.raises(SchemaValidationError):
            ingest_bronze(source_path=bad_path, bronze_path=bronze_path)


class TestSimulator:
    def test_split_into_batches(self, sample_parquet, tmp_path):
        from pipeline.bronze.simulator import split_parquet_into_batches

        output_dir = str(tmp_path / "batches")
        paths = split_parquet_into_batches(sample_parquet, output_dir, num_batches=2)

        assert len(paths) == 2
        for p in paths:
            assert Path(p).exists()

        # All rows accounted for
        total = sum(len(pl.read_parquet(p)) for p in paths)
        assert total == 4


class TestOrchestrator:
    def _setup_orchestrator(self, sample_parquet, tmp_path, monkeypatch):
        """Helper to set up orchestrator with temp paths."""
        from config.settings import Settings
        from monitoring.store import MonitoringStore

        s = Settings(
            bronze_source_path=sample_parquet,
            data_root=str(tmp_path / "data"),
        )
        monkeypatch.setattr("config.settings.get_settings", lambda: s)
        monkeypatch.setattr("pipeline.orchestrator.get_settings", lambda: s)
        temp_store = MonitoringStore(str(tmp_path / "mon.db"))
        monkeypatch.setattr("pipeline.orchestrator.get_monitoring_store", lambda: temp_store)
        return s, temp_store

    def test_run_bronze_step(self, sample_parquet, tmp_path, monkeypatch):
        from pipeline.orchestrator import PipelineOrchestrator

        _, temp_store = self._setup_orchestrator(sample_parquet, tmp_path, monkeypatch)
        orch = PipelineOrchestrator()
        orch.store = temp_store
        run = orch.run_pipeline(layers=["bronze"])

        assert run.status.value == "completed"
        assert len(run.steps) == 1
        assert run.steps[0].step_name == "bronze"
        assert run.steps[0].status.value == "completed"
        assert run.steps[0].rows_output > 0

    def test_run_records_to_monitoring(self, sample_parquet, tmp_path, monkeypatch):
        from pipeline.orchestrator import PipelineOrchestrator

        _, temp_store = self._setup_orchestrator(sample_parquet, tmp_path, monkeypatch)
        orch = PipelineOrchestrator()
        orch.store = temp_store
        run = orch.run_pipeline(layers=["bronze"])

        # Check monitoring store recorded the run
        runs = temp_store.get_pipeline_runs()
        assert len(runs) == 1
        assert runs[0].run_id == run.run_id
        assert runs[0].status.value == "completed"

    def test_failed_step_stops_pipeline(self, tmp_path, monkeypatch):
        from config.settings import Settings
        from monitoring.store import MonitoringStore
        from pipeline.orchestrator import PipelineOrchestrator

        s = Settings(
            bronze_source_path="/nonexistent/file.parquet",
            data_root=str(tmp_path / "data"),
        )
        monkeypatch.setattr("config.settings.get_settings", lambda: s)
        monkeypatch.setattr("pipeline.orchestrator.get_settings", lambda: s)
        temp_store = MonitoringStore(str(tmp_path / "mon.db"))
        monkeypatch.setattr("pipeline.orchestrator.get_monitoring_store", lambda: temp_store)

        orch = PipelineOrchestrator()
        orch.store = temp_store
        run = orch.run_pipeline(layers=["bronze"])

        assert run.status.value == "failed"
        assert run.steps[0].error_message is not None
