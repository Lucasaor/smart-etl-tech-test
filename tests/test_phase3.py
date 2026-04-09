"""Tests for Phase 3 — Silver Layer (cleaning, extraction, conversations)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_bronze_df(**kwargs) -> pl.DataFrame:
    """Minimal Bronze-like DataFrame for Silver tests."""
    defaults = {
        "message_id": ["msg001", "msg002", "msg003", "msg004", "msg005"],
        "conversation_id": ["conv_001", "conv_001", "conv_001", "conv_002", "conv_002"],
        "timestamp": [
            "2026-02-01 09:00:00",
            "2026-02-01 09:00:00",  # same timestamp as msg001 → duplicate
            "2026-02-01 09:01:30",
            "2026-02-01 10:00:00",
            "2026-02-01 10:05:00",
        ],
        "direction": ["outbound", "outbound", "inbound", "outbound", "inbound"],
        "sender_phone": [
            "+5511999990001",
            "+5511999990001",
            "+5511888880001",
            "+5511999990001",
            "+5511888880002",
        ],
        "sender_name": ["Agent A", "Agent A", "Ana Paula", "Agent A", "Joao Silva"],
        "message_type": ["text", "text", "text", "text", "audio"],
        "message_body": [
            "Olá, tudo bem?",
            "Olá, tudo bem?",  # same body as msg001 → duplicate
            "meu cpf eh 383.182.856-05, cep 08617-986 e email joao@example.com",
            "Bom dia!",
            "[audio transcrito] tenho um Onix 2022 placa SYL8V26",
        ],
        "status": ["sent", "delivered", "delivered", "read", "delivered"],
        "channel": ["whatsapp"] * 5,
        "campaign_id": ["camp_001"] * 5,
        "agent_id": ["agent_01"] * 5,
        "conversation_outcome": [
            "venda_fechada",
            "venda_fechada",
            "venda_fechada",
            "ghosting",
            "ghosting",
        ],
        "metadata": [
            '{"device": "desktop", "city": "Sao Paulo", "state": "SP", "response_time_sec": null, "is_business_hours": true, "lead_source": "google_ads"}',
            '{"device": "desktop", "city": "Sao Paulo", "state": "SP", "response_time_sec": null, "is_business_hours": true, "lead_source": "google_ads"}',
            '{"device": "android", "city": "Sao Paulo", "state": "SP", "response_time_sec": 90, "is_business_hours": true, "lead_source": "google_ads"}',
            '{"device": "desktop", "city": "Rio de Janeiro", "state": "RJ", "response_time_sec": null, "is_business_hours": false, "lead_source": "facebook"}',
            '{"device": "iphone", "city": "Rio de Janeiro", "state": "RJ", "response_time_sec": 300, "is_business_hours": false, "lead_source": "facebook"}',
        ],
    }
    defaults.update(kwargs)
    return pl.DataFrame(defaults)


def _ingest_to_bronze(df: pl.DataFrame, bronze_path: str) -> pl.DataFrame:
    """Run the Bronze ingestion pipeline on a DataFrame via temp parquet."""
    import tempfile

    from pipeline.bronze.ingestion import (
        add_ingestion_metadata,
        cast_types,
        parse_metadata,
    )

    df = parse_metadata(df)
    df = cast_types(df)
    df = add_ingestion_metadata(df, source_file="test")

    from core.storage import LocalDeltaBackend

    backend = LocalDeltaBackend()
    backend.write_table(df, bronze_path, mode="overwrite", schema_mode="overwrite")
    return df


# ─── Cleaning tests ───────────────────────────────────────────────────────────

class TestDeduplicateStatusEvents:
    def test_keeps_highest_status(self):
        """The row with the most advanced status is retained."""
        from pipeline.silver.cleaning import deduplicate_status_events

        df = pl.DataFrame(
            {
                "conversation_id": ["conv_001", "conv_001"],
                "sender_phone": ["+5511999990001", "+5511999990001"],
                "message_body": ["hello", "hello"],
                "direction": ["outbound", "outbound"],
                "timestamp": [
                    pl.Series(
                        ["2026-02-01 09:00:00", "2026-02-01 09:00:00"],
                        dtype=pl.Datetime,
                    )[0],
                    pl.Series(
                        ["2026-02-01 09:00:00", "2026-02-01 09:00:00"],
                        dtype=pl.Datetime,
                    )[1],
                ],
                "status": ["sent", "delivered"],
                "message_id": ["msg001", "msg002"],
                "extra": [1, 2],
            }
        ).with_columns(pl.col("timestamp").cast(pl.Datetime))

        result = deduplicate_status_events(df)
        assert len(result) == 1
        assert result["status"][0] == "delivered"

    def test_preserves_unique_messages(self):
        """Rows with distinct dedup keys are not removed."""
        from pipeline.silver.cleaning import deduplicate_status_events

        df = pl.DataFrame(
            {
                "conversation_id": ["conv_001", "conv_002"],
                "sender_phone": ["+55111", "+55222"],
                "message_body": ["hello", "world"],
                "direction": ["outbound", "inbound"],
                "timestamp": pl.Series(
                    ["2026-02-01 09:00:00", "2026-02-01 09:01:00"],
                    dtype=pl.Datetime,
                ),
                "status": ["delivered", "read"],
                "message_id": ["msg001", "msg002"],
            }
        )
        result = deduplicate_status_events(df)
        assert len(result) == 2

    def test_status_rank_order(self):
        """read > delivered > sent > failed in dedup priority."""
        from pipeline.silver.cleaning import deduplicate_status_events

        rows = []
        for status in ["failed", "sent", "delivered", "read"]:
            rows.append(
                {
                    "conversation_id": "conv_001",
                    "sender_phone": "+55111",
                    "message_body": "same body",
                    "direction": "outbound",
                    "status": status,
                    "message_id": f"msg_{status}",
                }
            )
        df = pl.DataFrame(rows).with_columns(
            pl.lit("2026-02-01 09:00:00").str.to_datetime().alias("timestamp")
        )
        result = deduplicate_status_events(df)
        assert len(result) == 1
        assert result["status"][0] == "read"


class TestAddContentFlags:
    def test_audio_flag_set(self):
        from pipeline.silver.cleaning import add_content_flags

        df = pl.DataFrame(
            {
                "message_body": [
                    "[audio transcrito] tenho um Gol 2019",
                    "mensagem normal",
                    "[audio transcrito] outro audio aqui",
                ]
            }
        )
        result = add_content_flags(df)
        assert result["is_audio_transcription"].to_list() == [True, False, True]

    def test_audio_prefix_stripped(self):
        from pipeline.silver.cleaning import add_content_flags

        df = pl.DataFrame(
            {"message_body": ["[audio transcrito] conteudo do audio"]}
        )
        result = add_content_flags(df)
        assert result["message_body_normalized"][0] == "conteudo do audio"

    def test_non_audio_body_unchanged(self):
        from pipeline.silver.cleaning import add_content_flags

        df = pl.DataFrame({"message_body": ["mensagem normal de texto"]})
        result = add_content_flags(df)
        assert result["message_body_normalized"][0] == "mensagem normal de texto"
        assert result["is_audio_transcription"][0] is False


class TestCleanSilverIntegration:
    def test_clean_silver_deduplicates(self, tmp_path):
        from pipeline.silver.cleaning import clean_silver
        from core.storage import LocalDeltaBackend

        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver_messages")

        df = _make_bronze_df()
        _ingest_to_bronze(df, bronze_path)

        stats = clean_silver(bronze_path, silver_path)

        # msg001 and msg002 are logical duplicates: same body, direction, timestamp, phone
        # Only one should survive (the "delivered" one)
        assert stats["duplicates_removed"] == 1
        assert stats["rows_written"] == 4  # 5 - 1 duplicate

    def test_clean_silver_has_required_columns(self, tmp_path):
        from pipeline.silver.cleaning import clean_silver
        from core.storage import LocalDeltaBackend

        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver_messages")

        _ingest_to_bronze(_make_bronze_df(), bronze_path)
        clean_silver(bronze_path, silver_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(silver_path)

        assert "is_audio_transcription" in result.columns
        assert "message_body_normalized" in result.columns
        assert "_silver_version" in result.columns

    def test_clean_silver_audio_count(self, tmp_path):
        from pipeline.silver.cleaning import clean_silver

        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver_messages")

        _ingest_to_bronze(_make_bronze_df(), bronze_path)
        stats = clean_silver(bronze_path, silver_path)

        # msg005 is the only audio transcription in the fixture
        assert stats["audio_transcriptions"] == 1


# ─── Extraction tests ─────────────────────────────────────────────────────────

class TestExtractFromBody:
    def test_cpf_extraction(self):
        from pipeline.silver.extraction import _extract_from_body

        result = _extract_from_body("meu cpf eh 383.182.856-05")
        assert result["pii_cpf_found"] is True
        assert result["extracted_cpf_hash"] is not None
        assert len(result["extracted_cpf_hash"]) == 16

    def test_email_extraction(self):
        from pipeline.silver.extraction import _extract_from_body

        result = _extract_from_body("meu email eh joao.silva@example.com.br")
        assert result["pii_email_found"] is True
        assert result["extracted_email_hash"] is not None

    def test_cep_extraction(self):
        from pipeline.silver.extraction import _extract_from_body

        result = _extract_from_body("cep 08617-986")
        assert result["pii_cep_found"] is True
        assert result["extracted_cep"] == "08617986"

    def test_plate_extraction(self):
        from pipeline.silver.extraction import _extract_from_body

        result = _extract_from_body("placa do carro eh SYL8V26")
        assert result["extracted_plate"] == "SYL8V26"

    def test_competitor_extraction(self):
        from pipeline.silver.extraction import _extract_from_body

        result = _extract_from_body("estou cotando tambem na HDI Seguros")
        assert result["extracted_competitor"] is not None
        assert "hdi" in result["extracted_competitor"].lower()

    def test_multiple_pii_in_same_message(self):
        from pipeline.silver.extraction import _extract_from_body

        body = "cpf 383.182.856-05, cep 08617-986, email x@x.com"
        result = _extract_from_body(body)
        assert result["pii_cpf_found"] is True
        assert result["pii_cep_found"] is True
        assert result["pii_email_found"] is True

    def test_empty_body(self):
        from pipeline.silver.extraction import _extract_from_body

        result = _extract_from_body(None)
        assert result["pii_cpf_found"] is False
        assert result["extracted_plate"] is None

    def test_clean_message_no_pii(self):
        from pipeline.silver.extraction import _extract_from_body

        result = _extract_from_body("Olá, tudo bem? Gostaria de fazer uma cotação!")
        assert result["pii_cpf_found"] is False
        assert result["pii_email_found"] is False
        assert result["extracted_plate"] is None


class TestMaskPiiInBody:
    def test_cpf_masked(self):
        from pipeline.silver.extraction import mask_pii_in_body

        result = mask_pii_in_body("cpf 383.182.856-05")
        assert "383.182.856-05" not in result
        assert "[CPF_MASKED]" in result

    def test_email_masked(self):
        from pipeline.silver.extraction import mask_pii_in_body

        result = mask_pii_in_body("email joao@example.com")
        assert "joao@example.com" not in result
        assert "[EMAIL_MASKED]" in result

    def test_none_body_returns_none(self):
        from pipeline.silver.extraction import mask_pii_in_body

        assert mask_pii_in_body(None) is None

    def test_non_pii_message_unchanged(self):
        from pipeline.silver.extraction import mask_pii_in_body

        msg = "Olá tudo bem?"
        assert mask_pii_in_body(msg) == msg


class TestExtractEntitiesIntegration:
    def test_extract_entities_adds_columns(self, tmp_path):
        from pipeline.silver.cleaning import clean_silver
        from pipeline.silver.extraction import extract_entities
        from core.storage import LocalDeltaBackend

        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver_messages")

        _ingest_to_bronze(_make_bronze_df(), bronze_path)
        clean_silver(bronze_path, silver_path)
        stats = extract_entities(silver_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(silver_path)

        assert "pii_cpf_found" in result.columns
        assert "pii_email_found" in result.columns
        assert "extracted_plate" in result.columns
        assert "extracted_competitor" in result.columns
        assert "message_body_masked" in result.columns

    def test_extract_entities_counts_pii(self, tmp_path):
        from pipeline.silver.cleaning import clean_silver
        from pipeline.silver.extraction import extract_entities

        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver_messages")

        _ingest_to_bronze(_make_bronze_df(), bronze_path)
        clean_silver(bronze_path, silver_path)
        stats = extract_entities(silver_path)

        # msg003 has CPF + CEP + email; msg005 has plate
        assert stats["messages_with_cpf"] >= 1
        assert stats["messages_with_plate"] >= 1

    def test_pii_not_in_masked_body(self, tmp_path):
        from pipeline.silver.cleaning import clean_silver
        from pipeline.silver.extraction import extract_entities
        from core.storage import LocalDeltaBackend

        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver_messages")

        _ingest_to_bronze(_make_bronze_df(), bronze_path)
        clean_silver(bronze_path, silver_path)
        extract_entities(silver_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(silver_path)

        # The CPF "383.182.856-05" should not appear in any masked body
        masked_bodies = result["message_body_masked"].drop_nulls().to_list()
        for body in masked_bodies:
            assert "383.182.856-05" not in body


# ─── Conversations tests ─────────────────────────────────────────────────────

class TestAggregateConversations:
    def _build_silver_messages(self, tmp_path) -> str:
        """Build a full Silver messages table for conversation tests."""
        from pipeline.silver.cleaning import clean_silver
        from pipeline.silver.extraction import extract_entities

        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver_messages")

        _ingest_to_bronze(_make_bronze_df(), bronze_path)
        clean_silver(bronze_path, silver_path)
        extract_entities(silver_path)
        return silver_path

    def test_conversation_count(self, tmp_path):
        from pipeline.silver.conversations import aggregate_conversations
        from core.storage import LocalDeltaBackend

        silver_path = self._build_silver_messages(tmp_path)
        conv_path = str(tmp_path / "conversations")

        stats = aggregate_conversations(silver_path, conv_path)
        assert stats["total_conversations"] == 2  # conv_001 and conv_002

    def test_conversation_has_required_columns(self, tmp_path):
        from pipeline.silver.conversations import aggregate_conversations
        from core.storage import LocalDeltaBackend

        silver_path = self._build_silver_messages(tmp_path)
        conv_path = str(tmp_path / "conversations")

        aggregate_conversations(silver_path, conv_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(conv_path)

        for col in [
            "conversation_id",
            "first_message_at",
            "last_message_at",
            "duration_minutes",
            "total_messages",
            "inbound_messages",
            "outbound_messages",
            "conversation_outcome",
            "has_cpf",
            "lead_plate",
            "competitor_mentioned",
            "lead_phone",
            "lead_name",
        ]:
            assert col in result.columns, f"Missing column: {col}"

    def test_conversation_with_sale(self, tmp_path):
        from pipeline.silver.conversations import aggregate_conversations

        silver_path = self._build_silver_messages(tmp_path)
        conv_path = str(tmp_path / "conversations")

        stats = aggregate_conversations(silver_path, conv_path)
        # conv_001 has outcome "venda_fechada"
        assert stats["conversations_with_sale"] == 1

    def test_duration_minutes_non_negative(self, tmp_path):
        from pipeline.silver.conversations import aggregate_conversations
        from core.storage import LocalDeltaBackend

        silver_path = self._build_silver_messages(tmp_path)
        conv_path = str(tmp_path / "conversations")

        aggregate_conversations(silver_path, conv_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(conv_path)

        durations = result["duration_minutes"].drop_nulls().to_list()
        assert all(d >= 0 for d in durations)

    def test_plate_found_in_conversation(self, tmp_path):
        from pipeline.silver.conversations import aggregate_conversations
        from core.storage import LocalDeltaBackend

        silver_path = self._build_silver_messages(tmp_path)
        conv_path = str(tmp_path / "conversations")

        stats = aggregate_conversations(silver_path, conv_path)
        # conv_002 has msg005 with plate SYL8V26
        assert stats["conversations_with_plate"] == 1


# ─── Spec Tools tests ────────────────────────────────────────────────────────

class TestSpecTools:
    """Tests for agents/tools/spec_tools.py."""

    def test_obter_preview_dados(self, tmp_path):
        from agents.tools.spec_tools import obter_preview_dados

        dados_path = str(tmp_path / "dados.parquet")
        pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "nome": ["a", "b", "c", "d", "e"],
        }).write_parquet(dados_path)

        preview = obter_preview_dados(dados_path, num_linhas=3)
        assert len(preview) == 3
        assert preview[0]["id"] == 1

    def test_obter_estatisticas_colunas(self, tmp_path):
        from agents.tools.spec_tools import obter_estatisticas_colunas

        dados_path = str(tmp_path / "dados.parquet")
        pl.DataFrame({
            "valor": [10, 20, 30],
            "nome": ["a", "b", "c"],
        }).write_parquet(dados_path)

        stats = obter_estatisticas_colunas(dados_path)
        assert "valor" in stats
        assert "nome" in stats
        assert stats["valor"]["tipo"] == "Int64"
        assert stats["valor"]["unicos"] == 3

    def test_validar_spec_completa(self, tmp_path):
        from agents.tools.spec_tools import validar_spec_completa
        from pipeline.specs import ProjectSpec

        dados_path = str(tmp_path / "d.parquet")
        pl.DataFrame({"x": [1]}).write_parquet(dados_path)

        spec = ProjectSpec(
            dados_brutos_path=dados_path,
            dicionario_dados="dicionário",
            descricao_kpis="kpis",
        )
        resultado = validar_spec_completa(spec)
        assert resultado["valida"] is True
        assert resultado["tem_dados"] is True
        assert resultado["tem_dicionario"] is True
        assert resultado["tem_kpis"] is True

    def test_validar_spec_incompleta(self):
        from agents.tools.spec_tools import validar_spec_completa
        from pipeline.specs import ProjectSpec

        spec = ProjectSpec(
            dados_brutos_path="",
            dicionario_dados="",
            descricao_kpis="",
        )
        resultado = validar_spec_completa(spec)
        assert resultado["valida"] is False
        assert len(resultado["problemas"]) >= 3
