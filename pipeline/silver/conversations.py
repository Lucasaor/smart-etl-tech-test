"""Silver Layer - Conversation Aggregation

Aggregates the per-message Silver table into a per-conversation summary
table (~15 k rows) with timing, counts, outcome, lead identity, and
extracted entities.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)


def aggregate_conversations(
    silver_messages_path: str | Path,
    silver_conversations_path: str | Path,
) -> dict[str, Any]:
    """Aggregate per-message Silver rows into one row per conversation.

    Args:
        silver_messages_path: Path to the Silver messages Delta table.
        silver_conversations_path: Destination path for the Silver conversations Delta table.

    Returns:
        Stats dict with conversation counts and highlights.
    """
    silver_messages_path = str(silver_messages_path)
    silver_conversations_path = str(silver_conversations_path)
    storage = get_storage_backend()

    logger.info(
        "reading_silver_messages_for_aggregation",
        extra={"path": silver_messages_path},
    )
    df = storage.read_table(silver_messages_path)

    # ── Per-conversation aggregations ────────────────────────────────────────
    conversations = df.group_by("conversation_id").agg(
        [
            # Timing
            pl.col("timestamp").min().alias("first_message_at"),
            pl.col("timestamp").max().alias("last_message_at"),
            # Message counts
            pl.len().alias("total_messages"),
            (pl.col("direction") == "inbound").sum().alias("inbound_messages"),
            (pl.col("direction") == "outbound").sum().alias("outbound_messages"),
            (pl.col("message_type") == "audio").sum().alias("audio_messages"),
            (pl.col("message_type") == "image").sum().alias("image_messages"),
            pl.col("is_audio_transcription").sum().alias("audio_transcriptions"),
            # PII / entity flags
            pl.col("pii_cpf_found").any().alias("has_cpf"),
            pl.col("pii_email_found").any().alias("has_email"),
            pl.col("pii_cep_found").any().alias("has_cep"),
            pl.col("pii_phone_found").any().alias("has_phone"),
            # First non-null extracted entities
            pl.col("extracted_plate").drop_nulls().first().alias("lead_plate"),
            pl.col("extracted_cep").drop_nulls().first().alias("lead_cep"),
            pl.col("extracted_competitor").drop_nulls().first().alias("competitor_mentioned"),
            # Conversation metadata (consistent across every message in a conversation)
            pl.col("conversation_outcome").first().alias("conversation_outcome"),
            pl.col("agent_id").first().alias("agent_id"),
            pl.col("campaign_id").first().alias("campaign_id"),
            pl.col("channel").first().alias("channel"),
            pl.col("meta_lead_source").first().alias("lead_source"),
            pl.col("meta_city").first().alias("lead_city"),
            pl.col("meta_state").first().alias("lead_state"),
            pl.col("meta_device").first().alias("lead_device"),
            pl.col("meta_response_time_sec").mean().alias("avg_response_time_sec"),
        ]
    )

    # ── Lead identity from the first inbound message ──────────────────────────
    lead_info = (
        df.filter(pl.col("direction") == "inbound")
        .sort("timestamp")
        .group_by("conversation_id")
        .agg(
            [
                pl.col("sender_phone").first().alias("lead_phone"),
                pl.col("sender_name").first().alias("lead_name"),
            ]
        )
    )
    conversations = conversations.join(lead_info, on="conversation_id", how="left")

    # ── Derived columns ───────────────────────────────────────────────────────
    conversations = conversations.with_columns(
        [
            (
                (pl.col("last_message_at") - pl.col("first_message_at"))
                .dt.total_seconds()
                .cast(pl.Float64)
                / 60.0
            ).alias("duration_minutes"),
            pl.col("first_message_at").dt.date().alias("conversation_date"),
        ]
    )

    storage.write_table(
        conversations,
        silver_conversations_path,
        mode="overwrite",
        schema_mode="overwrite",
    )

    stats = {
        "total_conversations": len(conversations),
        "conversations_with_sale": conversations.filter(
            pl.col("conversation_outcome") == "venda_fechada"
        ).height,
        "conversations_with_competitor": conversations.filter(
            pl.col("competitor_mentioned").is_not_null()
        ).height,
        "conversations_with_plate": conversations.filter(
            pl.col("lead_plate").is_not_null()
        ).height,
        "rows_written": len(conversations),
    }
    logger.info("conversation_aggregation_complete", extra=stats)
    return stats
