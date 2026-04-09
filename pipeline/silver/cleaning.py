"""Silver Layer - Message Cleaning

Deduplicates status events, normalizes audio content, and adds
quality flags to produce the canonical Silver messages table.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)

# Status priority: higher rank = more progressed in delivery lifecycle
STATUS_RANK: dict[str, int] = {
    "failed": 0,
    "sent": 1,
    "delivered": 2,
    "read": 3,
}

# Deduplication key: same logical message = same conversation + sender + body + direction + timestamp
DEDUP_COLS = [
    "conversation_id",
    "sender_phone",
    "message_body",
    "direction",
    "timestamp",
]


def deduplicate_status_events(df: pl.DataFrame) -> pl.DataFrame:
    """Remove duplicate rows that represent the same logical message at different delivery statuses.

    The source data contains the same message multiple times (sent, delivered, read).
    We keep only the row with the most advanced delivery status per logical message.
    """
    status_map = pl.DataFrame(
        {
            "status": list(STATUS_RANK.keys()),
            "status_rank": list(STATUS_RANK.values()),
        }
    )
    df = df.join(status_map, on="status", how="left").with_columns(
        pl.col("status_rank").fill_null(0)
    )
    df = (
        df.sort("status_rank", descending=True)
        .unique(subset=DEDUP_COLS, keep="first")
        .drop("status_rank")
    )
    return df


def add_content_flags(df: pl.DataFrame) -> pl.DataFrame:
    """Add is_audio_transcription flag and normalized body (prefix stripped)."""
    df = df.with_columns(
        pl.col("message_body")
        .str.starts_with("[audio transcrito]")
        .alias("is_audio_transcription")
    )
    df = df.with_columns(
        pl.when(pl.col("is_audio_transcription"))
        .then(
            pl.col("message_body")
            .str.strip_prefix("[audio transcrito]")
            .str.strip_chars()
        )
        .otherwise(pl.col("message_body"))
        .alias("message_body_normalized")
    )
    return df


def clean_silver(
    bronze_path: str | Path,
    silver_messages_path: str | Path,
) -> dict[str, Any]:
    """Read Bronze table, clean and deduplicate, write Silver messages table.

    Args:
        bronze_path: Path to the Bronze Delta table.
        silver_messages_path: Destination path for the Silver messages Delta table.

    Returns:
        Stats dict with row counts and quality metrics.
    """
    bronze_path = str(bronze_path)
    silver_messages_path = str(silver_messages_path)

    storage = get_storage_backend()

    logger.info("reading_bronze_table", extra={"path": bronze_path})
    df = storage.read_table(bronze_path)
    rows_before = len(df)

    logger.info("deduplicating_status_events", extra={"rows_before": rows_before})
    df = deduplicate_status_events(df)
    rows_after_dedup = len(df)

    df = add_content_flags(df)
    df = df.with_columns(pl.lit("silver_v1").alias("_silver_version"))

    logger.info(
        "writing_silver_messages",
        extra={"path": silver_messages_path, "rows": rows_after_dedup},
    )
    storage.write_table(df, silver_messages_path, mode="overwrite", schema_mode="overwrite")

    stats = {
        "rows_before": rows_before,
        "rows_after_dedup": rows_after_dedup,
        "duplicates_removed": rows_before - rows_after_dedup,
        "audio_transcriptions": df.filter(pl.col("is_audio_transcription")).height,
        "rows_written": rows_after_dedup,
    }
    logger.info("silver_cleaning_complete", extra=stats)
    return stats
