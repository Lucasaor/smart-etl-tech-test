"""Bronze layer ingestion — Parquet source → Delta Lake bronze table.

Reads the raw conversations parquet, validates the schema, parses the
metadata JSON column into typed sub-columns, casts types properly,
adds ingestion metadata, and writes to the Bronze Delta table.

Supports full load (first run) and incremental append (new data).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import polars as pl
import structlog

from core.compute import get_compute_backend
from core.storage import get_storage_backend

logger = structlog.get_logger(__name__)

# Expected columns in the source parquet (per data dictionary)
EXPECTED_COLUMNS = {
    "message_id",
    "conversation_id",
    "timestamp",
    "direction",
    "sender_phone",
    "sender_name",
    "message_type",
    "message_body",
    "status",
    "channel",
    "campaign_id",
    "agent_id",
    "conversation_outcome",
    "metadata",
}


def validate_schema(df: pl.DataFrame) -> list[str]:
    """Validate the source DataFrame schema. Returns list of issues (empty = OK)."""
    issues: list[str] = []
    actual = set(df.columns)

    missing = EXPECTED_COLUMNS - actual
    if missing:
        issues.append(f"Missing columns: {sorted(missing)}")

    extra = actual - EXPECTED_COLUMNS
    if extra:
        logger.info("extra_columns_detected", columns=sorted(extra))

    if len(df) == 0:
        issues.append("DataFrame is empty")

    return issues


def parse_metadata(df: pl.DataFrame) -> pl.DataFrame:
    """Parse the JSON metadata column into typed sub-columns.

    Extracts: device, city, state, response_time_sec, is_business_hours, lead_source.
    Keeps the original metadata column for reference.
    """
    # Parse JSON strings into struct column
    parsed = df.with_columns(
        pl.col("metadata").str.json_decode(
            pl.Struct({
                "device": pl.String,
                "city": pl.String,
                "state": pl.String,
                "response_time_sec": pl.Int64,
                "is_business_hours": pl.Boolean,
                "lead_source": pl.String,
            })
        ).alias("_meta_parsed")
    )

    # Unnest the struct into individual columns
    result = parsed.with_columns(
        pl.col("_meta_parsed").struct.field("device").alias("meta_device"),
        pl.col("_meta_parsed").struct.field("city").alias("meta_city"),
        pl.col("_meta_parsed").struct.field("state").alias("meta_state"),
        pl.col("_meta_parsed").struct.field("response_time_sec").alias("meta_response_time_sec"),
        pl.col("_meta_parsed").struct.field("is_business_hours").alias("meta_is_business_hours"),
        pl.col("_meta_parsed").struct.field("lead_source").alias("meta_lead_source"),
    ).drop("_meta_parsed")

    return result


def cast_types(df: pl.DataFrame) -> pl.DataFrame:
    """Cast string columns to proper types."""
    return df.with_columns(
        pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("timestamp"),
    )


def add_ingestion_metadata(df: pl.DataFrame, source_file: str) -> pl.DataFrame:
    """Add ingestion tracking columns."""
    now = datetime.now(timezone.utc)
    return df.with_columns(
        pl.lit(now).alias("_ingested_at"),
        pl.lit(source_file).alias("_source_file"),
    )


def ingest_bronze(
    source_path: str,
    bronze_path: str,
    mode: str = "auto",
) -> dict[str, int | str]:
    """Run the Bronze ingestion pipeline.

    Args:
        source_path: Path to the source parquet file.
        bronze_path: Path where the Bronze Delta table will be written.
        mode: "full" forces overwrite, "incremental" appends only new rows,
              "auto" detects based on whether the Bronze table exists.

    Returns:
        Dictionary with ingestion stats (rows_read, rows_written, mode_used, etc.)
    """
    storage = get_storage_backend()
    compute = get_compute_backend()

    # 1. Read source parquet
    logger.info("bronze_ingestion_start", source=source_path)
    df_raw = compute.read_parquet(source_path)
    rows_read = len(df_raw)
    logger.info("source_read", rows=rows_read, columns=len(df_raw.columns))

    # 2. Validate schema
    issues = validate_schema(df_raw)
    if issues:
        raise SchemaValidationError(f"Schema validation failed: {issues}")
    logger.info("schema_validated")

    # 3. Determine ingestion mode
    table_exists = storage.table_exists(bronze_path)
    if mode == "auto":
        mode_used = "incremental" if table_exists else "full"
    else:
        mode_used = mode

    # 4. For incremental: filter to only new rows
    rows_written = rows_read
    if mode_used == "incremental" and table_exists:
        df_raw = cast_types(df_raw)  # Need datetime for comparison
        existing = storage.read_table(bronze_path)
        max_ts = existing["timestamp"].max()
        existing_ids = set(existing["message_id"].to_list())

        # Filter: timestamp > max existing OR message_id not in existing
        df_new = df_raw.filter(
            (pl.col("timestamp") > max_ts) | (~pl.col("message_id").is_in(existing_ids))
        )
        rows_written = len(df_new)

        if rows_written == 0:
            logger.info("bronze_no_new_data")
            return {
                "rows_read": rows_read,
                "rows_written": 0,
                "mode": "incremental",
                "status": "no_new_data",
            }

        df_raw = df_new
        logger.info("incremental_filtered", new_rows=rows_written)
    else:
        # Full mode: cast types now
        df_raw = cast_types(df_raw)

    # 5. Parse metadata JSON → typed sub-columns
    df_parsed = parse_metadata(df_raw)
    logger.info("metadata_parsed")

    # 6. Add ingestion metadata
    df_final = add_ingestion_metadata(df_parsed, source_file=source_path)

    # 7. Write to Delta
    write_mode = "append" if mode_used == "incremental" else "overwrite"
    storage.write_table(df_final, bronze_path, mode=write_mode, schema_mode="merge")

    version = storage.get_table_version(bronze_path)
    total_rows = storage.get_table_row_count(bronze_path)

    logger.info(
        "bronze_ingestion_complete",
        rows_written=rows_written,
        total_rows=total_rows,
        mode=mode_used,
        delta_version=version,
    )

    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "total_rows": total_rows,
        "mode": mode_used,
        "delta_version": version,
        "status": "success",
    }


class SchemaValidationError(Exception):
    """Raised when source data doesn't match expected schema."""
