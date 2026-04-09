"""Simulate incremental data arrival by splitting a parquet into batches.

Useful for testing the incremental ingestion and the monitor agent's
ability to detect new data. Splits by date ranges within the timestamp column.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import structlog

logger = structlog.get_logger(__name__)


def split_parquet_into_batches(
    source_path: str,
    output_dir: str,
    num_batches: int = 4,
) -> list[str]:
    """Split a parquet file into N roughly equal batches by timestamp.

    Args:
        source_path: Path to the full parquet file.
        output_dir: Directory to write batch parquet files.
        num_batches: Number of batches to create.

    Returns:
        List of paths to the batch files, in chronological order.
    """
    df = pl.read_parquet(source_path)

    # Parse timestamp for sorting/splitting
    df = df.with_columns(
        pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("_ts_parsed")
    )
    df = df.sort("_ts_parsed")

    total_rows = len(df)
    batch_size = total_rows // num_batches
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    batch_paths: list[str] = []

    for i in range(num_batches):
        start = i * batch_size
        end = total_rows if i == num_batches - 1 else (i + 1) * batch_size

        batch = df.slice(start, end - start).drop("_ts_parsed")

        ts_min = df["_ts_parsed"][start]
        ts_max = df["_ts_parsed"][end - 1]

        path = str(output_path / f"batch_{i:02d}.parquet")
        batch.write_parquet(path)
        batch_paths.append(path)

        logger.info(
            "batch_created",
            batch=i,
            rows=len(batch),
            ts_range=f"{ts_min} → {ts_max}",
            path=path,
        )

    logger.info("split_complete", num_batches=num_batches, total_rows=total_rows)
    return batch_paths
