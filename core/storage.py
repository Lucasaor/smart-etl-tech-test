"""Storage backend abstraction — Delta Lake on local filesystem or Databricks.

Provides a unified interface for reading/writing Delta tables regardless
of whether the pipeline runs locally (deltalake + polars) or on Databricks
(PySpark + Delta). Switch via RUNTIME_ENV env var.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import polars as pl
import structlog

logger = structlog.get_logger(__name__)


class StorageBackend(ABC):
    """Abstract base class for Delta Lake storage operations."""

    @abstractmethod
    def read_table(self, path: str, version: int | None = None) -> pl.DataFrame:
        """Read a Delta table and return as Polars DataFrame."""

    @abstractmethod
    def write_table(
        self,
        df: pl.DataFrame,
        path: str,
        mode: Literal["overwrite", "append"] = "overwrite",
        schema_mode: str | None = None,
    ) -> None:
        """Write a Polars DataFrame to a Delta table."""

    @abstractmethod
    def table_exists(self, path: str) -> bool:
        """Check whether a Delta table exists at the given path."""

    @abstractmethod
    def get_table_version(self, path: str) -> int:
        """Return the current version number of the Delta table."""

    @abstractmethod
    def get_table_row_count(self, path: str) -> int:
        """Return the number of rows in the Delta table."""


class LocalDeltaBackend(StorageBackend):
    """Delta Lake on local filesystem using deltalake + polars."""

    def read_table(self, path: str, version: int | None = None) -> pl.DataFrame:
        from deltalake import DeltaTable

        logger.info("reading_delta_table", path=path, version=version)
        dt = DeltaTable(path, version=version)
        return pl.from_arrow(dt.to_pyarrow_table())

    def write_table(
        self,
        df: pl.DataFrame,
        path: str,
        mode: Literal["overwrite", "append"] = "overwrite",
        schema_mode: str | None = None,
    ) -> None:
        from deltalake import DeltaTable, write_deltalake

        Path(path).parent.mkdir(parents=True, exist_ok=True)

        arrow_table = df.to_arrow()
        logger.info("writing_delta_table", path=path, mode=mode, rows=len(df))

        write_kwargs: dict[str, Any] = {
            "table_or_uri": path,
            "data": arrow_table,
            "mode": mode,
        }
        if schema_mode:
            write_kwargs["schema_mode"] = schema_mode
        elif mode == "overwrite":
            write_kwargs["schema_mode"] = "overwrite"

        write_deltalake(**write_kwargs)

    def table_exists(self, path: str) -> bool:
        from deltalake import DeltaTable

        try:
            DeltaTable(path)
            return True
        except Exception:
            return False

    def get_table_version(self, path: str) -> int:
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        return dt.version()

    def get_table_row_count(self, path: str) -> int:
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        return len(dt.to_pyarrow_table())


class DatabricksBackend(StorageBackend):
    """Delta Lake on Databricks using PySpark.

    Requires a SparkSession available (e.g. inside a Databricks notebook).
    Converts Spark DataFrames ↔ Polars for a consistent interface.

    Compatible with Databricks Free Edition (serverless compute / Spark Connect).
    Uses SQL-based alternatives instead of delta.tables.DeltaTable API.
    """

    def _get_spark(self) -> Any:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "No active SparkSession. DatabricksBackend must run inside Databricks."
            )
        return spark

    def read_table(self, path: str, version: int | None = None) -> pl.DataFrame:
        spark = self._get_spark()
        reader = spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        spark_df = reader.load(path)
        # Convert via Arrow for efficiency
        return pl.from_arrow(spark_df.toPandas())  # Spark → Pandas → Polars

    def write_table(
        self,
        df: pl.DataFrame,
        path: str,
        mode: Literal["overwrite", "append"] = "overwrite",
        schema_mode: str | None = None,
    ) -> None:
        spark = self._get_spark()
        spark_df = spark.createDataFrame(df.to_pandas())
        writer = spark_df.write.format("delta").mode(mode)
        if schema_mode == "merge":
            writer = writer.option("mergeSchema", "true")
        elif mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        writer.save(path)

    def table_exists(self, path: str) -> bool:
        spark = self._get_spark()
        try:
            spark.read.format("delta").load(path).limit(0).collect()
            return True
        except Exception:
            return False

    def get_table_version(self, path: str) -> int:
        spark = self._get_spark()
        hist = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1")
        return int(hist.select("version").collect()[0][0])

    def get_table_row_count(self, path: str) -> int:
        spark = self._get_spark()
        return spark.read.format("delta").load(path).count()


@lru_cache(maxsize=1)
def get_storage_backend() -> StorageBackend:
    """Factory: returns the appropriate backend based on RUNTIME_ENV."""
    from config.settings import get_settings

    settings = get_settings()
    if settings.is_databricks:
        logger.info("storage_backend", backend="databricks")
        return DatabricksBackend()
    logger.info("storage_backend", backend="local_delta")
    return LocalDeltaBackend()
