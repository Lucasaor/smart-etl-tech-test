"""Compute backend abstraction — Polars (local) or PySpark (Databricks).

Pipeline transformations are written as pure functions operating on
Polars DataFrames. On Databricks, we convert at the boundary (read/write)
using the StorageBackend while keeping Polars as the compute engine for
simplicity. For heavy workloads on Databricks, individual steps can
opt-in to native Spark via the SparkCompute helper.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger(__name__)


class ComputeBackend(ABC):
    """Minimal abstraction for compute operations.

    Most pipeline logic runs directly on Polars DataFrames.
    This ABC exists primarily for operations that benefit from
    distributed compute on Databricks (large joins, heavy aggregations).
    """

    @abstractmethod
    def sql(self, query: str, tables: dict[str, pl.DataFrame]) -> pl.DataFrame:
        """Run a SQL query over named DataFrames."""

    @abstractmethod
    def read_parquet(self, path: str) -> pl.DataFrame:
        """Read a Parquet file and return as Polars DataFrame."""


class PolarsCompute(ComputeBackend):
    """Local compute using Polars + DuckDB for SQL."""

    def sql(self, query: str, tables: dict[str, pl.DataFrame]) -> pl.DataFrame:
        import duckdb

        conn = duckdb.connect()
        for name, df in tables.items():
            conn.register(name, df.to_arrow())
        result = conn.execute(query).arrow()
        conn.close()
        return pl.from_arrow(result)

    def read_parquet(self, path: str) -> pl.DataFrame:
        logger.info("reading_parquet", path=path, engine="polars")
        return pl.read_parquet(path)


class SparkCompute(ComputeBackend):
    """Databricks compute using PySpark.

    Results are always returned as Polars DataFrames for consistency
    with the rest of the pipeline code.
    """

    def _get_spark(self) -> Any:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession.")
        return spark

    def sql(self, query: str, tables: dict[str, pl.DataFrame]) -> pl.DataFrame:
        spark = self._get_spark()
        for name, df in tables.items():
            spark_df = spark.createDataFrame(df.to_pandas())
            spark_df.createOrReplaceTempView(name)
        result = spark.sql(query)
        return pl.from_pandas(result.toPandas())

    def read_parquet(self, path: str) -> pl.DataFrame:
        spark = self._get_spark()
        logger.info("reading_parquet", path=path, engine="spark")
        spark_df = spark.read.parquet(path)
        return pl.from_pandas(spark_df.toPandas())


@lru_cache(maxsize=1)
def get_compute_backend() -> ComputeBackend:
    """Factory: returns the appropriate compute backend based on RUNTIME_ENV."""
    from config.settings import get_settings

    settings = get_settings()
    if settings.is_databricks:
        logger.info("compute_backend", backend="spark")
        return SparkCompute()
    logger.info("compute_backend", backend="polars")
    return PolarsCompute()
