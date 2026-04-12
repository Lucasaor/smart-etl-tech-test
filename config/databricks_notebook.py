"""Shared helpers for Databricks notebook bootstrap.

Placed under `config` to avoid module-name conflicts with the external
`databricks` SDK package.

Compatible with Databricks Free Edition (serverless compute, Unity Catalog
Volumes). All paths use UC Volumes instead of DBFS.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Default UC Volume path for pipeline data.
# Format: /Volumes/<catalog>/<schema>/<volume>/<path>
DEFAULT_VOLUME_ROOT = "/Volumes/main/default/pipeline_data"


def detect_repo_path(explicit_repo_path: str | None = None) -> str:
    """Return the repository path inside Databricks Workspace when possible."""
    candidates = [
        explicit_repo_path,
        os.getenv("PROJECT_REPO_PATH"),
        "/Workspace/Repos/agentic-pipeline",
        "/Workspace/Repos/Lucasaor/smart-etl-tech-test",
    ]

    for candidate in candidates:
        if not candidate:
            continue
        marker = Path(candidate) / "config" / "settings.py"
        if marker.exists():
            return str(Path(candidate))

    return "/Workspace/Repos/agentic-pipeline"


def bootstrap_notebook(
    explicit_repo_path: str | None = None,
    data_root: str | None = None,
) -> dict[str, str]:
    """Configure sys.path and environment variables for Databricks execution.

    On Databricks Free Edition (serverless), the default data_root points to a
    Unity Catalog Volume: /Volumes/main/default/pipeline_data.
    """
    if data_root is None:
        data_root = DEFAULT_VOLUME_ROOT

    repo_path = detect_repo_path(explicit_repo_path)

    if repo_path not in sys.path and Path(repo_path).exists():
        sys.path.insert(0, repo_path)

    os.environ["RUNTIME_ENV"] = "databricks"
    os.environ["DATA_ROOT"] = data_root

    return {
        "repo_path": repo_path,
        "data_root": data_root,
    }


def default_paths(data_root: str | None = None) -> dict[str, str]:
    """Return canonical layer paths for Databricks notebooks.

    All paths point to Unity Catalog Volumes (POSIX-style paths).
    """
    if data_root is None:
        data_root = DEFAULT_VOLUME_ROOT

    return {
        "specs": f"{data_root}/specs",
        "bronze": f"{data_root}/bronze",
        "silver": f"{data_root}/silver",
        "silver_messages": f"{data_root}/silver/messages",
        "silver_conversations": f"{data_root}/silver/conversations",
        "gold": f"{data_root}/gold",
        "gold_sentiment": f"{data_root}/gold/sentiment",
        "gold_personas": f"{data_root}/gold/personas",
        "gold_segmentation": f"{data_root}/gold/segmentation",
        "gold_analytics": f"{data_root}/gold/analytics",
        "gold_vendor_analysis": f"{data_root}/gold/vendor_analysis",
        "monitoring": f"{data_root}/monitoring",
    }


def notebook_workspace_path(repo_path: str, notebook_filename: str) -> str:
    """Build a workspace notebook path accepted by dbutils.notebook.run."""
    stem = notebook_filename.removesuffix(".py")
    return f"{repo_path}/databricks/notebooks/{stem}"


# ---------------------------------------------------------------------------
# Structured result helpers — notebooks should use these to emit results
# that the orchestrator and monitoring can consume.
# ---------------------------------------------------------------------------


def make_notebook_result(
    notebook_name: str,
    status: str = "sucesso",
    rows_written: int = 0,
    tables_written: list[str] | None = None,
    error: str | None = None,
    extra: dict | None = None,
) -> str:
    """Build a JSON result string for ``dbutils.notebook.exit()``.

    Example usage at the end of each notebook::

        dbutils.notebook.exit(make_notebook_result(
            "01_bronze", rows_written=150347,
            tables_written=["/Volumes/main/default/pipeline_data/bronze"],
        ))
    """
    payload: dict = {
        "notebook": notebook_name,
        "status": status,
        "rows_written": rows_written,
        "tables_written": tables_written or [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        payload["error"] = error
    if extra:
        payload.update(extra)
    return json.dumps(payload)


def parse_notebook_result(raw: str | None) -> dict:
    """Parse a JSON result string returned by ``dbutils.notebook.run()``."""
    if not raw:
        return {"status": "desconhecido", "error": "resultado vazio"}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {"status": "desconhecido", "raw": str(raw)}


def save_run_metadata(
    spark_or_path,
    notebook_name: str,
    status: str,
    duration_sec: float,
    rows_written: int = 0,
    error: str | None = None,
) -> None:
    """Append a run-metadata record to the monitoring Delta table.

    Works both with a Spark session (Databricks) and with a plain path
    string (local testing via deltalake).

    Notebooks can call this at the end of execution to persist an audit
    trail independently of the orchestrator.
    """
    record = {
        "notebook": notebook_name,
        "status": status,
        "duration_sec": duration_sec,
        "rows_written": rows_written,
        "error": error,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Try Spark path first (Databricks runtime)
    try:
        from pyspark.sql import SparkSession

        if isinstance(spark_or_path, str):
            spark = SparkSession.getActiveSession()
            monitoring_path = f"{spark_or_path}/monitoring/notebook_runs"
        else:
            spark = spark_or_path
            monitoring_path = f"{DEFAULT_VOLUME_ROOT}/monitoring/notebook_runs"

        if spark is not None:
            df = spark.createDataFrame([record])
            df.write.format("delta").mode("append").option("mergeSchema", "true").save(monitoring_path)
            return
    except ImportError:
        pass

    # Fallback: use deltalake library directly
    import polars as pl
    from deltalake import write_deltalake

    if isinstance(spark_or_path, str):
        monitoring_path = f"{spark_or_path}/monitoring/notebook_runs"
    else:
        monitoring_path = "./data/monitoring/notebook_runs"
    Path(monitoring_path).parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame([record])
    write_deltalake(monitoring_path, df.to_arrow(), mode="append", schema_mode="merge")
