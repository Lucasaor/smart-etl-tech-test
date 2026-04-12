"""Monitoring persistence — SQLite (local) or Delta tables (Databricks).

Stores pipeline runs, step runs, agent actions, and alerts.
Provides query methods for the frontend dashboards.
"""

from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Generator

import structlog

from config.settings import get_settings
from monitoring.models import (
    AgentAction,
    Alert,
    AlertSeverity,
    PipelineRun,
    RunStatus,
    StepRun,
)

logger = structlog.get_logger(__name__)


class MonitoringBackend(ABC):
    """Abstract interface for monitoring persistence."""

    @abstractmethod
    def save_pipeline_run(self, run: PipelineRun) -> None: ...

    @abstractmethod
    def get_pipeline_runs(self, limit: int = 50) -> list[PipelineRun]: ...

    @abstractmethod
    def save_agent_action(self, action: AgentAction) -> None: ...

    @abstractmethod
    def get_agent_actions(self, agent_name: str | None = None, limit: int = 100) -> list[AgentAction]: ...

    @abstractmethod
    def save_alert(self, alert: Alert) -> None: ...

    @abstractmethod
    def get_alerts(self, unresolved_only: bool = True, limit: int = 50) -> list[Alert]: ...

    @abstractmethod
    def get_total_llm_cost(self) -> float: ...

    @abstractmethod
    def get_total_tokens(self) -> dict[str, int]: ...

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    layers TEXT NOT NULL,
    trigger TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS step_runs (
    step_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    rows_input INTEGER DEFAULT 0,
    rows_output INTEGER DEFAULT 0,
    error_message TEXT,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

CREATE TABLE IF NOT EXISTS agent_actions (
    action_id TEXT PRIMARY KEY,
    agent_name TEXT NOT NULL,
    action TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    input_summary TEXT,
    output_summary TEXT,
    model_used TEXT,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cost REAL DEFAULT 0.0,
    success INTEGER DEFAULT 1,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT,
    timestamp TEXT NOT NULL,
    resolved INTEGER DEFAULT 0,
    source TEXT
);
"""


class MonitoringStore(MonitoringBackend):
    """SQLite-based monitoring persistence for local runs."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(_SCHEMA_SQL)

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # --- Pipeline Runs ---

    def save_pipeline_run(self, run: PipelineRun) -> None:
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO pipeline_runs
                   (run_id, status, started_at, completed_at, layers, trigger)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    run.run_id,
                    run.status.value,
                    run.started_at.isoformat(),
                    run.completed_at.isoformat() if run.completed_at else None,
                    json.dumps(run.layers),
                    run.trigger,
                ),
            )
            for step in run.steps:
                conn.execute(
                    """INSERT OR REPLACE INTO step_runs
                       (step_id, run_id, step_name, status, started_at, completed_at,
                        rows_input, rows_output, error_message)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        step.step_id,
                        run.run_id,
                        step.step_name,
                        step.status.value,
                        step.started_at.isoformat() if step.started_at else None,
                        step.completed_at.isoformat() if step.completed_at else None,
                        step.rows_input,
                        step.rows_output,
                        step.error_message,
                    ),
                )

    def get_pipeline_runs(self, limit: int = 50) -> list[PipelineRun]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            runs = []
            for row in rows:
                steps = self._get_steps_for_run(conn, row["run_id"])
                runs.append(
                    PipelineRun(
                        run_id=row["run_id"],
                        status=RunStatus(row["status"]),
                        started_at=datetime.fromisoformat(row["started_at"]),
                        completed_at=(
                            datetime.fromisoformat(row["completed_at"])
                            if row["completed_at"]
                            else None
                        ),
                        layers=json.loads(row["layers"]),
                        steps=steps,
                        trigger=row["trigger"],
                    )
                )
            return runs

    def _get_steps_for_run(self, conn: sqlite3.Connection, run_id: str) -> list[StepRun]:
        rows = conn.execute(
            "SELECT * FROM step_runs WHERE run_id = ? ORDER BY started_at",
            (run_id,),
        ).fetchall()
        return [
            StepRun(
                step_id=r["step_id"],
                step_name=r["step_name"],
                status=RunStatus(r["status"]),
                started_at=datetime.fromisoformat(r["started_at"]) if r["started_at"] else None,
                completed_at=(
                    datetime.fromisoformat(r["completed_at"]) if r["completed_at"] else None
                ),
                rows_input=r["rows_input"],
                rows_output=r["rows_output"],
                error_message=r["error_message"],
            )
            for r in rows
        ]

    # --- Agent Actions ---

    def save_agent_action(self, action: AgentAction) -> None:
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO agent_actions
                   (action_id, agent_name, action, timestamp, input_summary,
                    output_summary, model_used, input_tokens, output_tokens,
                    cost, success, error_message)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    action.action_id,
                    action.agent_name,
                    action.action,
                    action.timestamp.isoformat(),
                    action.input_summary,
                    action.output_summary,
                    action.model_used,
                    action.input_tokens,
                    action.output_tokens,
                    action.cost,
                    int(action.success),
                    action.error_message,
                ),
            )

    def get_agent_actions(self, agent_name: str | None = None, limit: int = 100) -> list[AgentAction]:
        with self._conn() as conn:
            if agent_name:
                rows = conn.execute(
                    "SELECT * FROM agent_actions WHERE agent_name = ? ORDER BY timestamp DESC LIMIT ?",
                    (agent_name, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM agent_actions ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [
                AgentAction(
                    action_id=r["action_id"],
                    agent_name=r["agent_name"],
                    action=r["action"],
                    timestamp=datetime.fromisoformat(r["timestamp"]),
                    input_summary=r["input_summary"] or "",
                    output_summary=r["output_summary"] or "",
                    model_used=r["model_used"] or "",
                    input_tokens=r["input_tokens"],
                    output_tokens=r["output_tokens"],
                    cost=r["cost"],
                    success=bool(r["success"]),
                    error_message=r["error_message"],
                )
                for r in rows
            ]

    # --- Alerts ---

    def save_alert(self, alert: Alert) -> None:
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO alerts
                   (alert_id, severity, title, message, timestamp, resolved, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    alert.alert_id,
                    alert.severity.value,
                    alert.title,
                    alert.message,
                    alert.timestamp.isoformat(),
                    int(alert.resolved),
                    alert.source,
                ),
            )

    def get_alerts(self, unresolved_only: bool = True, limit: int = 50) -> list[Alert]:
        with self._conn() as conn:
            query = "SELECT * FROM alerts"
            if unresolved_only:
                query += " WHERE resolved = 0"
            query += " ORDER BY timestamp DESC LIMIT ?"
            rows = conn.execute(query, (limit,)).fetchall()
            return [
                Alert(
                    alert_id=r["alert_id"],
                    severity=AlertSeverity(r["severity"]),
                    title=r["title"],
                    message=r["message"] or "",
                    timestamp=datetime.fromisoformat(r["timestamp"]),
                    resolved=bool(r["resolved"]),
                    source=r["source"] or "",
                )
                for r in rows
            ]

    # --- Aggregates ---

    def get_total_llm_cost(self) -> float:
        with self._conn() as conn:
            row = conn.execute("SELECT COALESCE(SUM(cost), 0.0) as total FROM agent_actions").fetchone()
            return float(row["total"])

    def get_total_tokens(self) -> dict[str, int]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(input_tokens), 0) as inp, COALESCE(SUM(output_tokens), 0) as out FROM agent_actions"
            ).fetchone()
            return {"input_tokens": row["inp"], "output_tokens": row["out"]}


# ---------------------------------------------------------------------------
# DeltaMonitoringStore — for Databricks execution (append-only Delta tables)
# ---------------------------------------------------------------------------


class DeltaMonitoringStore(MonitoringBackend):
    """Monitoring persistence using Delta tables for Databricks.

    Stores records as append-only Delta tables under the configured
    monitoring path.  Each entity type (pipeline_runs, agent_actions,
    alerts) gets its own sub-table.

    Designed for Databricks Community Edition where no external DB is
    available.  Records are serialised via Polars → deltalake so the
    same code can also be exercised locally without PySpark.
    """

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path
        self._runs_path = f"{base_path}/pipeline_runs"
        self._actions_path = f"{base_path}/agent_actions"
        self._alerts_path = f"{base_path}/alerts"

    # --- helpers ---

    @staticmethod
    def _write_delta(df: "pl.DataFrame", path: str) -> None:  # noqa: F821
        import polars as pl
        from deltalake import write_deltalake
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(path, df.to_arrow(), mode="append", schema_mode="merge")

    @staticmethod
    def _read_delta(path: str) -> "pl.DataFrame":  # noqa: F821
        import polars as pl
        from deltalake import DeltaTable
        try:
            dt = DeltaTable(path)
            return pl.from_arrow(dt.to_pyarrow_table())
        except Exception:
            return pl.DataFrame()

    # --- Pipeline Runs ---

    def save_pipeline_run(self, run: PipelineRun) -> None:
        import polars as pl

        rows = [{
            "run_id": run.run_id,
            "status": run.status.value,
            "started_at": run.started_at.isoformat(),
            "completed_at": run.completed_at.isoformat() if run.completed_at else "",
            "layers": json.dumps(run.layers),
            "trigger": run.trigger,
            "steps_json": json.dumps([
                {
                    "step_id": s.step_id,
                    "step_name": s.step_name,
                    "status": s.status.value,
                    "started_at": s.started_at.isoformat() if s.started_at else "",
                    "completed_at": s.completed_at.isoformat() if s.completed_at else "",
                    "rows_input": s.rows_input,
                    "rows_output": s.rows_output,
                    "error_message": s.error_message or "",
                }
                for s in run.steps
            ]),
        }]
        self._write_delta(pl.DataFrame(rows), self._runs_path)

    def get_pipeline_runs(self, limit: int = 50) -> list[PipelineRun]:
        df = self._read_delta(self._runs_path)
        if df.is_empty():
            return []
        df = df.sort("started_at", descending=True).head(limit)
        runs: list[PipelineRun] = []
        for row in df.iter_rows(named=True):
            steps_raw = json.loads(row.get("steps_json", "[]"))
            steps = [
                StepRun(
                    step_id=s["step_id"],
                    step_name=s["step_name"],
                    status=RunStatus(s["status"]),
                    started_at=datetime.fromisoformat(s["started_at"]) if s.get("started_at") else None,
                    completed_at=datetime.fromisoformat(s["completed_at"]) if s.get("completed_at") else None,
                    rows_input=s.get("rows_input", 0),
                    rows_output=s.get("rows_output", 0),
                    error_message=s.get("error_message") or None,
                )
                for s in steps_raw
            ]
            runs.append(PipelineRun(
                run_id=row["run_id"],
                status=RunStatus(row["status"]),
                started_at=datetime.fromisoformat(row["started_at"]),
                completed_at=datetime.fromisoformat(row["completed_at"]) if row.get("completed_at") else None,
                layers=json.loads(row.get("layers", "[]")),
                steps=steps,
                trigger=row.get("trigger", "manual"),
            ))
        return runs

    # --- Agent Actions ---

    def save_agent_action(self, action: AgentAction) -> None:
        import polars as pl

        rows = [{
            "action_id": action.action_id,
            "agent_name": action.agent_name,
            "action": action.action,
            "timestamp": action.timestamp.isoformat(),
            "input_summary": action.input_summary or "",
            "output_summary": action.output_summary or "",
            "model_used": action.model_used or "",
            "input_tokens": action.input_tokens,
            "output_tokens": action.output_tokens,
            "cost": action.cost,
            "success": action.success,
            "error_message": action.error_message or "",
        }]
        self._write_delta(pl.DataFrame(rows), self._actions_path)

    def get_agent_actions(self, agent_name: str | None = None, limit: int = 100) -> list[AgentAction]:
        import polars as pl

        df = self._read_delta(self._actions_path)
        if df.is_empty():
            return []
        if agent_name:
            df = df.filter(pl.col("agent_name") == agent_name)
        df = df.sort("timestamp", descending=True).head(limit)
        return [
            AgentAction(
                action_id=r["action_id"],
                agent_name=r["agent_name"],
                action=r["action"],
                timestamp=datetime.fromisoformat(r["timestamp"]),
                input_summary=r.get("input_summary") or "",
                output_summary=r.get("output_summary") or "",
                model_used=r.get("model_used") or "",
                input_tokens=r.get("input_tokens", 0),
                output_tokens=r.get("output_tokens", 0),
                cost=r.get("cost", 0.0),
                success=bool(r.get("success", True)),
                error_message=r.get("error_message"),
            )
            for r in df.iter_rows(named=True)
        ]

    # --- Alerts ---

    def save_alert(self, alert: Alert) -> None:
        import polars as pl

        rows = [{
            "alert_id": alert.alert_id,
            "severity": alert.severity.value,
            "title": alert.title,
            "message": alert.message,
            "timestamp": alert.timestamp.isoformat(),
            "resolved": alert.resolved,
            "source": alert.source,
        }]
        self._write_delta(pl.DataFrame(rows), self._alerts_path)

    def get_alerts(self, unresolved_only: bool = True, limit: int = 50) -> list[Alert]:
        import polars as pl

        df = self._read_delta(self._alerts_path)
        if df.is_empty():
            return []
        if unresolved_only:
            df = df.filter(pl.col("resolved") == False)  # noqa: E712
        df = df.sort("timestamp", descending=True).head(limit)
        return [
            Alert(
                alert_id=r["alert_id"],
                severity=AlertSeverity(r["severity"]),
                title=r["title"],
                message=r.get("message") or "",
                timestamp=datetime.fromisoformat(r["timestamp"]),
                resolved=bool(r.get("resolved", False)),
                source=r.get("source") or "",
            )
            for r in df.iter_rows(named=True)
        ]

    # --- Aggregates ---

    def get_total_llm_cost(self) -> float:
        import polars as pl

        df = self._read_delta(self._actions_path)
        if df.is_empty():
            return 0.0
        return float(df.select(pl.col("cost").sum()).item())

    def get_total_tokens(self) -> dict[str, int]:
        import polars as pl

        df = self._read_delta(self._actions_path)
        if df.is_empty():
            return {"input_tokens": 0, "output_tokens": 0}
        return {
            "input_tokens": int(df.select(pl.col("input_tokens").sum()).item()),
            "output_tokens": int(df.select(pl.col("output_tokens").sum()).item()),
        }


@lru_cache(maxsize=1)
def get_monitoring_store() -> MonitoringBackend:
    """Singleton factory — returns SQLite store locally, Delta store on Databricks.

    Uses DeltaMonitoringStore when running on Databricks OR when
    ``force_delta_monitoring`` is set (e.g. Databricks FUSE mode with
    RUNTIME_ENV=local and DATA_ROOT=/dbfs/delta).
    """
    settings = get_settings()
    if settings.is_databricks or settings.force_delta_monitoring:
        logger.info("monitoring_store", backend="delta", path=settings.monitoring_db_path)
        return DeltaMonitoringStore(settings.monitoring_db_path)
    logger.info("monitoring_store", backend="sqlite", path=settings.monitoring_db_path)
    return MonitoringStore(settings.monitoring_db_path)
