"""Monitoring persistence — SQLite (local) or Delta tables (Databricks).

Stores pipeline runs, step runs, agent actions, and alerts.
Provides query methods for the frontend dashboards.
"""

from __future__ import annotations

import json
import sqlite3
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


class MonitoringStore:
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


@lru_cache(maxsize=1)
def get_monitoring_store() -> MonitoringStore:
    """Singleton factory for the monitoring store."""
    settings = get_settings()
    return MonitoringStore(settings.monitoring_db_path)
