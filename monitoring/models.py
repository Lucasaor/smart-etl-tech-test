"""Monitoring data models for pipeline runs, steps, and agent actions."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class StepRun:
    """Record of a single pipeline step execution."""

    step_name: str
    status: RunStatus = RunStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    rows_input: int = 0
    rows_output: int = 0
    error_message: str | None = None
    step_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    @property
    def duration_sec(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class PipelineRun:
    """Record of a full pipeline execution (Bronze → Silver → Gold)."""

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    status: RunStatus = RunStatus.PENDING
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    layers: list[str] = field(default_factory=list)
    steps: list[StepRun] = field(default_factory=list)
    trigger: str = "manual"  # "manual", "monitor_agent", "schedule"

    @property
    def duration_sec(self) -> float | None:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class AgentAction:
    """Record of a single action taken by an agent."""

    agent_name: str
    action: str
    action_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    input_summary: str = ""
    output_summary: str = ""
    model_used: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost: float = 0.0
    success: bool = True
    error_message: str | None = None


@dataclass
class Alert:
    """An alert raised by the monitoring system."""

    alert_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    severity: AlertSeverity = AlertSeverity.INFO
    title: str = ""
    message: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved: bool = False
    source: str = ""  # e.g. "monitor_agent", "pipeline_step"
