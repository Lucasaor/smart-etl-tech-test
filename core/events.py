"""In-process event bus for pipeline and agent events.

Lightweight pub/sub used to decouple pipeline execution from monitoring,
alerting, and the frontend. Events are emitted synchronously; subscribers
persist them to the monitoring store.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import Any, Callable

import structlog

logger = structlog.get_logger(__name__)


# --- Event Types ---

class EventType(str, Enum):
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"
    STEP_STARTED = "step_started"
    STEP_COMPLETED = "step_completed"
    STEP_FAILED = "step_failed"
    DATA_INGESTED = "data_ingested"
    AGENT_ACTION = "agent_action"
    AGENT_ERROR = "agent_error"
    ALERT_RAISED = "alert_raised"


@dataclass
class Event:
    """A single event emitted by the pipeline or an agent."""

    event_type: EventType
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    data: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"[{self.event_type.value}] {self.event_id} @ {self.timestamp.isoformat()}"


# --- Subscriber type ---
Subscriber = Callable[[Event], None]


class EventBus:
    """Simple synchronous pub/sub event bus."""

    def __init__(self) -> None:
        self._subscribers: dict[EventType | None, list[Subscriber]] = defaultdict(list)

    def subscribe(self, event_type: EventType | None, callback: Subscriber) -> None:
        """Subscribe to a specific event type. Use None to subscribe to all."""
        self._subscribers[event_type].append(callback)
        logger.debug("event_subscribed", event_type=event_type)

    def emit(self, event: Event) -> None:
        """Emit an event to all matching subscribers."""
        logger.info("event_emitted", event_type=event.event_type.value, event_id=event.event_id)

        # Notify specific subscribers
        for callback in self._subscribers.get(event.event_type, []):
            try:
                callback(event)
            except Exception:
                logger.exception("subscriber_error", event_id=event.event_id)

        # Notify wildcard subscribers (subscribed to None)
        for callback in self._subscribers.get(None, []):
            try:
                callback(event)
            except Exception:
                logger.exception("subscriber_error", event_id=event.event_id)

    def clear(self) -> None:
        """Remove all subscribers."""
        self._subscribers.clear()


# --- Convenience emitters ---

def emit_pipeline_started(bus: EventBus, run_id: str, layers: list[str]) -> Event:
    event = Event(
        event_type=EventType.PIPELINE_STARTED,
        data={"run_id": run_id, "layers": layers},
    )
    bus.emit(event)
    return event


def emit_step_completed(
    bus: EventBus, run_id: str, step: str, rows_processed: int, duration_sec: float
) -> Event:
    event = Event(
        event_type=EventType.STEP_COMPLETED,
        data={
            "run_id": run_id,
            "step": step,
            "rows_processed": rows_processed,
            "duration_sec": round(duration_sec, 2),
        },
    )
    bus.emit(event)
    return event


def emit_step_failed(bus: EventBus, run_id: str, step: str, error: str) -> Event:
    event = Event(
        event_type=EventType.STEP_FAILED,
        data={"run_id": run_id, "step": step, "error": error},
    )
    bus.emit(event)
    return event


def emit_agent_action(
    bus: EventBus,
    agent: str,
    action: str,
    details: dict[str, Any] | None = None,
) -> Event:
    event = Event(
        event_type=EventType.AGENT_ACTION,
        data={"agent": agent, "action": action, **(details or {})},
    )
    bus.emit(event)
    return event


@lru_cache(maxsize=1)
def get_event_bus() -> EventBus:
    """Singleton EventBus for the application."""
    return EventBus()
