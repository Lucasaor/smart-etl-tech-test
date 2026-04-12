"""Pacote de infraestrutura central."""

from core.storage import get_storage_backend
from core.events import EventBus, get_event_bus

__all__ = ["get_storage_backend", "EventBus", "get_event_bus"]
