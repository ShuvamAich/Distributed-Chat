from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .messages import utc_now


EventSink = Callable[[dict], None]


@dataclass(slots=True)
class EventLogger:
    sink: EventSink

    def emit(self, level: str, category: str, message: str, **extra: object) -> None:
        payload = {
            "kind": "event",
            "timestamp": utc_now(),
            "level": level,
            "category": category,
            "message": message,
        }
        payload.update(extra)
        self.sink(payload)

    def info(self, category: str, message: str, **extra: object) -> None:
        self.emit("INFO", category, message, **extra)

    def warning(self, category: str, message: str, **extra: object) -> None:
        self.emit("WARN", category, message, **extra)

    def error(self, category: str, message: str, **extra: object) -> None:
        self.emit("ERROR", category, message, **extra)
