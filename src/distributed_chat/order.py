from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Iterable


@dataclass(slots=True)
class LamportClock:
    value: int = 0
    _lock: Lock = field(default_factory=Lock)

    def tick(self, observed: int | None = None) -> int:
        with self._lock:
            if observed is None:
                self.value += 1
            else:
                self.value = max(self.value, observed) + 1
            return self.value


class VectorClock(dict[str, int]):
    def advance(self, node_uid: str) -> dict[str, int]:
        self[node_uid] = self.get(node_uid, 0) + 1
        return dict(self)

    def merge(self, incoming: dict[str, int] | None) -> dict[str, int]:
        if not incoming:
            return dict(self)
        for node_uid, value in incoming.items():
            self[node_uid] = max(self.get(node_uid, 0), value)
        return dict(self)

    @staticmethod
    def summarize(clock: dict[str, int], members: Iterable[str]) -> str:
        return ", ".join(f"{member[:6]}={clock.get(member, 0)}" for member in sorted(members))
