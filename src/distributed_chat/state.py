from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import asyncio


@dataclass(slots=True)
class PeerConnection:
    uid: str
    username: str
    host: str
    port: int
    priority: int
    writer: asyncio.StreamWriter
    connected_at: float
    last_seen: float
    is_leader: bool = False
    view_version: int = 0
    delivered_seq: int = 0


@dataclass(slots=True)
class KnownNode:
    uid: str
    username: str
    host: str
    port: int
    priority: int
    last_discovered: float
    leader_uid: str | None = None


@dataclass(slots=True)
class MessageRecord:
    seq: int
    sender_uid: str
    username: str
    text: str
    created_at: str
    delivered_at: str
    lamport: int
    vector_clock: dict[str, int]
    client_message_id: str
    view_version: int
    retries: int = 0
    acknowledgements: set[str] = field(default_factory=set)

    def as_packet(self) -> dict[str, Any]:
        return {
            "type": "ordered_chat",
            "seq": self.seq,
            "sender_uid": self.sender_uid,
            "username": self.username,
            "text": self.text,
            "created_at": self.created_at,
            "delivered_at": self.delivered_at,
            "lamport": self.lamport,
            "vector_clock": self.vector_clock,
            "client_message_id": self.client_message_id,
            "view_version": self.view_version,
        }
