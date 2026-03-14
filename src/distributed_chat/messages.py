from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
import uuid


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_message_id() -> str:
    return uuid.uuid4().hex


def encode_packet(packet: dict[str, Any]) -> bytes:
    return (json.dumps(packet, separators=(",", ":")) + "\n").encode("utf-8")


def decode_packet(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))
