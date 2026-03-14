from __future__ import annotations

from dataclasses import dataclass, field
import os
import socket
import time
import uuid


def detect_host_ip() -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())
    finally:
        sock.close()


@dataclass(slots=True)
class ChatConfig:
    room_name: str = "demo-room"
    username: str = "user"
    host: str = field(default_factory=detect_host_ip)
    tcp_port: int = 60000
    multicast_group: str = "239.255.42.99"
    multicast_port: int = 45454
    discovery_interval: float = 2.0
    heartbeat_interval: float = 1.5
    failure_timeout: float = 6.0
    ack_timeout: float = 1.5
    retransmit_limit: int = 6
    history_limit: int = 500
    ordering_mode: str = "total"
    priority: int = field(default_factory=lambda: int(time.time() * 1000) % 1_000_000)
    node_uid: str = field(default_factory=lambda: uuid.uuid4().hex)
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "ChatConfig":
        return cls(
            room_name=os.getenv("CHAT_ROOM", "demo-room"),
            username=os.getenv("CHAT_USER", "user"),
            host=os.getenv("CHAT_HOST", detect_host_ip()),
            tcp_port=int(os.getenv("CHAT_TCP_PORT", "60000")),
            multicast_group=os.getenv("CHAT_MULTICAST_GROUP", "239.255.42.99"),
            multicast_port=int(os.getenv("CHAT_MULTICAST_PORT", "45454")),
            priority=int(os.getenv("CHAT_PRIORITY", str(int(time.time() * 1000) % 1_000_000))),
        )
