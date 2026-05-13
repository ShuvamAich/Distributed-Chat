"""
Structured event logger for the distributed chat middleware.
Every middleware event is logged with ISO timestamp, node ID, event type, and payload.
Logs go to stdout (with color) and to a per-node log file for post-demo review.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import colorama
from colorama import Fore, Style

colorama.init(autoreset=True)

EVENT_COLORS = {
    "NODE_JOIN":          Fore.GREEN,
    "NODE_LEAVE":         Fore.YELLOW,
    "ELECTION_START":     Fore.CYAN,
    "ELECTION_ROUND":     Fore.CYAN,
    "COORDINATOR_ELECTED":Fore.MAGENTA,
    "MSG_SENT":           Fore.WHITE,
    "MSG_SEQUENCED":      Fore.BLUE,
    "MSG_DELIVERED":      Fore.GREEN,
    "ACK":                Fore.WHITE,
    "RETRANSMIT":         Fore.YELLOW,
    "HEARTBEAT_TIMEOUT":  Fore.RED,
    "VIEW_CHANGE":        Fore.MAGENTA,
    "AUTH_OK":            Fore.GREEN,
    "AUTH_FAIL":          Fore.RED,
    "FAULT_DETECTED":     Fore.RED,
    "DISCOVERY":          Fore.CYAN,
    "SYSTEM":             Fore.WHITE,
}


class NodeLogger:
    def __init__(self, node_id: str, log_dir: str = "logs"):
        self.node_id = node_id
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"node_{node_id}.log")

        self._file_logger = logging.getLogger(f"node_{node_id}")
        self._file_logger.setLevel(logging.DEBUG)
        if not self._file_logger.handlers:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(message)s"))
            self._file_logger.addHandler(fh)

        self._subscribers = []

    def subscribe(self, callback):
        """Subscribe to receive log events (used by WS bridge to push to browser)."""
        self._subscribers.append(callback)

    def unsubscribe(self, callback):
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def log(
        self,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
        level: str = "INFO",
    ) -> Dict:
        ts = datetime.now(timezone.utc).isoformat()
        record = {
            "timestamp": ts,
            "node_id": self.node_id,
            "event_type": event_type,
            "level": level,
            "data": data or {},
        }
        json_line = json.dumps(record)
        self._file_logger.info(json_line)

        color = EVENT_COLORS.get(event_type, Fore.WHITE)
        severity_color = Fore.RED if level == "ERROR" else Fore.YELLOW if level == "WARN" else ""
        print(
            f"{Fore.WHITE}[{ts}] "
            f"{Fore.CYAN}[{self.node_id[:8]}] "
            f"{color}[{event_type}] "
            f"{severity_color}{json.dumps(data or {})}{Style.RESET_ALL}"
        )

        for cb in list(self._subscribers):
            try:
                cb(record)
            except Exception:
                pass

        return record
