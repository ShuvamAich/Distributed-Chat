"""
Group View Manager
------------------
Maintains the current membership view of the distributed system.
A "view" is a versioned, ordered list of active node IDs.
View changes are triggered on node join / leave / crash.

View-synchrony guarantee (lite):
  - No message is delivered across a view boundary.
  - All nodes must install the same view before exchanging new application messages.
"""

import threading
import time
from typing import Callable, Dict, List, Optional


class GroupView:
    def __init__(self, node_id: str, logger):
        self.node_id = node_id
        self.logger = logger
        self._lock = threading.Lock()
        self._view_id: int = 0
        self._members: Dict[str, dict] = {}   # node_id -> {ip, tcp_port, username, joined_at}
        self._on_change_callbacks: List[Callable] = []

    # --------------------------------------------------------- callbacks
    def on_view_change(self, cb: Callable):
        """Register a callback(view_id, members) called on every view change."""
        self._on_change_callbacks.append(cb)

    def _notify(self):
        view = self.get_view()
        for cb in list(self._on_change_callbacks):
            try:
                cb(self._view_id, view)
            except Exception:
                pass

    # --------------------------------------------------------- mutations
    def add_member(self, node_id: str, ip: str, tcp_port: int, username: str = ""):
        with self._lock:
            if node_id not in self._members:
                self._view_id += 1
                self._members[node_id] = {
                    "ip": ip,
                    "tcp_port": tcp_port,
                    "username": username,
                    "joined_at": time.time(),
                }
                self.logger.log(
                    "VIEW_CHANGE",
                    {
                        "action": "ADD",
                        "node": node_id,
                        "username": username,
                        "view_id": self._view_id,
                        "members": list(self._members.keys()),
                    },
                )
        self._notify()

    def remove_member(self, node_id: str):
        changed = False
        with self._lock:
            if node_id in self._members:
                del self._members[node_id]
                self._view_id += 1
                changed = True
                self.logger.log(
                    "VIEW_CHANGE",
                    {
                        "action": "REMOVE",
                        "node": node_id,
                        "view_id": self._view_id,
                        "members": list(self._members.keys()),
                    },
                )
        if changed:
            self._notify()

    def update_username(self, node_id: str, username: str):
        with self._lock:
            if node_id in self._members:
                self._members[node_id]["username"] = username

    # --------------------------------------------------------- queries
    def get_view(self) -> dict:
        with self._lock:
            return {
                "view_id": self._view_id,
                "members": {
                    nid: dict(info) for nid, info in self._members.items()
                },
            }

    def member_ids(self) -> List[str]:
        with self._lock:
            return list(self._members.keys())

    def get_member(self, node_id: str) -> Optional[dict]:
        with self._lock:
            return dict(self._members[node_id]) if node_id in self._members else None

    def is_member(self, node_id: str) -> bool:
        with self._lock:
            return node_id in self._members

    def count(self) -> int:
        with self._lock:
            return len(self._members)
