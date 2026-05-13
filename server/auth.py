"""
Authentication Module
----------------------
Provides room-code + username validation.
The leader node maintains the authoritative username registry.
Username must be unique within the room.

Room code is configured at startup (env var or CLI flag).
"""

import hashlib
import threading
from typing import Dict, Optional, Set, Tuple


class AuthManager:
    def __init__(self, room_code: str, logger):
        self.logger = logger
        self._room_hash = hashlib.sha256(room_code.strip().encode()).hexdigest()
        self._lock = threading.Lock()
        self._registered: Dict[str, str] = {}   # node_id -> username
        self._usernames: Set[str] = set()

    def verify_room_code(self, provided_code: str) -> bool:
        h = hashlib.sha256(provided_code.strip().encode()).hexdigest()
        return h == self._room_hash

    def register_user(self, node_id: str, username: str) -> Tuple[bool, str]:
        """
        Attempt to register a username for node_id.
        Returns (ok, reason).
        """
        username = username.strip()
        if not username or len(username) > 32:
            return False, "Username must be 1-32 characters"
        if not username.replace("_", "").replace("-", "").isalnum():
            return False, "Username may only contain letters, digits, _ or -"

        with self._lock:
            if node_id in self._registered:
                old = self._registered[node_id]
                if old == username:
                    return True, "already_registered"
                # Changing username
                self._usernames.discard(old)

            if username.lower() in {u.lower() for u in self._usernames}:
                return False, "Username already taken"

            self._registered[node_id] = username
            self._usernames.add(username)

        self.logger.log("AUTH_OK", {"node_id": node_id, "username": username})
        return True, "ok"

    def deregister(self, node_id: str):
        with self._lock:
            username = self._registered.pop(node_id, None)
            if username:
                self._usernames.discard(username)

    def get_username(self, node_id: str) -> Optional[str]:
        with self._lock:
            return self._registered.get(node_id)

    def get_all_users(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._registered)
