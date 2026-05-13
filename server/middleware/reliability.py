"""
Reliability Layer — ACK + Selective Retransmit
-----------------------------------------------
Wraps every peer-to-peer TCP message with:
  - A unique message ID
  - Sequence tracking per sender
  - ACK expected within ACK_TIMEOUT; retransmitted up to MAX_RETRIES times

Used by the Sequencer for LEADER→MEMBER broadcasts so every node receives
every sequenced message exactly once.

Outbound message envelope (JSON):
  {
    "type":    "DATA" | "ACK" | "RETRANSMIT_REQ",
    "msg_id":  "uuid-string",
    "seq":     N,             # global sequence number (0 for non-sequenced)
    "payload": { ... }        # original message
  }
"""

import json
import threading
import time
import uuid
from typing import Callable, Dict, Optional, Tuple

ACK_TIMEOUT = 2.0
MAX_RETRIES = 3
BUFFER_SIZE = 65535


class ReliabilityManager:
    def __init__(self, node_id: str, logger):
        self.node_id = node_id
        self.logger = logger
        self._lock = threading.Lock()
        # pending: msg_id -> (envelope_bytes, retry_count, last_sent, send_fn)
        self._pending: Dict[str, Tuple] = {}
        self._acked: set = set()          # msg_ids we have ACKed (dedup)
        self._delivered: set = set()      # msg_ids already delivered upstream
        self._running = False

    def start(self):
        self._running = True
        threading.Thread(target=self._retry_loop, daemon=True, name="reliability-retry").start()

    def stop(self):
        self._running = False

    # ----------------------------------------------------------------- send
    def send_reliable(
        self,
        payload: dict,
        seq: int,
        send_fn: Callable[[bytes], bool],
        msg_id: Optional[str] = None,
    ) -> str:
        """
        Wrap payload in a reliable envelope and send via send_fn.
        send_fn(bytes) -> bool: returns True if send succeeded.
        Returns msg_id.
        """
        msg_id = msg_id or str(uuid.uuid4())
        envelope = {
            "type": "DATA",
            "msg_id": msg_id,
            "seq": seq,
            "sender": self.node_id,
            "payload": payload,
        }
        raw = self._encode(envelope)
        ok = send_fn(raw)
        if ok:
            with self._lock:
                self._pending[msg_id] = (raw, 0, time.time(), send_fn)
        return msg_id

    def ack_received(self, msg_id: str):
        """Call this when an ACK for msg_id is received."""
        with self._lock:
            if msg_id in self._pending:
                del self._pending[msg_id]
                self.logger.log("ACK", {"msg_id": msg_id[:8]})

    def receive_message(
        self,
        envelope: dict,
        send_ack_fn: Callable[[bytes], None],
        deliver_fn: Callable[[dict, int], None],
    ):
        """
        Process an incoming DATA envelope.
        - Sends ACK immediately.
        - Deduplicates and calls deliver_fn(payload, seq) for new messages.
        """
        mtype = envelope.get("type")
        msg_id = envelope.get("msg_id")

        if mtype == "ACK":
            self.ack_received(msg_id)
            return

        if mtype == "DATA":
            # Always ACK
            ack = self._encode({"type": "ACK", "msg_id": msg_id, "sender": self.node_id})
            try:
                send_ack_fn(ack)
            except Exception:
                pass

            with self._lock:
                already_delivered = msg_id in self._delivered
                if not already_delivered:
                    self._delivered.add(msg_id)

            if not already_delivered:
                seq = envelope.get("seq", 0)
                payload = envelope.get("payload", {})
                deliver_fn(payload, seq)

    # ----------------------------------------------------------------- retry
    def _retry_loop(self):
        while self._running:
            now = time.time()
            with self._lock:
                to_retry = []
                to_fail = []
                for msg_id, (raw, retries, last_sent, send_fn) in self._pending.items():
                    if now - last_sent > ACK_TIMEOUT:
                        if retries < MAX_RETRIES:
                            to_retry.append((msg_id, raw, retries + 1, send_fn))
                        else:
                            to_fail.append(msg_id)
                for msg_id, raw, new_retries, send_fn in to_retry:
                    self._pending[msg_id] = (raw, new_retries, now, send_fn)
                for msg_id in to_fail:
                    del self._pending[msg_id]
                    self.logger.log(
                        "RETRANSMIT",
                        {"msg_id": msg_id[:8], "status": "FAILED_MAX_RETRIES"},
                        level="WARN",
                    )

            for msg_id, raw, retries, send_fn in to_retry:
                self.logger.log("RETRANSMIT", {"msg_id": msg_id[:8], "attempt": retries})
                try:
                    send_fn(raw)
                except Exception:
                    pass
            time.sleep(0.5)

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _encode(obj: dict) -> bytes:
        raw = json.dumps(obj).encode()
        return len(raw).to_bytes(4, "big") + raw

    @staticmethod
    def decode_frame(data: bytes) -> Optional[dict]:
        try:
            return json.loads(data)
        except Exception:
            return None
