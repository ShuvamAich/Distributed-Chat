"""
Fault Detector (Heartbeat-based)
---------------------------------
Each node sends UDP heartbeat pings every HEARTBEAT_INTERVAL seconds to all known peers.
If a peer misses MISS_THRESHOLD consecutive heartbeats, it is declared dead.
A callback is invoked so the Election Manager can start a new election.

Message format (JSON over UDP, port 50001):
  { "type": "HB", "node_id": "...", "ts": <epoch_float> }
  { "type": "HB_ACK", "node_id": "...", "ts": <epoch_float> }
"""

import json
import socket
import threading
import time
from typing import Callable, Dict, List, Optional

HEARTBEAT_PORT = 50001
HEARTBEAT_INTERVAL = 2.0      # seconds between pings
MISS_THRESHOLD = 3            # missed pings before fault declared  (= ~6 s timeout)
BUFFER_SIZE = 1024


class FaultDetector:
    def __init__(
        self,
        node_id: str,
        logger,
        get_peers_fn: Callable[[], Dict[str, dict]],
        on_fault: Optional[Callable[[str], None]] = None,
    ):
        self.node_id = node_id
        self.logger = logger
        self.get_peers = get_peers_fn
        self.on_fault = on_fault
        self._running = False
        self._last_seen: Dict[str, float] = {}     # node_id -> last epoch
        self._miss_count: Dict[str, int] = {}
        self._suspected: set = set()
        self._lock = threading.Lock()

    # ----------------------------------------------------------------- control
    def start(self):
        self._running = True
        threading.Thread(target=self._send_loop, daemon=True, name="hb-send").start()
        threading.Thread(target=self._recv_loop, daemon=True, name="hb-recv").start()
        threading.Thread(target=self._check_loop, daemon=True, name="hb-check").start()
        self.logger.log("SYSTEM", {"heartbeat": "started", "interval": HEARTBEAT_INTERVAL})

    def stop(self):
        self._running = False

    def reset_peer(self, node_id: str):
        """Call this when a peer (re-)joins so it is no longer suspected."""
        with self._lock:
            self._last_seen[node_id] = time.time()
            self._miss_count[node_id] = 0
            self._suspected.discard(node_id)

    def remove_peer(self, node_id: str):
        with self._lock:
            self._last_seen.pop(node_id, None)
            self._miss_count.pop(node_id, None)
            self._suspected.discard(node_id)

    # ----------------------------------------------------------------- send HB
    def _send_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg_template = json.dumps({"type": "HB", "node_id": self.node_id})
        try:
            while self._running:
                peers = self.get_peers()
                for nid, info in peers.items():
                    try:
                        payload = json.dumps({"type": "HB", "node_id": self.node_id, "ts": time.time()}).encode()
                        sock.sendto(payload, (info["ip"], HEARTBEAT_PORT))
                    except Exception:
                        pass
                time.sleep(HEARTBEAT_INTERVAL)
        finally:
            sock.close()

    # ----------------------------------------------------------------- receive HB
    def _recv_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1.0)
        try:
            sock.bind(("", HEARTBEAT_PORT))
        except OSError as e:
            self.logger.log("SYSTEM", {"hb_recv_bind_error": str(e)}, level="ERROR")
            return

        ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while self._running:
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                if msg.get("type") == "HB":
                    sender = msg.get("node_id")
                    if sender and sender != self.node_id:
                        with self._lock:
                            self._last_seen[sender] = time.time()
                            self._miss_count[sender] = 0
                            self._suspected.discard(sender)
                        ack = json.dumps({"type": "HB_ACK", "node_id": self.node_id, "ts": time.time()}).encode()
                        ack_sock.sendto(ack, addr)
            except socket.timeout:
                continue
            except Exception:
                continue
        sock.close()
        ack_sock.close()

    # ----------------------------------------------------------------- monitor
    def _check_loop(self):
        time.sleep(HEARTBEAT_INTERVAL * 2)   # grace period on startup
        while self._running:
            now = time.time()
            peers = self.get_peers()
            for nid in list(peers.keys()):
                with self._lock:
                    last = self._last_seen.get(nid, now)
                    elapsed = now - last
                    if elapsed > HEARTBEAT_INTERVAL * MISS_THRESHOLD:
                        if nid not in self._suspected:
                            self._suspected.add(nid)
                            self._miss_count[nid] = MISS_THRESHOLD
                            self.logger.log(
                                "HEARTBEAT_TIMEOUT",
                                {"suspected_node": nid, "elapsed_s": round(elapsed, 2)},
                                level="WARN",
                            )
                            if self.on_fault:
                                threading.Thread(
                                    target=self.on_fault,
                                    args=(nid,),
                                    daemon=True,
                                ).start()
            time.sleep(HEARTBEAT_INTERVAL)
