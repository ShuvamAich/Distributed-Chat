"""
Bully Election Algorithm
--------------------------
Each node has a numeric priority derived from its node_id (UUID → int).
Higher priority wins.

Protocol (TCP messages on election_port = tcp_port + 1):
  ELECTION  { "type": "ELECTION",    "node_id": "...", "priority": N }
  OK        { "type": "OK",          "node_id": "...", "priority": N }
  COORDINATOR { "type": "COORDINATOR","node_id": "...", "priority": N }

On trigger:
  1. Node sends ELECTION to all peers with higher priority.
  2. If it receives OK within ELECTION_TIMEOUT → it lost, waits for COORDINATOR.
  3. If no OK received → it won, broadcasts COORDINATOR.
  4. On receiving COORDINATOR → update current_leader.
  5. On receiving ELECTION from lower-priority node → send OK, start own election.
"""

import json
import socket
import threading
import time
import uuid
from typing import Callable, Dict, Optional

ELECTION_TIMEOUT = 3.0     # seconds to wait for OK response
COORDINATOR_TIMEOUT = 5.0  # seconds to wait for COORDINATOR after getting OK
BUFFER_SIZE = 4096


def node_priority(node_id: str) -> int:
    """Convert UUID node_id to a comparable integer."""
    try:
        return uuid.UUID(node_id).int
    except Exception:
        return hash(node_id) & 0xFFFFFFFF


class ElectionManager:
    def __init__(
        self,
        node_id: str,
        tcp_port: int,          # base TCP port; election listens on tcp_port+1
        logger,
        get_peers_fn: Callable[[], Dict[str, dict]],
        on_leader_change: Optional[Callable[[str], None]] = None,
    ):
        self.node_id = node_id
        self.priority = node_priority(node_id)
        self.tcp_port = tcp_port
        self.election_port = tcp_port + 1
        self.logger = logger
        self.get_peers = get_peers_fn
        self.on_leader_change = on_leader_change

        self.current_leader: Optional[str] = None
        self._election_in_progress = False
        self._received_ok = False
        self._lock = threading.Lock()
        self._running = False

    # ----------------------------------------------------------------- control
    def start(self):
        self._running = True
        threading.Thread(target=self._listen_loop, daemon=True, name="election-listen").start()
        self.logger.log("SYSTEM", {"election": "listener started", "port": self.election_port})

    def stop(self):
        self._running = False

    # ----------------------------------------------------------------- trigger
    def trigger_election(self, reason: str = "manual"):
        with self._lock:
            if self._election_in_progress:
                return
            self._election_in_progress = True
            self._received_ok = False

        self.logger.log(
            "ELECTION_START",
            {
                "initiator": self.node_id,
                "priority": self.priority,
                "reason": reason,
            },
        )
        threading.Thread(target=self._run_election, args=(reason,), daemon=True).start()

    def _run_election(self, reason: str):
        peers = self.get_peers()
        higher_peers = {
            nid: info
            for nid, info in peers.items()
            if node_priority(nid) > self.priority
        }

        self.logger.log(
            "ELECTION_ROUND",
            {
                "node": self.node_id,
                "higher_peers": list(higher_peers.keys()),
                "total_peers": len(peers),
            },
        )

        if not higher_peers:
            # We are the highest — declare victory immediately
            self._declare_coordinator()
            return

        # Send ELECTION to all higher-priority peers
        ok_received = threading.Event()
        for nid, info in higher_peers.items():
            threading.Thread(
                target=self._send_election,
                args=(info["ip"], info["tcp_port"] + 1, ok_received),
                daemon=True,
            ).start()

        ok_received.wait(timeout=ELECTION_TIMEOUT)

        with self._lock:
            if self._received_ok:
                # Wait for COORDINATOR message
                self.logger.log(
                    "ELECTION_ROUND",
                    {"node": self.node_id, "status": "received_ok_waiting_for_coordinator"},
                )
                self._election_in_progress = False
                # If no coordinator arrives in time, restart election
                threading.Timer(COORDINATOR_TIMEOUT, self._coordinator_timeout_check).start()
            else:
                # No one higher responded — we win
                self._declare_coordinator()

    def _coordinator_timeout_check(self):
        if self.current_leader is None or not self._is_leader_alive():
            self.logger.log(
                "ELECTION_START",
                {"reason": "coordinator_timeout", "node": self.node_id},
            )
            self.trigger_election(reason="coordinator_timeout")

    def _is_leader_alive(self) -> bool:
        if self.current_leader == self.node_id:
            return True
        peers = self.get_peers()
        return self.current_leader in peers

    def _declare_coordinator(self):
        self.current_leader = self.node_id
        self.logger.log(
            "COORDINATOR_ELECTED",
            {"leader": self.node_id, "priority": self.priority},
        )
        peers = self.get_peers()
        msg = json.dumps({
            "type": "COORDINATOR",
            "node_id": self.node_id,
            "priority": self.priority,
        }).encode()
        for nid, info in peers.items():
            threading.Thread(
                target=self._send_tcp_msg,
                args=(info["ip"], info["tcp_port"] + 1, msg),
                daemon=True,
            ).start()
        with self._lock:
            self._election_in_progress = False
        if self.on_leader_change:
            self.on_leader_change(self.node_id)

    # ----------------------------------------------------------------- send
    def _send_election(self, ip: str, port: int, ok_event: threading.Event):
        msg = json.dumps({
            "type": "ELECTION",
            "node_id": self.node_id,
            "priority": self.priority,
        }).encode()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(ELECTION_TIMEOUT)
            sock.connect((ip, port))
            sock.sendall(len(msg).to_bytes(4, "big") + msg)
            # Wait for OK
            raw_len = sock.recv(4)
            if raw_len:
                length = int.from_bytes(raw_len, "big")
                data = self._recv_exact(sock, length)
                resp = json.loads(data)
                if resp.get("type") == "OK":
                    with self._lock:
                        self._received_ok = True
                    ok_event.set()
            sock.close()
        except Exception:
            pass   # peer unreachable — skip

    def _send_tcp_msg(self, ip: str, port: int, msg: bytes):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0)
            sock.connect((ip, port))
            sock.sendall(len(msg).to_bytes(4, "big") + msg)
            sock.close()
        except Exception:
            pass

    # ----------------------------------------------------------------- listen
    def _listen_loop(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(1.0)
        try:
            server.bind(("", self.election_port))
        except OSError as e:
            self.logger.log("SYSTEM", {"election_bind_error": str(e)}, level="ERROR")
            return
        server.listen(20)
        while self._running:
            try:
                conn, addr = server.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn,),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except Exception:
                continue
        server.close()

    def _handle_connection(self, conn: socket.socket):
        try:
            conn.settimeout(5.0)
            raw_len = conn.recv(4)
            if not raw_len:
                return
            length = int.from_bytes(raw_len, "big")
            data = self._recv_exact(conn, length)
            msg = json.loads(data)
            mtype = msg.get("type")
            sender_priority = msg.get("priority", 0)
            sender_id = msg.get("node_id")

            if mtype == "ELECTION":
                self.logger.log(
                    "ELECTION_ROUND",
                    {
                        "received_election_from": sender_id,
                        "their_priority": sender_priority,
                        "my_priority": self.priority,
                    },
                )
                if self.priority > sender_priority:
                    # Send OK back
                    ok_msg = json.dumps({"type": "OK", "node_id": self.node_id, "priority": self.priority}).encode()
                    conn.sendall(len(ok_msg).to_bytes(4, "big") + ok_msg)
                    conn.close()
                    # Start our own election
                    self.trigger_election(reason=f"higher_than_{sender_id}")
                else:
                    conn.close()

            elif mtype == "COORDINATOR":
                self.logger.log(
                    "COORDINATOR_ELECTED",
                    {
                        "leader": sender_id,
                        "priority": sender_priority,
                        "accepted_by": self.node_id,
                    },
                )
                self.current_leader = sender_id
                with self._lock:
                    self._election_in_progress = False
                conn.close()
                if self.on_leader_change:
                    self.on_leader_change(sender_id)
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def _recv_exact(sock: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                break
            buf += chunk
        return buf
