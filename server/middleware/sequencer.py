"""
Total Ordering Sequencer
--------------------------
The elected leader acts as the global sequencer.

SEND path (any node → leader → all nodes):
  1. Any node sends MSG_SUBMIT to the leader via TCP.
  2. Leader assigns next global sequence number, stores in log.
  3. Leader broadcasts SEQ_MSG(seq, payload) to all members reliably.
  4. Members buffer in hold-back queue; deliver only in strict seq order.
     Gaps block delivery until filled (retransmit requested after HOLDBACK_TIMEOUT).

FIFO within causal: messages from the same sender also maintain FIFO order
because the leader processes submits sequentially per-sender.

Message types (JSON over TCP, port = tcp_port):
  MSG_SUBMIT  { "type": "MSG_SUBMIT",  "node_id": "...", "payload": {...} }
  SEQ_MSG     { "type": "SEQ_MSG",     "seq": N, "node_id": "...", "payload": {...}, "msg_id": "..." }
  ACK         { "type": "ACK",         "msg_id": "..." }
  RETRANSMIT_REQ { "type": "RETRANSMIT_REQ", "seq": N, "node_id": "..." }
"""

import json
import socket
import threading
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional

HOLDBACK_TIMEOUT = 3.0    # seconds before requesting retransmit of missing seq
BUFFER_SIZE = 65535


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(min(4096, n - len(buf)))
        if not chunk:
            raise ConnectionError("socket closed")
        buf += chunk
    return buf


def _send_framed(sock: socket.socket, obj: dict):
    raw = json.dumps(obj).encode()
    sock.sendall(len(raw).to_bytes(4, "big") + raw)


def _recv_framed(sock: socket.socket) -> dict:
    raw_len = _recv_exact(sock, 4)
    length = int.from_bytes(raw_len, "big")
    data = _recv_exact(sock, length)
    return json.loads(data)


class Sequencer:
    def __init__(
        self,
        node_id: str,
        tcp_port: int,
        logger,
        get_peers_fn: Callable[[], Dict[str, dict]],
        get_leader_fn: Callable[[], Optional[str]],
        on_deliver: Optional[Callable[[dict, int], None]] = None,
    ):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.logger = logger
        self.get_peers = get_peers_fn
        self.get_leader = get_leader_fn
        self.on_deliver = on_deliver

        self._running = False
        self._lock = threading.Lock()

        # Leader state
        self._global_seq: int = 0
        self._seq_log: Dict[int, dict] = {}    # seq -> full SEQ_MSG

        # Member state (hold-back queue)
        self._next_deliver: int = 1
        self._holdback: Dict[int, dict] = {}   # seq -> SEQ_MSG
        self._holdback_ts: Dict[int, float] = {}

        # Per-peer connections cache (leader → members)
        self._peer_conns: Dict[str, socket.socket] = {}
        self._peer_conn_lock = threading.Lock()

    # ----------------------------------------------------------------- control
    def start(self):
        self._running = True
        threading.Thread(target=self._listen_loop, daemon=True, name="seq-listen").start()
        threading.Thread(target=self._holdback_monitor, daemon=True, name="seq-holdback").start()
        self.logger.log("SYSTEM", {"sequencer": "started", "port": self.tcp_port})

    def stop(self):
        self._running = False

    # ----------------------------------------------------------------- submit (any node)
    def submit_message(self, payload: dict) -> bool:
        """Submit a chat message for total-ordering. Routes to leader."""
        leader = self.get_leader()
        if leader is None:
            self.logger.log("SYSTEM", {"warn": "no leader, cannot submit message"}, level="WARN")
            return False

        if leader == self.node_id:
            # We are the leader — sequence directly
            self._sequence_message(self.node_id, payload)
            return True

        # Send to leader via TCP
        peers = self.get_peers()
        leader_info = peers.get(leader)
        if not leader_info:
            self.logger.log("SYSTEM", {"warn": f"leader {leader} not in peers"}, level="WARN")
            return False

        msg = {"type": "MSG_SUBMIT", "node_id": self.node_id, "payload": payload}
        return self._send_to_peer(leader_info["ip"], leader_info["tcp_port"], msg)

    # ----------------------------------------------------------------- listen (all nodes)
    def _listen_loop(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(1.0)
        try:
            server.bind(("", self.tcp_port))
        except OSError as e:
            self.logger.log("SYSTEM", {"seq_bind_error": str(e)}, level="ERROR")
            return
        server.listen(50)
        while self._running:
            try:
                conn, addr = server.accept()
                threading.Thread(
                    target=self._handle_conn,
                    args=(conn, addr),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except Exception:
                continue
        server.close()

    def _handle_conn(self, conn: socket.socket, addr):
        try:
            conn.settimeout(10.0)
            while True:
                try:
                    msg = _recv_framed(conn)
                except (ConnectionError, OSError):
                    break
                mtype = msg.get("type")
                if mtype == "MSG_SUBMIT":
                    # Only leader handles this
                    if self.get_leader() == self.node_id:
                        self._sequence_message(msg["node_id"], msg["payload"])
                elif mtype == "SEQ_MSG":
                    self._receive_seq_msg(msg)
                    # Send ACK
                    try:
                        _send_framed(conn, {"type": "ACK", "msg_id": msg.get("msg_id")})
                    except Exception:
                        pass
                elif mtype == "RETRANSMIT_REQ":
                    seq = msg.get("seq")
                    self._handle_retransmit_req(seq, conn)
                elif mtype == "ACK":
                    pass  # handled by reliability layer
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ----------------------------------------------------------------- leader: sequence
    def _sequence_message(self, sender_id: str, payload: dict):
        with self._lock:
            self._global_seq += 1
            seq = self._global_seq
            import uuid as _uuid
            msg_id = str(_uuid.uuid4())
            seq_msg = {
                "type": "SEQ_MSG",
                "seq": seq,
                "msg_id": msg_id,
                "sender": sender_id,
                "payload": payload,
                "ts": time.time(),
            }
            self._seq_log[seq] = seq_msg

        self.logger.log(
            "MSG_SEQUENCED",
            {"seq": seq, "sender": sender_id, "msg_id": msg_id[:8]},
        )

        # Deliver locally first (leader is also a member)
        self._receive_seq_msg(seq_msg)

        # Broadcast to all peers
        peers = self.get_peers()
        for nid, info in peers.items():
            threading.Thread(
                target=self._deliver_to_peer,
                args=(nid, info, seq_msg),
                daemon=True,
            ).start()

    def _deliver_to_peer(self, nid: str, info: dict, seq_msg: dict):
        try:
            self._send_to_peer(info["ip"], info["tcp_port"], seq_msg)
        except Exception as e:
            self.logger.log("SYSTEM", {"peer_deliver_error": nid, "err": str(e)}, level="WARN")

    # ----------------------------------------------------------------- member: receive & hold-back
    def _receive_seq_msg(self, msg: dict):
        seq = msg.get("seq")
        if seq is None:
            return

        with self._lock:
            if seq < self._next_deliver:
                return  # already delivered
            if seq not in self._holdback:
                self._holdback[seq] = msg
                self._holdback_ts[seq] = time.time()

        self._try_deliver()

    def _try_deliver(self):
        while True:
            with self._lock:
                if self._next_deliver not in self._holdback:
                    break
                msg = self._holdback.pop(self._next_deliver)
                self._holdback_ts.pop(self._next_deliver, None)
                seq = self._next_deliver
                self._next_deliver += 1

            self.logger.log(
                "MSG_DELIVERED",
                {
                    "seq": seq,
                    "sender": msg.get("sender"),
                    "msg_id": msg.get("msg_id", "")[:8],
                },
            )
            if self.on_deliver:
                try:
                    self.on_deliver(msg.get("payload", {}), seq)
                except Exception:
                    pass

    # ----------------------------------------------------------------- hold-back monitor
    def _holdback_monitor(self):
        while self._running:
            time.sleep(1.0)
            now = time.time()
            with self._lock:
                gaps = [
                    seq for seq, ts in self._holdback_ts.items()
                    if now - ts > HOLDBACK_TIMEOUT and seq < self._next_deliver + 50
                ]
                missing = []
                expected = self._next_deliver
                if self._holdback:
                    max_seq = max(self._holdback.keys())
                    for s in range(expected, max_seq):
                        if s not in self._holdback:
                            missing.append(s)

            for seq in missing:
                self.logger.log("RETRANSMIT", {"requesting_seq": seq})
                self._request_retransmit(seq)

    def _request_retransmit(self, seq: int):
        leader = self.get_leader()
        if not leader or leader == self.node_id:
            return
        peers = self.get_peers()
        leader_info = peers.get(leader)
        if not leader_info:
            return
        self._send_to_peer(
            leader_info["ip"],
            leader_info["tcp_port"],
            {"type": "RETRANSMIT_REQ", "seq": seq, "node_id": self.node_id},
        )

    def _handle_retransmit_req(self, seq: int, conn: socket.socket):
        with self._lock:
            msg = self._seq_log.get(seq)
        if msg:
            try:
                _send_framed(conn, msg)
            except Exception:
                pass
        self.logger.log("RETRANSMIT", {"resent_seq": seq})

    # ----------------------------------------------------------------- helpers
    def _send_to_peer(self, ip: str, port: int, msg: dict) -> bool:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((ip, port))
            _send_framed(sock, msg)
            sock.close()
            return True
        except Exception as e:
            return False

    def reset_sequence(self):
        """Call after leader change to reset sequence state."""
        with self._lock:
            self._global_seq = 0
            self._seq_log.clear()
            self._next_deliver = 1
            self._holdback.clear()
            self._holdback_ts.clear()
