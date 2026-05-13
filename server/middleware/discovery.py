"""
UDP Broadcast Discovery Module
--------------------------------
Each node periodically broadcasts a HELLO datagram on the LAN.
Other nodes receive it and add the sender to their peer table.
Uses port 50000 for discovery traffic.

Message format (JSON over UDP):
  { "type": "HELLO", "node_id": "...", "ip": "...", "tcp_port": N, "ws_port": N }
  { "type": "BYE",   "node_id": "..." }
"""

import asyncio
import json
import socket
import threading
import time
from typing import Callable, Dict, Optional

DISCOVERY_PORT = 50000
BROADCAST_ADDR = "<broadcast>"
HELLO_INTERVAL = 3.0
BUFFER_SIZE = 4096


def get_local_ip() -> str:
    """Best-effort: get the LAN IP of this machine (not 127.0.0.1)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class DiscoveryManager:
    """
    Manages UDP broadcast discovery of peers on the LAN.

    on_peer_discovered(node_id, ip, tcp_port, ws_port) -> called when new peer found
    on_peer_left(node_id) -> called when explicit BYE received
    """

    def __init__(
        self,
        node_id: str,
        tcp_port: int,
        ws_port: int,
        logger,
        on_peer_discovered: Optional[Callable] = None,
        on_peer_left: Optional[Callable] = None,
    ):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.ws_port = ws_port
        self.logger = logger
        self.on_peer_discovered = on_peer_discovered
        self.on_peer_left = on_peer_left
        self.local_ip = get_local_ip()
        self._running = False
        self._known_peers: Dict[str, dict] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ send
    def _make_hello(self) -> bytes:
        msg = {
            "type": "HELLO",
            "node_id": self.node_id,
            "ip": self.local_ip,
            "tcp_port": self.tcp_port,
            "ws_port": self.ws_port,
        }
        return json.dumps(msg).encode()

    def _make_bye(self) -> bytes:
        return json.dumps({"type": "BYE", "node_id": self.node_id}).encode()

    def _broadcast_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            while self._running:
                try:
                    sock.sendto(self._make_hello(), (BROADCAST_ADDR, DISCOVERY_PORT))
                except Exception as e:
                    self.logger.log("DISCOVERY", {"error": str(e)}, level="WARN")
                time.sleep(HELLO_INTERVAL)
        finally:
            sock.close()

    # ----------------------------------------------------------------- receive
    def _listen_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(1.0)
        try:
            sock.bind(("", DISCOVERY_PORT))
        except OSError as e:
            self.logger.log("DISCOVERY", {"error": f"bind failed: {e}"}, level="ERROR")
            return

        while self._running:
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                self._handle_message(msg, addr[0])
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    self.logger.log("DISCOVERY", {"error": str(e)}, level="WARN")

        sock.close()

    def _handle_message(self, msg: dict, sender_ip: str):
        mtype = msg.get("type")
        nid = msg.get("node_id")
        if not nid or nid == self.node_id:
            return

        if mtype == "HELLO":
            ip = msg.get("ip", sender_ip)
            tcp_port = msg.get("tcp_port")
            ws_port = msg.get("ws_port")
            with self._lock:
                is_new = nid not in self._known_peers
                self._known_peers[nid] = {
                    "ip": ip,
                    "tcp_port": tcp_port,
                    "ws_port": ws_port,
                    "last_seen": time.time(),
                }
            if is_new:
                self.logger.log(
                    "DISCOVERY",
                    {"peer": nid, "ip": ip, "tcp_port": tcp_port},
                )
                if self.on_peer_discovered:
                    self.on_peer_discovered(nid, ip, tcp_port, ws_port)

        elif mtype == "BYE":
            with self._lock:
                self._known_peers.pop(nid, None)
            self.logger.log("DISCOVERY", {"peer_left": nid})
            if self.on_peer_left:
                self.on_peer_left(nid)

    # ----------------------------------------------------------------- control
    def start(self):
        self._running = True
        threading.Thread(target=self._broadcast_loop, daemon=True, name="discovery-broadcast").start()
        threading.Thread(target=self._listen_loop, daemon=True, name="discovery-listen").start()
        self.logger.log("DISCOVERY", {"status": "started", "local_ip": self.local_ip})

    def stop(self):
        self._running = False
        # send BYE
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.sendto(self._make_bye(), (BROADCAST_ADDR, DISCOVERY_PORT))
            sock.close()
        except Exception:
            pass

    def get_peers(self) -> Dict[str, dict]:
        with self._lock:
            return dict(self._known_peers)
