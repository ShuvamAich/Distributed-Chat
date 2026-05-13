"""
Distributed Chat Node — Entry Point
-------------------------------------
Usage:
  python node.py --port 9000 --ws-port 8765 --room-code DEMO123

Each device on the LAN runs one node instance.
Nodes discover each other via UDP broadcast, elect a leader (Bully),
and deliver messages in total order via the leader sequencer.

Port allocation per node:
  tcp_port        : 9000  — sequencer + app TCP
  tcp_port + 1    : 9001  — election TCP
  ws_port         : 8765  — browser WebSocket
  50000 (fixed)   : UDP broadcast discovery
  50001 (fixed)   : UDP heartbeat
"""

import argparse
import os
import signal
import sys
import threading
import time
import uuid

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from middleware.logger import NodeLogger
from middleware.discovery import DiscoveryManager, get_local_ip
from middleware.group_view import GroupView
from middleware.fault_detector import FaultDetector
from middleware.election import ElectionManager
from middleware.sequencer import Sequencer
from auth import AuthManager
from ws_bridge import WSBridge


def parse_args():
    p = argparse.ArgumentParser(description="Distributed Chat Node")
    p.add_argument("--port",      type=int, default=9000, help="TCP port for sequencer")
    p.add_argument("--ws-port",   type=int, default=8765, help="WebSocket port for browser UI")
    p.add_argument("--room-code", type=str, default=os.environ.get("ROOM_CODE", "DEMO123"),
                   help="Room access code (shared secret)")
    p.add_argument("--node-id",   type=str, default=None,
                   help="Override node UUID (auto-generated if not set)")
    p.add_argument("--log-dir",   type=str, default="logs")
    return p.parse_args()


class ChatNode:
    def __init__(self, node_id: str, tcp_port: int, ws_port: int, room_code: str, log_dir: str):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.ws_port = ws_port
        self.room_code = room_code
        self.local_ip = get_local_ip()

        # ---- Logger (must be first)
        self.logger = NodeLogger(node_id=node_id, log_dir=log_dir)
        self.logger.log("SYSTEM", {
            "event": "node_starting",
            "node_id": node_id,
            "ip": self.local_ip,
            "tcp_port": tcp_port,
            "ws_port": ws_port,
        })

        # ---- Auth
        self.auth = AuthManager(room_code=room_code, logger=self.logger)

        # ---- Group View
        self.group_view = GroupView(node_id=node_id, logger=self.logger)
        self.group_view.add_member(node_id, self.local_ip, tcp_port, username="(self)")

        # ---- Discovery
        self.discovery = DiscoveryManager(
            node_id=node_id,
            tcp_port=tcp_port,
            ws_port=ws_port,
            logger=self.logger,
            on_peer_discovered=self._on_peer_discovered,
            on_peer_left=self._on_peer_left,
        )

        # ---- Election
        self.election = ElectionManager(
            node_id=node_id,
            tcp_port=tcp_port,
            logger=self.logger,
            get_peers_fn=self._get_active_peers,
            on_leader_change=self._on_leader_change,
        )

        # ---- Sequencer
        self.sequencer = Sequencer(
            node_id=node_id,
            tcp_port=tcp_port,
            logger=self.logger,
            get_peers_fn=self._get_active_peers,
            get_leader_fn=lambda: self.election.current_leader,
            on_deliver=self._on_message_delivered,
        )

        # ---- Fault Detector
        self.fault_detector = FaultDetector(
            node_id=node_id,
            logger=self.logger,
            get_peers_fn=self._get_active_peers,
            on_fault=self._on_fault_detected,
        )

        # ---- WS Bridge
        self.ws_bridge = WSBridge(
            node_id=node_id,
            ws_port=ws_port,
            logger=self.logger,
            auth_manager=self.auth,
            sequencer=self.sequencer,
            group_view=self.group_view,
            get_leader_fn=lambda: self.election.current_leader,
            on_user_joined=self._on_user_joined,
            on_user_left=self._on_user_left,
        )

        # Subscribe group view changes to ws_bridge
        self.group_view.on_view_change(self.ws_bridge.push_view_update)

    # ----------------------------------------------------------------- start / stop
    def start(self):
        self.election.start()
        self.sequencer.start()
        self.ws_bridge.start()
        self.discovery.start()
        self.fault_detector.start()

        # Register self as first member, start initial election
        time.sleep(0.5)
        self.election.trigger_election(reason="node_startup")

        self.logger.log("SYSTEM", {
            "status": "RUNNING",
            "ui_url": f"http://{self.local_ip}:{self.ws_port}",
        })
        print(f"\n{'='*60}")
        print(f"  Chat Node Ready!")
        print(f"  Node ID  : {self.node_id}")
        print(f"  Local IP : {self.local_ip}")
        print(f"  TCP Port : {self.tcp_port}")
        print(f"  UI       : http://{self.local_ip}:{self.ws_port}")
        print(f"{'='*60}\n")

    def stop(self):
        self.logger.log("SYSTEM", {"status": "shutting_down"})
        self.fault_detector.stop()
        self.discovery.stop()
        self.election.stop()
        self.sequencer.stop()

    # ----------------------------------------------------------------- callbacks
    def _on_peer_discovered(self, peer_id: str, ip: str, tcp_port: int, ws_port: int):
        """Called by DiscoveryManager when a new peer is found on the LAN."""
        self.group_view.add_member(peer_id, ip, tcp_port, username="")
        self.fault_detector.reset_peer(peer_id)
        self.election.trigger_election(reason=f"peer_discovered:{peer_id[:8]}")
        username = self.auth.get_username(peer_id) or ""
        if username:
            self.ws_bridge.push_member_join(peer_id, username)

    def _on_peer_left(self, peer_id: str):
        """Called by DiscoveryManager when a peer sends BYE."""
        self._remove_peer(peer_id, reason="bye")

    def _on_fault_detected(self, peer_id: str):
        """Called by FaultDetector when a peer stops heartbeating."""
        self.logger.log("FAULT_DETECTED", {"node": peer_id, "action": "removing_and_triggering_election"})
        self._remove_peer(peer_id, reason="fault")

    def _remove_peer(self, peer_id: str, reason: str):
        username = self.auth.get_username(peer_id) or peer_id[:8]
        self.auth.deregister(peer_id)
        self.group_view.remove_member(peer_id)
        self.fault_detector.remove_peer(peer_id)
        self.ws_bridge.push_member_leave(peer_id, username)
        # Re-elect if removed peer was the leader
        if self.election.current_leader == peer_id:
            self.logger.log("ELECTION_START", {"reason": f"leader_{reason}", "lost_leader": peer_id})
            self.election.current_leader = None
            self.election.trigger_election(reason=f"leader_{reason}")
        else:
            self.election.trigger_election(reason=f"member_{reason}:{peer_id[:8]}")

    def _on_leader_change(self, leader_id: str):
        """Called by ElectionManager when a new leader is elected."""
        self.logger.log("COORDINATOR_ELECTED", {"leader": leader_id, "is_self": leader_id == self.node_id})
        self.ws_bridge.push_leader_change(leader_id)
        # Leader resets sequencer counter to avoid seq conflicts after re-election
        if leader_id == self.node_id:
            self.sequencer.reset_sequence()
        view = self.group_view.get_view()
        self.ws_bridge.push_view_update(view["view_id"], view)

    def _on_message_delivered(self, payload: dict, seq: int):
        """Called by Sequencer when a message is ready for delivery (in order)."""
        kind = payload.get("kind")
        if kind == "chat":
            self.ws_bridge.push_chat_message(payload, seq)
        elif kind == "system_join":
            nid = payload.get("node_id")
            uname = payload.get("username", "")
            if nid and nid != self.node_id:
                ok, _ = self.auth.register_user(nid, uname)
                if ok:
                    self.group_view.update_username(nid, uname)
                    self.ws_bridge.push_member_join(nid, uname)
        elif kind == "system_leave":
            nid = payload.get("node_id")
            uname = payload.get("username", "")
            if nid and nid != self.node_id:
                self.ws_bridge.push_member_leave(nid, uname)

    def _on_user_joined(self, node_id: str, username: str, ip: str):
        """Called by WSBridge when a browser client authenticates."""
        self.group_view.update_username(node_id, username)
        self.logger.log("NODE_JOIN", {"node_id": node_id, "username": username, "ip": ip})
        self.ws_bridge.push_member_join(node_id, username)
        # Broadcast join via sequencer so all nodes know the username
        self.sequencer.submit_message({
            "kind": "system_join",
            "node_id": node_id,
            "username": username,
            "ts": time.time(),
        })
        self.election.trigger_election(reason=f"user_join:{username}")

    def _on_user_left(self, node_id: str):
        """Called by WSBridge when a browser disconnects."""
        username = self.auth.get_username(node_id) or node_id[:8]
        self.auth.deregister(node_id)
        self.group_view.remove_member(node_id)
        self.ws_bridge.push_member_leave(node_id, username)
        self.logger.log("NODE_LEAVE", {"node_id": node_id, "username": username})
        self.sequencer.submit_message({
            "kind": "system_leave",
            "node_id": node_id,
            "username": username,
            "ts": time.time(),
        })
        self.election.trigger_election(reason=f"user_leave:{username}")

    # ----------------------------------------------------------------- peer access
    def _get_active_peers(self) -> dict:
        """Combines discovered peers with current group view members."""
        disc_peers = self.discovery.get_peers()
        view = self.group_view.get_view()
        result = {}
        for nid, info in view["members"].items():
            if nid == self.node_id:
                continue
            if nid in disc_peers:
                result[nid] = disc_peers[nid]
            elif info.get("ip") and info.get("tcp_port"):
                result[nid] = info
        return result


def main():
    args = parse_args()
    node_id = args.node_id or str(uuid.uuid4())

    node = ChatNode(
        node_id=node_id,
        tcp_port=args.port,
        ws_port=args.ws_port,
        room_code=args.room_code,
        log_dir=args.log_dir,
    )

    def _shutdown(sig, frame):
        print("\nShutting down...")
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    node.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
