from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import suppress
import json
import socket
import struct
import time
from typing import Any, Callable

from .config import ChatConfig
from .logging_utils import EventLogger
from .messages import decode_packet, encode_packet, new_message_id, utc_now
from .order import LamportClock, VectorClock
from .state import KnownNode, MessageRecord, PeerConnection


class DiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "ChatNode") -> None:
        self.node = node

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.node.discovery_transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return
        self.node.on_discovery_datagram(message, addr)


class ChatNode:
    def __init__(self, config: ChatConfig, event_sink: Callable[[dict[str, Any]], None] | None = None) -> None:
        self.config = config
        self._sink = event_sink or self._default_sink
        self.log = EventLogger(self._sink)
        self.lamport = LamportClock()
        self.vector_clock = VectorClock()
        self.server: asyncio.AbstractServer | None = None
        self.discovery_transport: asyncio.DatagramTransport | None = None
        self.running = False
        self.is_leader = False
        self.leader_uid: str | None = None
        self.leader_host: str | None = None
        self.leader_port: int | None = None
        self.election_in_progress = False
        self.election_ok_received = asyncio.Event()
        self.coordinator_received = asyncio.Event()
        self.tasks: set[asyncio.Task[Any]] = set()
        self.reader_tasks: dict[str, asyncio.Task[Any]] = {}
        self.peers: dict[str, PeerConnection] = {}
        self.known_nodes: dict[str, KnownNode] = {}
        self.connecting: set[str] = set()
        self.pending_history: dict[int, MessageRecord] = {}
        self.pending_ack_targets: dict[int, set[str]] = defaultdict(set)
        self.delivery_buffer: dict[int, dict[str, Any]] = {}
        self.last_sync_request_seq = 0
        self.last_delivered_seq = 0
        self.next_sequence = 1
        self.view_version = 0
        self._state_lock = asyncio.Lock()
        self._pending_endpoints: set[tuple[str, int]] = set()

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.server = await asyncio.start_server(self._handle_incoming_connection, host="0.0.0.0", port=self.config.tcp_port)
        await self._start_discovery()
        self.tasks = {
            asyncio.create_task(self._discovery_announce_loop(), name="discovery_announce"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self._monitor_loop(), name="monitor"),
            asyncio.create_task(self._bootstrap_election_loop(), name="bootstrap_election"),
        }
        self._emit_status("STARTED", f"Node online at {self.config.host}:{self.config.tcp_port}")
        self.log.info("startup", "Distributed chat node started", uid=self.config.node_uid, priority=self.config.priority)

    async def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        await self._broadcast_best_effort({"type": "goodbye", "uid": self.config.node_uid, "username": self.config.username})
        for task in list(self.tasks):
            task.cancel()
        for task in list(self.reader_tasks.values()):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await asyncio.gather(*self.reader_tasks.values(), return_exceptions=True)
        self.tasks.clear()
        self.reader_tasks.clear()
        if self.discovery_transport:
            self.discovery_transport.close()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for peer in list(self.peers.values()):
            peer.writer.close()
            with suppress(Exception):
                await peer.writer.wait_closed()
        self.peers.clear()
        self._emit_status("STOPPED", "Node stopped")

    async def send_chat(self, text: str) -> None:
        text = text.strip()
        if not text:
            return
        self.lamport.tick()
        self.vector_clock.advance(self.config.node_uid)
        packet = {
            "type": "chat_submit",
            "client_message_id": new_message_id(),
            "sender_uid": self.config.node_uid,
            "username": self.config.username,
            "text": text,
            "created_at": utc_now(),
            "lamport": self.lamport.value,
            "vector_clock": dict(self.vector_clock),
            "ordering_mode": self.config.ordering_mode,
        }
        if self.is_leader:
            await self._accept_chat(packet)
            return
        leader = self._get_leader_peer()
        if not leader:
            self.log.warning("routing", "Leader unavailable, starting election before send")
            await self.initiate_election(reason="send_without_leader")
            if self.is_leader:
                await self._accept_chat(packet)
                return
            leader = self._get_leader_peer()
        if not leader:
            self.log.error("routing", "Unable to send message because no leader is available")
            self._sink({"kind": "event", "timestamp": utc_now(), "level": "ERROR", "category": "chat", "message": "Message not sent: no leader available"})
            return
        await self._send_packet(leader.writer, packet)
        self.log.info("chat", "Submitted message to leader", leader_uid=leader.uid, message_id=packet["client_message_id"])

    async def initiate_election(self, reason: str) -> None:
        async with self._state_lock:
            if self.election_in_progress or not self.running:
                return
            self.election_in_progress = True
            self.election_ok_received = asyncio.Event()
            self.coordinator_received = asyncio.Event()
        self.log.warning("election", f"Starting bully election ({reason})", priority=self.config.priority)
        higher_priority_nodes = [
            node for node in self.known_nodes.values() if node.priority > self.config.priority and self._node_is_recent(node)
        ]
        for node in sorted(higher_priority_nodes, key=lambda item: item.priority, reverse=True):
            await self._ensure_peer_connection(node)
            peer = self.peers.get(node.uid)
            if peer:
                await self._send_packet(
                    peer.writer,
                    {
                        "type": "election",
                        "uid": self.config.node_uid,
                        "priority": self.config.priority,
                        "username": self.config.username,
                        "reason": reason,
                    },
                )
        if not higher_priority_nodes:
            await self._become_leader("no_higher_priority_nodes")
            return
        try:
            await asyncio.wait_for(self.election_ok_received.wait(), timeout=2.5)
        except asyncio.TimeoutError:
            await self._become_leader("no_ok_response")
            return
        try:
            await asyncio.wait_for(self.coordinator_received.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            await self._become_leader("coordinator_timeout")
            return
        finally:
            self.election_in_progress = False

    def on_discovery_datagram(self, message: dict[str, Any], addr: tuple[str, int]) -> None:
        if message.get("type") != "discovery_hello":
            return
        uid = message.get("uid")
        if not uid or uid == self.config.node_uid:
            return
        host = str(message.get("host") or addr[0])
        port = int(message.get("port") or 0)
        if not port:
            return
        self.known_nodes[uid] = KnownNode(
            uid=uid,
            username=str(message.get("username", "peer")),
            host=host,
            port=port,
            priority=int(message.get("priority", 0)),
            last_discovered=time.monotonic(),
            leader_uid=message.get("leader_uid"),
        )
        if not self.leader_uid and message.get("leader_uid"):
            self.leader_uid = message.get("leader_uid")
            self.leader_host = message.get("leader_host")
            self.leader_port = int(message.get("leader_port") or 0) or None
        if uid not in self.peers and uid not in self.connecting and self.running:
            task = asyncio.create_task(self._ensure_peer_connection(self.known_nodes[uid]))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    async def _start_discovery(self) -> None:
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        with suppress(AttributeError, OSError):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        with suppress(OSError):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        with suppress(OSError):
            sock.bind(("", self.config.multicast_port))
        with suppress(OSError):
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        with suppress(OSError):
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.config.host))
        joined = False
        for interface_ip in (self.config.host, "0.0.0.0"):
            try:
                membership = struct.pack("4s4s", socket.inet_aton(self.config.multicast_group), socket.inet_aton(interface_ip))
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
                joined = True
                self.log.info("discovery", "Joined discovery group", group=self.config.multicast_group, interface=interface_ip)
                break
            except OSError as exc:
                self.log.warning("discovery", f"Unable to join multicast group on {interface_ip}: {exc}")
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        transport, _ = await loop.create_datagram_endpoint(lambda: DiscoveryProtocol(self), sock=sock)
        self.discovery_transport = transport  # type: ignore[assignment]
        self.log.info(
            "discovery",
            "Discovery transport ready",
            group=self.config.multicast_group,
            port=self.config.multicast_port,
            host=self.config.host,
            broadcast_fallback=True,
            joined=joined,
        )

    async def _discovery_announce_loop(self) -> None:
        while self.running:
            packet = {
                "type": "discovery_hello",
                "uid": self.config.node_uid,
                "username": self.config.username,
                "room_name": self.config.room_name,
                "host": self.config.host,
                "port": self.config.tcp_port,
                "priority": self.config.priority,
                "leader_uid": self.leader_uid,
                "leader_host": self.leader_host,
                "leader_port": self.leader_port,
                "view_version": self.view_version,
                "timestamp": utc_now(),
            }
            if self.discovery_transport:
                payload = json.dumps(packet).encode("utf-8")
                self.discovery_transport.sendto(payload, (self.config.multicast_group, self.config.multicast_port))
                with suppress(OSError):
                    self.discovery_transport.sendto(payload, ("255.255.255.255", self.config.multicast_port))
            await asyncio.sleep(self.config.discovery_interval)

    async def _bootstrap_election_loop(self) -> None:
        await asyncio.sleep(3.0)
        if not self.leader_uid:
            await self.initiate_election(reason="startup_discovery_complete")

    async def _monitor_loop(self) -> None:
        while self.running:
            now = time.monotonic()
            stale = [uid for uid, peer in self.peers.items() if now - peer.last_seen > self.config.failure_timeout]
            for uid in stale:
                await self._drop_peer(uid, f"timeout>{self.config.failure_timeout}s")
            if not self.is_leader:
                leader = self._get_leader_peer()
                if self.leader_uid and (not leader or now - leader.last_seen > self.config.failure_timeout):
                    self.log.warning("fault_tolerance", "Leader heartbeat missed, triggering election", leader_uid=self.leader_uid)
                    await self.initiate_election(reason="leader_timeout")
            await asyncio.sleep(1.0)

    async def _heartbeat_loop(self) -> None:
        while self.running:
            for uid, peer in list(self.peers.items()):
                await self._send_packet(
                    peer.writer,
                    {
                        "type": "heartbeat",
                        "uid": self.config.node_uid,
                        "leader_uid": self.leader_uid,
                        "last_delivered_seq": self.last_delivered_seq,
                        "timestamp": utc_now(),
                    },
                )
            await asyncio.sleep(self.config.heartbeat_interval)

    async def _handle_incoming_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_uid: str | None = None
        try:
            request = await self._read_packet(reader, timeout=10.0)
            if request.get("type") != "join_request":
                raise ConnectionError("join_request expected")
            peer_uid = str(request["uid"])
            await self._register_peer(
                uid=peer_uid,
                username=str(request.get("username", "peer")),
                host=str(request.get("host") or writer.get_extra_info("peername")[0]),
                port=int(request.get("port", 0)),
                priority=int(request.get("priority", 0)),
                writer=writer,
            )
            await self._send_packet(
                writer,
                {
                    "type": "join_accept",
                    "uid": self.config.node_uid,
                    "username": self.config.username,
                    "host": self.config.host,
                    "port": self.config.tcp_port,
                    "priority": self.config.priority,
                    "room_name": self.config.room_name,
                    "leader_uid": self.leader_uid,
                    "leader_host": self.leader_host,
                    "leader_port": self.leader_port,
                    "view_version": self.view_version,
                    "next_sequence": self.next_sequence,
                    "last_delivered_seq": self.last_delivered_seq,
                },
            )
            self.log.info("membership", "Accepted incoming peer", peer_uid=peer_uid, room_name=request.get("room_name"))
            await self._send_view_update()
            await self._push_history_if_leader(writer, int(request.get("last_delivered_seq", 0)))
            await self._peer_reader_loop(peer_uid, reader, writer)
        except Exception as exc:
            self.log.warning("network", f"Incoming connection closed during setup: {exc}")
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()
            if peer_uid:
                await self._drop_peer(peer_uid, "setup_failed")

    async def _ensure_peer_connection(self, node: KnownNode) -> None:
        if node.uid == self.config.node_uid or node.uid in self.peers or node.uid in self.connecting:
            return
        self.connecting.add(node.uid)
        try:
            await self._connect_to_endpoint(node.host, node.port, source="discovery", expected_uid=node.uid, expected_username=node.username, expected_priority=node.priority)
        except Exception as exc:
            self.log.warning("network", f"Unable to connect to {node.username}@{node.host}:{node.port}: {exc}")
        finally:
            self.connecting.discard(node.uid)

    async def _connect_to_endpoint(
        self,
        host: str,
        port: int,
        source: str,
        expected_uid: str | None = None,
        expected_username: str = "peer",
        expected_priority: int = 0,
    ) -> None:
        endpoint = (host, port)
        if endpoint in self._pending_endpoints:
            return
        if any(peer.host == host and peer.port == port for peer in self.peers.values()):
            return
        self._pending_endpoints.add(endpoint)
        try:
            reader, writer = await asyncio.open_connection(host, port)
            await self._send_packet(
                writer,
                {
                    "type": "join_request",
                    "uid": self.config.node_uid,
                    "username": self.config.username,
                    "room_name": self.config.room_name,
                    "host": self.config.host,
                    "port": self.config.tcp_port,
                    "priority": self.config.priority,
                    "last_delivered_seq": self.last_delivered_seq,
                },
            )
            accept = await self._read_packet(reader, timeout=10.0)
            if accept.get("type") != "join_accept":
                raise ConnectionError("join_accept expected")
            peer_uid = str(accept["uid"])
            if peer_uid == self.config.node_uid:
                raise ConnectionError("refusing self-connection")
            await self._register_peer(
                uid=peer_uid,
                username=str(accept.get("username", expected_username)),
                host=str(accept.get("host", host)),
                port=int(accept.get("port", port)),
                priority=int(accept.get("priority", expected_priority)),
                writer=writer,
            )
            if expected_uid and expected_uid != peer_uid:
                self.log.warning("membership", "Endpoint identity changed since discovery", expected_uid=expected_uid, actual_uid=peer_uid, host=host, port=port)
            if accept.get("leader_uid"):
                self.leader_uid = str(accept.get("leader_uid"))
                self.leader_host = str(accept.get("leader_host") or self.leader_host or host)
                leader_port = accept.get("leader_port")
                self.leader_port = int(leader_port) if leader_port else self.leader_port
                leader_name = str(accept.get("username")) if self.leader_uid == peer_uid else "unknown"
                self._emit_status(
                    "FOLLOWER",
                    f"Leader is {leader_name} @ {self.leader_host}:{self.leader_port}",
                )
            self.log.info("membership", "Connected to peer", peer_uid=peer_uid, host=host, port=port, source=source, room_name=accept.get("room_name"))
            if self.last_delivered_seq + 1 < int(accept.get("next_sequence", self.next_sequence)):
                await self._request_sync(self.last_delivered_seq + 1)
            await self._push_history_if_leader(writer, int(accept.get("last_delivered_seq", 0)))
            task = asyncio.create_task(self._peer_reader_loop(peer_uid, reader, writer))
            self.reader_tasks[peer_uid] = task
            task.add_done_callback(lambda _: self.reader_tasks.pop(peer_uid, None))
            await self._send_view_update()
        finally:
            self._pending_endpoints.discard(endpoint)

    async def _register_peer(self, uid: str, username: str, host: str, port: int, priority: int, writer: asyncio.StreamWriter) -> None:
        existing = self.peers.get(uid)
        if existing and existing.writer is not writer:
            existing.writer.close()
            with suppress(Exception):
                await existing.writer.wait_closed()
        now = time.monotonic()
        peer = PeerConnection(
            uid=uid,
            username=username,
            host=host,
            port=port,
            priority=priority,
            writer=writer,
            connected_at=now,
            last_seen=now,
            is_leader=(uid == self.leader_uid),
            view_version=self.view_version,
        )
        self.peers[uid] = peer
        self.known_nodes[uid] = KnownNode(uid=uid, username=username, host=host, port=port, priority=priority, last_discovered=now, leader_uid=self.leader_uid)
        self._emit_view()

    async def _peer_reader_loop(self, peer_uid: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while self.running:
                packet = await self._read_packet(reader)
                self._mark_peer_seen(peer_uid)
                await self._handle_packet(peer_uid, packet)
        except (asyncio.IncompleteReadError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass
        except Exception as exc:
            self.log.warning("network", f"Reader loop failed for {peer_uid}: {exc}")
        finally:
            await self._drop_peer(peer_uid, "connection_closed", writer)

    async def _handle_packet(self, peer_uid: str, packet: dict[str, Any]) -> None:
        packet_type = packet.get("type")
        if packet_type == "heartbeat":
            self._mark_peer_seen(peer_uid)
            peer = self.peers.get(peer_uid)
            if peer:
                peer.delivered_seq = int(packet.get("last_delivered_seq", peer.delivered_seq))
                await self._send_packet(peer.writer, {"type": "heartbeat_ack", "uid": self.config.node_uid, "timestamp": utc_now()})
            return
        if packet_type == "heartbeat_ack":
            self._mark_peer_seen(peer_uid)
            return
        if packet_type == "chat_submit":
            if self.is_leader:
                await self._accept_chat(packet)
            return
        if packet_type == "ordered_chat":
            await self._handle_ordered_chat(packet)
            return
        if packet_type == "delivery_ack":
            seq = int(packet.get("seq", 0))
            if seq in self.pending_ack_targets:
                self.pending_ack_targets[seq].discard(peer_uid)
                record = self.pending_history.get(seq)
                if record:
                    record.acknowledgements.add(peer_uid)
                if not self.pending_ack_targets[seq]:
                    self.pending_ack_targets.pop(seq, None)
                    self.log.info("ordering", "All acknowledgements received", seq=seq)
            return
        if packet_type == "sync_request":
            await self._handle_sync_request(peer_uid, packet)
            return
        if packet_type == "sync_response":
            for message in packet.get("messages", []):
                await self._handle_ordered_chat(message, from_sync=True)
            return
        if packet_type == "election":
            await self._handle_election_message(peer_uid, packet)
            return
        if packet_type == "ok":
            self.election_ok_received.set()
            self.log.info("election", "Received OK from higher priority node", peer_uid=peer_uid)
            return
        if packet_type == "coordinator":
            await self._handle_coordinator(packet)
            return
        if packet_type == "view_update":
            self.view_version = max(self.view_version, int(packet.get("view_version", self.view_version)))
            members = packet.get("members", [])
            leader_uid = packet.get("leader_uid")
            if leader_uid:
                leader_member = next((member for member in members if member.get("uid") == leader_uid), None)
                if leader_member:
                    self.leader_uid = str(leader_uid)
                    self.leader_host = str(leader_member.get("host"))
                    self.leader_port = int(leader_member.get("port"))
                    state = "LEADER" if self.leader_uid == self.config.node_uid else "FOLLOWER"
                    self._emit_status(state, f"Leader is {leader_member.get('username')} @ {self.leader_host}:{self.leader_port}")
            self._emit_view(members)
            self._sink({
                "kind": "event",
                "timestamp": utc_now(),
                "level": "INFO",
                "category": "group_view",
                "message": f"View v{self.view_version} received",
                "members": members,
            })
            return
        if packet_type == "goodbye":
            await self._drop_peer(peer_uid, "goodbye")
            return

    async def _accept_chat(self, packet: dict[str, Any]) -> None:
        self.lamport.tick(int(packet.get("lamport", 0)))
        self.vector_clock.merge(packet.get("vector_clock"))
        self.vector_clock.advance(self.config.node_uid)
        seq = self.next_sequence
        self.next_sequence += 1
        record = MessageRecord(
            seq=seq,
            sender_uid=str(packet.get("sender_uid", self.config.node_uid)),
            username=str(packet.get("username", self.config.username)),
            text=str(packet.get("text", "")),
            created_at=str(packet.get("created_at", utc_now())),
            delivered_at=utc_now(),
            lamport=self.lamport.value,
            vector_clock=dict(self.vector_clock),
            client_message_id=str(packet.get("client_message_id", new_message_id())),
            view_version=self.view_version,
        )
        self.pending_history[seq] = record
        await self._handle_ordered_chat(record.as_packet(), from_sync=False, local_origin=True)
        peers = [peer for uid, peer in self.peers.items() if uid != self.config.node_uid]
        if peers:
            self.pending_ack_targets[seq] = {peer.uid for peer in peers}
            for peer in peers:
                await self._send_packet(peer.writer, record.as_packet())
            task = asyncio.create_task(self._retransmit_until_acked(seq))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
        self.log.info(
            "ordering",
            "Total order message committed",
            seq=seq,
            sender=record.username,
            ordering=self.config.ordering_mode,
        )

    async def _handle_ordered_chat(self, packet: dict[str, Any], from_sync: bool = False, local_origin: bool = False) -> None:
        seq = int(packet.get("seq", 0))
        if not seq:
            return
        if seq <= self.last_delivered_seq:
            if not self.is_leader:
                await self._acknowledge_delivery(seq)
            return
        self.delivery_buffer[seq] = packet
        if seq > self.last_delivered_seq + 1 and self.last_sync_request_seq != self.last_delivered_seq + 1:
            self.last_sync_request_seq = self.last_delivered_seq + 1
            await self._request_sync(self.last_delivered_seq + 1)
        while self.last_delivered_seq + 1 in self.delivery_buffer:
            next_seq = self.last_delivered_seq + 1
            current = self.delivery_buffer.pop(next_seq)
            observed_lamport = int(current.get("lamport", 0))
            self.lamport.tick(observed_lamport)
            self.vector_clock.merge(current.get("vector_clock"))
            self.last_delivered_seq = next_seq
            self._sink(
                {
                    "kind": "chat",
                    "timestamp": utc_now(),
                    "seq": next_seq,
                    "sender_uid": current.get("sender_uid"),
                    "username": current.get("username"),
                    "text": current.get("text"),
                    "created_at": current.get("created_at"),
                    "delivered_at": current.get("delivered_at"),
                    "ordering": self.config.ordering_mode,
                    "lamport": current.get("lamport"),
                    "vector_clock": current.get("vector_clock"),
                    "from_sync": from_sync,
                    "local_origin": local_origin,
                }
            )
            self.log.info("chat", "Delivered ordered message", seq=next_seq, sender=current.get("username"), from_sync=from_sync)
            if not self.is_leader:
                await self._acknowledge_delivery(next_seq)

    async def _acknowledge_delivery(self, seq: int) -> None:
        leader = self._get_leader_peer()
        if not leader:
            return
        await self._send_packet(leader.writer, {"type": "delivery_ack", "uid": self.config.node_uid, "seq": seq, "timestamp": utc_now()})

    async def _request_sync(self, from_seq: int) -> None:
        leader = self._get_leader_peer()
        if not leader:
            return
        await self._send_packet(leader.writer, {"type": "sync_request", "uid": self.config.node_uid, "from_seq": from_seq})
        self.log.warning("fault_tolerance", "Gap detected, requesting retransmission", from_seq=from_seq)

    async def _handle_sync_request(self, peer_uid: str, packet: dict[str, Any]) -> None:
        if not self.is_leader:
            return
        from_seq = int(packet.get("from_seq", 1))
        messages = [record.as_packet() for seq, record in sorted(self.pending_history.items()) if seq >= from_seq]
        peer = self.peers.get(peer_uid)
        if peer:
            await self._send_packet(peer.writer, {"type": "sync_response", "messages": messages, "timestamp": utc_now()})
            self.log.info("fault_tolerance", "Sent missing messages to recovering peer", peer_uid=peer_uid, from_seq=from_seq, count=len(messages))

    async def _push_history_if_leader(self, writer: asyncio.StreamWriter, peer_last_delivered_seq: int) -> None:
        if not self.is_leader:
            return
        messages = [record.as_packet() for seq, record in sorted(self.pending_history.items()) if seq > peer_last_delivered_seq]
        if not messages:
            return
        with suppress(Exception):
            await self._send_packet(writer, {"type": "sync_response", "messages": messages, "timestamp": utc_now()})
            self.log.info("fault_tolerance", "Pushed missing history to new peer", from_seq=peer_last_delivered_seq + 1, count=len(messages))

    async def _retransmit_until_acked(self, seq: int) -> None:
        retries = 0
        while self.running and seq in self.pending_ack_targets and self.pending_ack_targets[seq] and retries < self.config.retransmit_limit:
            await asyncio.sleep(self.config.ack_timeout)
            remaining = set(self.pending_ack_targets.get(seq, set()))
            if not remaining:
                return
            record = self.pending_history.get(seq)
            if not record:
                return
            for uid in list(remaining):
                peer = self.peers.get(uid)
                if peer:
                    await self._send_packet(peer.writer, record.as_packet())
            retries += 1
            record.retries = retries
            self.log.warning("fault_tolerance", "Retransmitting message awaiting ACK", seq=seq, retries=retries, remaining=list(remaining))
        if seq in self.pending_ack_targets and self.pending_ack_targets[seq]:
            self.log.warning("fault_tolerance", "Will rely on sync for undelivered peers after retransmit budget", seq=seq, remaining=list(self.pending_ack_targets[seq]))

    async def _handle_election_message(self, peer_uid: str, packet: dict[str, Any]) -> None:
        peer = self.peers.get(peer_uid)
        if peer and self.config.priority > int(packet.get("priority", 0)):
            await self._send_packet(peer.writer, {"type": "ok", "uid": self.config.node_uid, "priority": self.config.priority})
            self.log.info("election", "Replied OK to lower priority node", peer_uid=peer_uid)
            if not self.election_in_progress:
                await self.initiate_election(reason="received_election")

    async def _handle_coordinator(self, packet: dict[str, Any]) -> None:
        self.is_leader = packet.get("uid") == self.config.node_uid
        self.leader_uid = str(packet.get("uid"))
        self.leader_host = str(packet.get("host"))
        self.leader_port = int(packet.get("port"))
        self.coordinator_received.set()
        self.election_in_progress = False
        self.log.info("election", "Coordinator announced", leader_uid=self.leader_uid, leader_host=self.leader_host, leader_port=self.leader_port)
        self._emit_status("LEADER" if self.is_leader else "FOLLOWER", f"Leader is {packet.get('username')} @ {self.leader_host}:{self.leader_port}")
        await self._send_view_update()

    async def _become_leader(self, reason: str) -> None:
        self.is_leader = True
        self.leader_uid = self.config.node_uid
        self.leader_host = self.config.host
        self.leader_port = self.config.tcp_port
        self.election_in_progress = False
        self.coordinator_received.set()
        self.view_version += 1
        self.log.warning("election", f"This node became coordinator ({reason})", view_version=self.view_version)
        packet = {
            "type": "coordinator",
            "uid": self.config.node_uid,
            "username": self.config.username,
            "host": self.config.host,
            "port": self.config.tcp_port,
            "priority": self.config.priority,
            "view_version": self.view_version,
            "timestamp": utc_now(),
        }
        await self._broadcast_best_effort(packet)
        self._emit_status("LEADER", "This node is the leader")
        await self._send_view_update()

    async def _send_view_update(self) -> None:
        if not self.is_leader:
            return
        self.view_version += 1
        members = self._compose_membership_view()
        packet = {
            "type": "view_update",
            "view_version": self.view_version,
            "leader_uid": self.leader_uid,
            "members": members,
            "timestamp": utc_now(),
        }
        await self._broadcast_best_effort(packet)
        self._emit_view(members)

    async def _broadcast_best_effort(self, packet: dict[str, Any]) -> None:
        for peer in list(self.peers.values()):
            await self._send_packet(peer.writer, packet)

    async def _drop_peer(self, uid: str, reason: str, writer: asyncio.StreamWriter | None = None) -> None:
        peer = self.peers.get(uid)
        if not peer:
            if writer is not None:
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()
            return
        if writer is not None and peer.writer is not writer:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()
            self.log.info("membership", "Ignored stale connection close", peer_uid=uid, reason=reason)
            return
        self.peers.pop(uid, None)
        peer.writer.close()
        with suppress(Exception):
            await peer.writer.wait_closed()
        self.log.warning("membership", "Peer removed", peer_uid=uid, reason=reason)
        leader_dropped = uid == self.leader_uid
        if uid == self.leader_uid:
            self.leader_uid = None
            self.leader_host = None
            self.leader_port = None
            if not self.is_leader and self.running:
                self._emit_status("FOLLOWER", "Leader lost, starting election")
                self.log.warning("election", "Leader disconnected, starting election", peer_uid=uid, reason=reason)
                task = asyncio.create_task(self.initiate_election(reason="leader_disconnected"))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
        if self.is_leader:
            await self._send_view_update()
        self._emit_view()

    def _compose_membership_view(self) -> list[dict[str, Any]]:
        members = [
            {
                "uid": self.config.node_uid,
                "username": self.config.username,
                "host": self.config.host,
                "port": self.config.tcp_port,
                "priority": self.config.priority,
                "role": "leader" if self.is_leader else "member",
                "last_delivered_seq": self.last_delivered_seq,
            }
        ]
        for peer in sorted(self.peers.values(), key=lambda item: (-item.priority, item.username)):
            members.append(
                {
                    "uid": peer.uid,
                    "username": peer.username,
                    "host": peer.host,
                    "port": peer.port,
                    "priority": peer.priority,
                    "role": "leader" if peer.uid == self.leader_uid else "member",
                    "last_delivered_seq": peer.delivered_seq,
                }
            )
        return members

    def _emit_view(self, members: list[dict[str, Any]] | None = None) -> None:
        self._sink(
            {
                "kind": "view",
                "timestamp": utc_now(),
                "view_version": self.view_version,
                "leader_uid": self.leader_uid,
                "members": members or self._compose_membership_view(),
            }
        )

    def _emit_status(self, state: str, detail: str) -> None:
        self._sink({"kind": "status", "timestamp": utc_now(), "state": state, "detail": detail})

    def _get_leader_peer(self) -> PeerConnection | None:
        if self.leader_uid == self.config.node_uid:
            return None
        if self.leader_uid:
            return self.peers.get(self.leader_uid)
        if self.leader_host and self.leader_port:
            for peer in self.peers.values():
                if peer.host == self.leader_host and peer.port == self.leader_port:
                    return peer
        return None

    def _mark_peer_seen(self, peer_uid: str) -> None:
        peer = self.peers.get(peer_uid)
        if peer:
            peer.last_seen = time.monotonic()

    def _node_is_recent(self, node: KnownNode) -> bool:
        return (time.monotonic() - node.last_discovered) <= self.config.failure_timeout

    async def _read_packet(self, reader: asyncio.StreamReader, timeout: float | None = None) -> dict[str, Any]:
        if timeout is None:
            raw = await reader.readline()
        else:
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not raw:
            raise ConnectionResetError("connection closed")
        return decode_packet(raw)

    async def _send_packet(self, writer: asyncio.StreamWriter, packet: dict[str, Any]) -> None:
        try:
            writer.write(encode_packet(packet))
            await writer.drain()
        except Exception:
            raise

    @staticmethod
    def _default_sink(payload: dict[str, Any]) -> None:
        print(payload)
