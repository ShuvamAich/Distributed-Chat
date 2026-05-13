"""
WebSocket Bridge
-----------------
Bridges browser clients (React UI) to the node middleware.
Runs an asyncio WebSocket server on ws_port.

Browser → Node messages:
  { "action": "auth",    "room_code": "...", "username": "..." }
  { "action": "message", "text": "..." }
  { "action": "ping" }

Node → Browser messages:
  { "event": "auth_ok",      "node_id": "...", "username": "...", "is_leader": bool }
  { "event": "auth_fail",    "reason": "..." }
  { "event": "chat_message", "seq": N, "sender": "...", "username": "...", "text": "...", "ts": "..." }
  { "event": "member_join",  "node_id": "...", "username": "..." }
  { "event": "member_leave", "node_id": "...", "username": "..." }
  { "event": "leader_change","leader_id": "...", "leader_username": "..." }
  { "event": "view_update",  "members": [...] }
  { "event": "system_log",   <logger record> }
  { "event": "error",        "reason": "..." }
"""

import asyncio
import json
import time
import threading
import websockets
from typing import Any, Callable, Dict, Optional, Set


class WSBridge:
    def __init__(
        self,
        node_id: str,
        ws_port: int,
        logger,
        auth_manager,
        sequencer,
        group_view,
        get_leader_fn: Callable[[], Optional[str]],
        on_user_joined: Optional[Callable[[str, str, str], None]] = None,
        on_user_left: Optional[Callable[[str], None]] = None,
    ):
        self.node_id = node_id
        self.ws_port = ws_port
        self.logger = logger
        self.auth = auth_manager
        self.sequencer = sequencer
        self.group_view = group_view
        self.get_leader = get_leader_fn
        self.on_user_joined = on_user_joined   # (node_id, username, ip)
        self.on_user_left = on_user_left       # (node_id)

        self._clients: Dict[websockets.WebSocketServerProtocol, dict] = {}
        self._lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._authenticated_node_id: Optional[str] = None   # This node local browser session

        # Subscribe to logger events so we can forward to browser
        self.logger.subscribe(self._on_log_event)

    # ----------------------------------------------------------------- start
    def start(self):
        """Run asyncio event loop in a dedicated thread."""
        self._loop = asyncio.new_event_loop()
        t = threading.Thread(target=self._run_loop, daemon=True, name="ws-bridge")
        t.start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve())

    async def _serve(self):
        self.logger.log("SYSTEM", {"ws_bridge": f"listening on port {self.ws_port}"})
        async with websockets.serve(
            self._handle_client,
            "0.0.0.0",
            self.ws_port,
            ping_interval=20,
            ping_timeout=10,
        ):
            await asyncio.Future()   # run forever

    # ----------------------------------------------------------------- handle client
    async def _handle_client(self, ws, path=None):
        remote = ws.remote_address
        self.logger.log("SYSTEM", {"ws_client_connected": str(remote)})
        async with self._lock:
            self._clients[ws] = {"authed": False, "username": None, "node_id": None}

        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    await self._send(ws, {"event": "error", "reason": "invalid JSON"})
                    continue

                action = msg.get("action")
                client_state = self._clients.get(ws, {})

                if action == "auth":
                    await self._handle_auth(ws, msg, remote)
                elif action == "message":
                    if not client_state.get("authed"):
                        await self._send(ws, {"event": "error", "reason": "not authenticated"})
                    else:
                        await self._handle_message(ws, msg)
                elif action == "ping":
                    await self._send(ws, {"event": "pong", "ts": time.time()})
                else:
                    await self._send(ws, {"event": "error", "reason": f"unknown action: {action}"})
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            self.logger.log("SYSTEM", {"ws_error": str(e)}, level="WARN")
        finally:
            async with self._lock:
                state = self._clients.pop(ws, {})
            uid = state.get("username")
            nid = state.get("node_id")
            if uid and nid:
                self.logger.log("NODE_LEAVE", {"node_id": nid, "username": uid})
                if self.on_user_left:
                    self.on_user_left(nid)
            self.logger.log("SYSTEM", {"ws_client_disconnected": str(remote)})

    # ----------------------------------------------------------------- auth
    async def _handle_auth(self, ws, msg: dict, remote):
        room_code = msg.get("room_code", "")
        username = msg.get("username", "")
        node_id = msg.get("node_id", self.node_id)

        if not self.auth.verify_room_code(room_code):
            self.logger.log("AUTH_FAIL", {"reason": "wrong room code", "from": str(remote)})
            await self._send(ws, {"event": "auth_fail", "reason": "Invalid room code"})
            return

        ok, reason = self.auth.register_user(node_id, username)
        if not ok:
            self.logger.log("AUTH_FAIL", {"reason": reason, "username": username})
            await self._send(ws, {"event": "auth_fail", "reason": reason})
            return

        async with self._lock:
            self._clients[ws]["authed"] = True
            self._clients[ws]["username"] = username
            self._clients[ws]["node_id"] = node_id

        leader = self.get_leader()
        await self._send(ws, {
            "event": "auth_ok",
            "node_id": node_id,
            "username": username,
            "is_leader": leader == self.node_id,
            "leader_id": leader,
        })

        # Notify middleware of join
        if self.on_user_joined:
            ip = remote[0] if remote else "unknown"
            self.on_user_joined(node_id, username, ip)

        # Send current view
        view = self.group_view.get_view()
        await self._send(ws, {"event": "view_update", "view": view})

    # ----------------------------------------------------------------- message
    async def _handle_message(self, ws, msg: dict):
        state = self._clients.get(ws, {})
        username = state.get("username", "unknown")
        node_id = state.get("node_id", self.node_id)
        text = msg.get("text", "").strip()
        if not text:
            return
        if len(text) > 2000:
            await self._send(ws, {"event": "error", "reason": "Message too long (max 2000 chars)"})
            return

        payload = {
            "kind": "chat",
            "sender": node_id,
            "username": username,
            "text": text,
            "ts": time.time(),
        }
        self.logger.log("MSG_SENT", {"sender": username, "text": text[:50]})
        success = self.sequencer.submit_message(payload)
        if not success:
            await self._send(ws, {"event": "error", "reason": "Could not submit message (no leader?)"})

    # ----------------------------------------------------------------- push to browser
    def push_chat_message(self, payload: dict, seq: int):
        """Called by sequencer.on_deliver — push delivered message to all local browsers."""
        event = {
            "event": "chat_message",
            "seq": seq,
            "sender": payload.get("sender"),
            "username": payload.get("username", "unknown"),
            "text": payload.get("text", ""),
            "ts": payload.get("ts", time.time()),
        }
        self._broadcast_sync(event)

    def push_system_event(self, event: dict):
        """Push any system event to all local browsers."""
        self._broadcast_sync(event)

    def push_view_update(self, view_id: int, view: dict):
        self._broadcast_sync({"event": "view_update", "view": {"view_id": view_id, **view}})

    def push_leader_change(self, leader_id: str):
        username = self.auth.get_username(leader_id) or leader_id[:8]
        self._broadcast_sync({
            "event": "leader_change",
            "leader_id": leader_id,
            "leader_username": username,
        })

    def push_member_join(self, node_id: str, username: str):
        self._broadcast_sync({"event": "member_join", "node_id": node_id, "username": username})

    def push_member_leave(self, node_id: str, username: str):
        self._broadcast_sync({"event": "member_leave", "node_id": node_id, "username": username})

    # ----------------------------------------------------------------- internal
    def _on_log_event(self, record: dict):
        """Receives logger events and forwards to browser event log panel."""
        event = {"event": "system_log", "record": record}
        self._broadcast_sync(event)

    def _broadcast_sync(self, event: dict):
        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._broadcast(event), self._loop)

    async def _broadcast(self, event: dict):
        async with self._lock:
            clients = [ws for ws, s in self._clients.items() if s.get("authed")]
        for ws in clients:
            await self._send(ws, event)

    async def _broadcast_all(self, event: dict):
        """Broadcast to all connected clients including unauthenticated."""
        async with self._lock:
            clients = list(self._clients.keys())
        for ws in clients:
            await self._send(ws, event)

    async def _send(self, ws, event: dict):
        try:
            await ws.send(json.dumps(event))
        except Exception:
            pass
