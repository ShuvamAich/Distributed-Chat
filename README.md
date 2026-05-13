# Distributed Chat

A LAN-based distributed chat system implementing core distributed systems concepts: **leader election (Bully algorithm)**, **total-order message delivery (sequencer pattern)**, **UDP broadcast discovery**, **heartbeat fault detection**, and **group view management**.

## Architecture

```
┌─────────────┐     WebSocket      ┌──────────────────────────────────┐
│  Browser UI │ ◄────────────────► │           Chat Node              │
│  (Vite/TS)  │                    │  ┌──────────┐  ┌─────────────┐  │
└─────────────┘                    │  │ WSBridge │  │  Sequencer  │  │
                                   │  └──────────┘  └─────────────┘  │
                                   │  ┌──────────┐  ┌─────────────┐  │
                                   │  │ Election │  │  Discovery  │  │
                                   │  └──────────┘  └─────────────┘  │
                                   │  ┌──────────┐  ┌─────────────┐  │
                                   │  │  Fault   │  │ Group View  │  │
                                   │  │ Detector │  │             │  │
                                   │  └──────────┘  └─────────────┘  │
                                   └──────────────────────────────────┘
                                            │  TCP / UDP
                                            ▼
                                      Other Nodes on LAN
```

## Features

- **Bully Election** — highest-priority node (by UUID int) becomes leader
- **Total-Order Delivery** — leader assigns global sequence numbers; members hold-back until gaps fill
- **UDP Broadcast Discovery** — nodes find peers automatically on the LAN (port 50000)
- **Heartbeat Fault Detection** — UDP heartbeats on port 50001; missed beats trigger re-election
- **Group View** — versioned membership list broadcast to all browser clients on change
- **Auth** — room code (SHA-256) + unique username per session
- **Real-time Event Log** — browser UI streams all middleware events live

## Port Allocation (per node)

| Port | Purpose |
|------|---------|
| `tcp_port` (default 9000) | Sequencer + app TCP |
| `tcp_port + 1` (9001) | Election TCP |
| `ws_port` (default 8765) | Browser WebSocket |
| `50000` (fixed) | UDP discovery broadcast |
| `50001` (fixed) | UDP heartbeat |

## Quick Start

### Backend

```bash
pip install -r requirements.txt

# Node 1
python server/node.py --port 9000 --ws-port 8765 --room-code DEMO123

# Node 2 (same or different machine)
python server/node.py --port 9002 --ws-port 8766 --room-code DEMO123
```

### Frontend

```bash
cd frontend
npm install
npx vite --host --port 5173
```

Open browser tabs:
- `http://localhost:5173/` → connects to node on ws port `8765`
- `http://localhost:5173/?port=8766` → connects to node on ws port `8766`

Enter room code `DEMO123` and a unique username in each tab.

## Project Structure

```
distributed-chat/
├── server/
│   ├── node.py              # Entry point / ChatNode orchestrator
│   ├── auth.py              # Room-code + username auth
│   ├── ws_bridge.py         # asyncio WebSocket bridge
│   └── middleware/
│       ├── discovery.py     # UDP broadcast peer discovery
│       ├── election.py      # Bully election algorithm
│       ├── sequencer.py     # Total-order sequencer
│       ├── fault_detector.py# Heartbeat-based fault detection
│       ├── group_view.py    # Versioned membership view
│       └── logger.py        # Structured event logger
├── frontend/
│   ├── src/
│   │   ├── main.ts          # Full chat UI (auth, messages, members, event log)
│   │   └── style.css        # Dark-themed chat UI styles
│   ├── index.html
│   └── package.json
└── requirements.txt
```

## WebSocket Protocol

**Browser → Node:**
```json
{ "action": "auth",    "room_code": "...", "username": "..." }
{ "action": "message", "text": "..." }
{ "action": "ping" }
```

**Node → Browser:**
```json
{ "event": "auth_ok",       "node_id": "...", "username": "...", "is_leader": true, "leader_id": "..." }
{ "event": "chat_message",  "seq": 1, "sender": "...", "username": "...", "text": "...", "ts": 0.0 }
{ "event": "member_join",   "node_id": "...", "username": "..." }
{ "event": "member_leave",  "node_id": "...", "username": "..." }
{ "event": "leader_change", "leader_id": "...", "leader_username": "..." }
{ "event": "view_update",   "view": { "view_id": 1, "members": { ... } } }
{ "event": "system_log",    "record": { ... } }
```
