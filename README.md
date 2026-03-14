# Distributed Chat Demo

A Python distributed chat system designed for live demos across three separate computers. The project uses multicast discovery to find nodes dynamically across the LAN, TCP for reliable ordered chat delivery, bully leader election for coordinator failover, detailed event logging for fault-tolerance visibility, and a small Tkinter frontend for operators.

## Implemented architecture

- **Hybrid topology**: every process is a peer for discovery and leader election, while the elected leader temporarily behaves as the room coordinator.
- **Discovery**: UDP multicast announcements allow nodes to discover each other anywhere on the LAN.
- **Transport split**:
  - **UDP multicast** for dynamic discovery.
  - **TCP** for peer sessions, ordered chat messages, heartbeats, election messages, group views, acknowledgements, and recovery sync.
- **Concurrency**: `asyncio` handles network concurrency while the GUI runs in a separate thread.
- **Leader election**: bully algorithm based on numeric node priority.
- **Group view communication**: the leader publishes membership snapshots with a monotonically increasing view version.
- **Fault tolerance**:
  - heartbeat-based failure detection,
  - automatic election on leader loss,
  - delivery acknowledgements,
  - retransmissions for missing recipients,
  - history-based sync for recovering nodes.
- **Ordering choice**: **total ordering** using a sequencer (leader-assigned sequence numbers).
  - Reason: for a live demo, all participants must display the same message order even during discovery churn and failure recovery.
  - Lamport timestamps and vector-clock metadata are still logged to help explain distributed ordering concepts.
- **Open discovery**: any running node on the LAN can be discovered and connected without a password gate.

## Why this design handles the common demo failure

Problem scenario: A sends to B and C, B receives it, C misses it, and later partitions form inconsistent views.

Mitigation in this project:

1. A node never fan-outs chat messages directly to every peer.
2. The current leader assigns one global sequence number for every chat message.
3. Recipients send delivery acknowledgements back to the leader.
4. If an acknowledgement is missing, the leader retransmits the ordered message.
5. If a node detects a sequence gap, it requests a sync starting from the missing sequence.
6. On leader failure, bully election selects a new coordinator and the room view is republished.

This keeps message order consistent and allows lagging nodes to catch up.

## Project layout

- `run_demo.py` - easiest launcher.
- `src/distributed_chat/node.py` - distributed runtime.
- `src/distributed_chat/gui.py` - demo frontend.
- `tests/` - basic unit tests for ordering and configuration helpers.

## Requirements

- Python 3.11+
- Three machines on the same LAN for the full demo
- Multicast enabled on the network
- Same room name on all demo nodes if you want them grouped consistently in the demo

No external Python packages are required.

## Run the GUI demo

On each machine:

1. Open the project folder.
2. Run:
   - `python run_demo.py`
3. In the GUI, enter:
   - a unique username,
   - the same room name on all machines,
  - the machine's LAN IP in the **Host** field,
   - a unique TCP port per machine,
   - optionally a different numeric priority.
4. Click **Start Node**.
5. Wait for discovery and leader election.
6. Send chat messages and watch:
   - ordered sequence numbers,
   - Lamport timestamps,
   - vector-clock summaries,
   - membership view changes,
   - election and retransmission logs.

## Run in headless mode

Useful for terminals or remote sessions:

- `python run_demo.py --headless --username Alice --room demo-room --port 60000 --priority 100`

## Suggested 3-machine live demo script

1. Start node A, B, and C with the same room.
2. Show multicast discovery in the event log.
3. Show leader election by assigning different priorities.
4. Send messages from every node and compare the ordered sequence numbers.
5. Stop the leader node and show automatic re-election.
6. Restart the stopped node and show view recovery plus sync of missing messages.
7. Change which machine is started last to demonstrate dynamic join and recovery.

## Troubleshooting

- If nodes do not discover each other, verify multicast is allowed by the LAN and local firewall.
- Always use the LAN IP of the machine, not `127.0.0.1` or `localhost`.
- If a port is already in use, change the TCP port in the GUI.
- If a node cannot join, verify the TCP ports are reachable across the LAN and the machines are on the same subnet.
- If a machine misses a message, the log should show either retransmission or sync recovery.

## Demo notes

- Keep the event log visible during presentation.
- Use different priorities so bully election is easy to explain.
- The current implementation prioritizes open discovery, demo visibility, and recoverability on a LAN.
- For production hardening, add TLS, persistent replicated logs, and stronger state transfer.
