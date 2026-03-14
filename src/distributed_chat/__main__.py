from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from .config import ChatConfig, parse_seed_peers
from .gui import launch_gui
from .node import ChatNode


async def run_headless(config: ChatConfig) -> None:
    def sink(payload: dict[str, Any]) -> None:
        print(json.dumps(payload, default=str))

    node = ChatNode(config, sink)
    await node.start()
    print("Headless mode started. Type messages and press Enter. Press Ctrl+C to exit.")
    try:
        while True:
            text = await asyncio.to_thread(input, "> ")
            if text.strip():
                await node.send_chat(text)
    finally:
        await node.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed chat demo")
    parser.add_argument("--headless", action="store_true", help="Run without the Tkinter frontend")
    parser.add_argument("--username", default="user")
    parser.add_argument("--password", default="changeme")
    parser.add_argument("--room", default="demo-room")
    parser.add_argument("--host", default=ChatConfig().host)
    parser.add_argument("--port", type=int, default=60000)
    parser.add_argument("--multicast-group", default="239.255.42.99")
    parser.add_argument("--multicast-port", type=int, default=45454)
    parser.add_argument(
        "--seed-peer",
        action="append",
        default=[],
        help="Optional fallback peer in host:port format. Repeat or comma-separate values when multicast is unavailable.",
    )
    parser.add_argument("--priority", type=int, default=ChatConfig().priority)
    args = parser.parse_args()

    config = ChatConfig(
        username=args.username,
        password=args.password,
        room_name=args.room,
        host=args.host,
        tcp_port=args.port,
        multicast_group=args.multicast_group,
        multicast_port=args.multicast_port,
        seed_peers=parse_seed_peers(args.seed_peer),
        priority=args.priority,
    )
    if args.headless:
        asyncio.run(run_headless(config))
        return
    launch_gui()


if __name__ == "__main__":
    main()
