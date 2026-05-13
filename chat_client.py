"""
Terminal chat client for distributed-chat.
Usage:
    python chat_client.py --ws ws://localhost:8765 --room DEMO123 --username Alice
"""

import asyncio
import json
import argparse
import sys
import os
from datetime import datetime

import websockets
from colorama import init, Fore, Style

init(autoreset=True)

# ── Colours ──────────────────────────────────────────────────────────────────
C_SELF    = Fore.CYAN + Style.BRIGHT
C_OTHER   = Fore.GREEN + Style.BRIGHT
C_SYSTEM  = Fore.YELLOW
C_ERROR   = Fore.RED + Style.BRIGHT
C_LEADER  = Fore.MAGENTA + Style.BRIGHT
C_DIM     = Style.DIM
C_RESET   = Style.RESET_ALL

def ts():
    return datetime.now().strftime('%H:%M:%S')

def print_system(text: str):
    print(f"\r{C_SYSTEM}[{ts()}] *** {text}{C_RESET}")

def print_error(text: str):
    print(f"\r{C_ERROR}[{ts()}] ERR {text}{C_RESET}")

def print_message(seq: int, username: str, text: str, is_self: bool):
    color = C_SELF if is_self else C_OTHER
    print(f"\r{C_DIM}#{seq:<4}{C_RESET} {color}{username:<16}{C_RESET} {text}")

def print_prompt():
    print(f"{Fore.CYAN}> {C_RESET}", end='', flush=True)

# ── Input reader (non-blocking, works on Windows) ────────────────────────────
async def read_input(loop: asyncio.AbstractEventLoop) -> str:
    return await loop.run_in_executor(None, sys.stdin.readline)

# ── Main ─────────────────────────────────────────────────────────────────────
async def run(ws_url: str, room_code: str, username: str):
    my_node_id = None
    leader_id  = None

    print(f"{C_DIM}Connecting to {ws_url} ...{C_RESET}")

    try:
        async with websockets.connect(ws_url) as ws:
            # Auth
            await ws.send(json.dumps({
                "action": "auth",
                "room_code": room_code,
                "username": username,
            }))

            # Wait for auth response
            raw = await ws.recv()
            msg = json.loads(raw)

            if msg["event"] == "auth_fail":
                print_error(f"Auth failed: {msg['reason']}")
                return

            if msg["event"] != "auth_ok":
                print_error(f"Unexpected response: {msg}")
                return

            my_node_id = msg["node_id"]
            leader_id  = msg.get("leader_id")
            leader_mark = " (leader)" if msg.get("is_leader") else ""
            print_system(f"Joined as {C_SELF}{username}{C_RESET}{C_SYSTEM} on node {my_node_id[:8]}{leader_mark}")
            print_system("Type a message and press Enter to send. Ctrl+C to quit.")
            print()
            print_prompt()

            loop = asyncio.get_event_loop()

            async def receiver():
                nonlocal leader_id
                async for raw in ws:
                    msg = json.loads(raw)
                    event = msg.get("event")

                    if event == "chat_message":
                        is_self = msg["sender"] == my_node_id
                        print_message(msg["seq"], msg["username"], msg["text"], is_self)
                        print_prompt()

                    elif event == "member_join":
                        print_system(f"{msg['username']} joined")
                        print_prompt()

                    elif event == "member_leave":
                        print_system(f"{msg['username']} left")
                        print_prompt()

                    elif event == "leader_change":
                        leader_id = msg["leader_id"]
                        is_me = leader_id == my_node_id
                        who = "You are" if is_me else f"{msg['leader_username']} is"
                        print_system(f"{C_LEADER}★ {who} the new leader{C_RESET}")
                        print_prompt()

                    elif event == "error":
                        print_error(msg["reason"])
                        print_prompt()

                    elif event in ("pong", "system_log", "view_update"):
                        pass  # suppress noise

            async def sender():
                while True:
                    line = await read_input(loop)
                    text = line.rstrip('\n').strip()
                    if not text:
                        print_prompt()
                        continue
                    if text.lower() in ('/quit', '/exit'):
                        print_system("Disconnecting…")
                        await ws.close()
                        return
                    await ws.send(json.dumps({"action": "message", "text": text}))

            async def keepalive():
                while True:
                    await asyncio.sleep(15)
                    try:
                        await ws.send(json.dumps({"action": "ping"}))
                    except Exception:
                        break

            await asyncio.gather(receiver(), sender(), keepalive())

    except (ConnectionRefusedError, OSError):
        print_error(f"Could not connect to {ws_url}. Is the node running?")
    except KeyboardInterrupt:
        print_system("Disconnected.")
    except websockets.exceptions.ConnectionClosed:
        print_system("Connection closed by server.")


def main():
    parser = argparse.ArgumentParser(description="Distributed Chat — terminal client")
    parser.add_argument("--ws",       default="ws://localhost:8765", help="WebSocket URL of the node")
    parser.add_argument("--room",     required=True,                 help="Room code")
    parser.add_argument("--username", required=True,                 help="Your username")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.ws, args.room, args.username))
    except KeyboardInterrupt:
        print()

if __name__ == "__main__":
    main()
