from __future__ import annotations

import asyncio
from queue import Empty, Queue
import threading
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any

from .config import ChatConfig
from .node import ChatNode
from .order import VectorClock


class NodeController:
    def __init__(self, event_queue: Queue[dict[str, Any]]) -> None:
        self.event_queue = event_queue
        self.loop: asyncio.AbstractEventLoop | None = None
        self.node: ChatNode | None = None
        self.thread: threading.Thread | None = None
        self.started = threading.Event()

    def start(self, config: ChatConfig) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.started.clear()
        self.thread = threading.Thread(target=self._thread_main, args=(config,), daemon=True)
        self.thread.start()
        self.started.wait(timeout=10)

    def _thread_main(self, config: ChatConfig) -> None:
        loop = asyncio.new_event_loop()
        self.loop = loop
        asyncio.set_event_loop(loop)
        self.node = ChatNode(config, self.event_queue.put)
        loop.run_until_complete(self.node.start())
        self.started.set()
        try:
            loop.run_forever()
        finally:
            if self.node:
                loop.run_until_complete(self.node.stop())
            loop.close()

    def send_message(self, text: str) -> None:
        if not self.loop or not self.node:
            return
        asyncio.run_coroutine_threadsafe(self.node.send_chat(text), self.loop)

    def stop(self) -> None:
        if not self.loop or not self.node:
            return
        future = asyncio.run_coroutine_threadsafe(self.node.stop(), self.loop)
        try:
            future.result(timeout=5)
        except Exception:
            pass
        self.loop.call_soon_threadsafe(self.loop.stop)
        if self.thread:
            self.thread.join(timeout=5)
        self.loop = None
        self.node = None


class ChatApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Distributed Chat Demo")
        self.geometry("1200x760")
        self.minsize(1040, 680)
        self.event_queue: Queue[dict[str, Any]] = Queue()
        self.controller = NodeController(self.event_queue)
        self._build_ui()
        self.after(200, self._drain_events)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=3)
        self.columnconfigure(1, weight=2)
        self.rowconfigure(1, weight=1)

        config_frame = ttk.LabelFrame(self, text="Session")
        config_frame.grid(row=0, column=0, columnspan=2, sticky="nsew", padx=12, pady=12)
        for column in range(8):
            config_frame.columnconfigure(column, weight=1)

        self.username_var = tk.StringVar(value="user")
        self.room_var = tk.StringVar(value="demo-room")
        self.host_var = tk.StringVar(value=ChatConfig().host)
        self.port_var = tk.StringVar(value="60000")
        self.group_var = tk.StringVar(value="239.255.42.99")
        self.group_port_var = tk.StringVar(value="45454")
        self.priority_var = tk.StringVar(value=str(ChatConfig().priority))
        self.status_var = tk.StringVar(value="Offline")
        self.leader_var = tk.StringVar(value="Leader: unknown")

        controls = [
            ("Username", self.username_var),
            ("Room", self.room_var),
            ("Host", self.host_var),
            ("TCP Port", self.port_var),
            ("Multicast Group", self.group_var),
            ("Multicast Port", self.group_port_var),
            ("Priority", self.priority_var),
        ]
        self.entry_widgets: list[ttk.Entry] = []
        for index, (label, variable) in enumerate(controls):
            ttk.Label(config_frame, text=label).grid(row=0, column=index, sticky="w", padx=4, pady=(6, 0))
            entry = ttk.Entry(config_frame, textvariable=variable)
            entry.grid(row=1, column=index, sticky="ew", padx=4, pady=6)
            self.entry_widgets.append(entry)

        self.start_button = ttk.Button(config_frame, text="Start Node", command=self._start_node)
        self.start_button.grid(row=2, column=0, columnspan=2, sticky="ew", padx=4, pady=8)
        self.stop_button = ttk.Button(config_frame, text="Stop Node", command=self._stop_node, state="disabled")
        self.stop_button.grid(row=2, column=2, columnspan=2, sticky="ew", padx=4, pady=8)
        ttk.Label(config_frame, textvariable=self.status_var).grid(row=2, column=4, columnspan=2, sticky="w", padx=4)
        ttk.Label(config_frame, textvariable=self.leader_var).grid(row=2, column=6, columnspan=2, sticky="w", padx=4)

        chat_frame = ttk.LabelFrame(self, text="Ordered Chat")
        chat_frame.grid(row=1, column=0, sticky="nsew", padx=(12, 6), pady=(0, 12))
        chat_frame.columnconfigure(0, weight=1)
        chat_frame.rowconfigure(0, weight=1)

        self.chat_text = tk.Text(chat_frame, wrap="word", state="disabled", font=("Consolas", 10))
        self.chat_text.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        chat_scroll = ttk.Scrollbar(chat_frame, orient="vertical", command=self.chat_text.yview)
        chat_scroll.grid(row=0, column=1, sticky="ns", pady=8)
        self.chat_text.configure(yscrollcommand=chat_scroll.set)

        send_frame = ttk.Frame(chat_frame)
        send_frame.grid(row=1, column=0, columnspan=2, sticky="ew", padx=8, pady=(0, 8))
        send_frame.columnconfigure(0, weight=1)
        self.message_var = tk.StringVar()
        entry = ttk.Entry(send_frame, textvariable=self.message_var)
        entry.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        entry.bind("<Return>", lambda _event: self._send_message())
        self.send_button = ttk.Button(send_frame, text="Send", command=self._send_message, state="disabled")
        self.send_button.grid(row=0, column=1)

        right_frame = ttk.Frame(self)
        right_frame.grid(row=1, column=1, sticky="nsew", padx=(6, 12), pady=(0, 12))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(1, weight=1)

        members_frame = ttk.LabelFrame(right_frame, text="Group View")
        members_frame.grid(row=0, column=0, sticky="nsew", pady=(0, 8))
        members_frame.columnconfigure(0, weight=1)
        members_frame.rowconfigure(0, weight=1)
        self.members_tree = ttk.Treeview(members_frame, columns=("user", "role", "priority", "seq", "endpoint"), show="headings", height=8)
        for name, width in (("user", 120), ("role", 80), ("priority", 80), ("seq", 70), ("endpoint", 220)):
            self.members_tree.heading(name, text=name.title())
            self.members_tree.column(name, width=width, anchor="w")
        self.members_tree.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        log_frame = ttk.LabelFrame(right_frame, text="Live Event Log")
        log_frame.grid(row=1, column=0, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, wrap="word", state="disabled", font=("Consolas", 9))
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns", pady=8)
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def _start_node(self) -> None:
        try:
            config = ChatConfig(
                username=self.username_var.get().strip() or "user",
                room_name=self.room_var.get().strip() or "demo-room",
                host=self.host_var.get().strip() or ChatConfig().host,
                tcp_port=int(self.port_var.get()),
                multicast_group=self.group_var.get().strip() or "239.255.42.99",
                multicast_port=int(self.group_port_var.get()),
                priority=int(self.priority_var.get()),
            )
        except ValueError as exc:
            messagebox.showerror("Invalid settings", str(exc))
            return
        self.controller.start(config)
        for widget in self.entry_widgets:
            widget.configure(state="disabled")
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.send_button.configure(state="normal")
        self.status_var.set("Starting")
        self._append_log("INFO", "ui", "Node startup requested")

    def _stop_node(self) -> None:
        self.controller.stop()
        for widget in self.entry_widgets:
            widget.configure(state="normal")
        self.start_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.send_button.configure(state="disabled")
        self.status_var.set("Offline")
        self.leader_var.set("Leader: unknown")
        self._append_log("WARN", "ui", "Node stopped")

    def _send_message(self) -> None:
        text = self.message_var.get().strip()
        if not text:
            return
        self.controller.send_message(text)
        self.message_var.set("")

    def _drain_events(self) -> None:
        while True:
            try:
                payload = self.event_queue.get_nowait()
            except Empty:
                break
            self._handle_event(payload)
        self.after(200, self._drain_events)

    def _handle_event(self, payload: dict[str, Any]) -> None:
        kind = payload.get("kind")
        if kind == "status":
            self.status_var.set(f"{payload.get('state')}: {payload.get('detail')}")
            if payload.get("state") in {"LEADER", "FOLLOWER"}:
                self.leader_var.set(payload.get("detail", "Leader: unknown"))
            return
        if kind == "chat":
            vector = VectorClock.summarize(payload.get("vector_clock", {}), payload.get("vector_clock", {}).keys())
            text = (
                f"[{payload.get('timestamp')}] seq={payload.get('seq'):>3} | sender={payload.get('username')} | "
                f"created={payload.get('created_at')} | committed={payload.get('delivered_at')} | "
                f"lamport={payload.get('lamport')} | vector={vector}\n"
                f"    {payload.get('text')}\n"
            )
            self._append_chat(text)
            return
        if kind == "view":
            self._refresh_members(payload.get("members", []), payload.get("leader_uid"))
            return
        if kind == "event":
            self._append_log(payload.get("level", "INFO"), payload.get("category", "system"), payload.get("message", ""))

    def _refresh_members(self, members: list[dict[str, Any]], leader_uid: str | None) -> None:
        for item in self.members_tree.get_children():
            self.members_tree.delete(item)
        leader_detail = "Leader: unknown"
        for member in members:
            endpoint = f"{member.get('host')}:{member.get('port')}"
            role = member.get("role")
            if member.get("uid") == leader_uid:
                role = "leader"
                leader_detail = f"Leader is {member.get('username')} @ {endpoint}"
            self.members_tree.insert(
                "",
                "end",
                values=(member.get("username"), role, member.get("priority"), member.get("last_delivered_seq"), endpoint),
            )
        self.leader_var.set(leader_detail)

    def _append_chat(self, text: str) -> None:
        self.chat_text.configure(state="normal")
        self.chat_text.insert("end", text + "\n")
        self.chat_text.see("end")
        self.chat_text.configure(state="disabled")

    def _append_log(self, level: str, category: str, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{level}] {category}: {message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _on_close(self) -> None:
        self.controller.stop()
        self.destroy()


def launch_gui() -> None:
    app = ChatApp()
    app.mainloop()
