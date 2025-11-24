import asyncio
import json
import math
import os
import random
import threading
import time
import tkinter as tk
from tkinter import filedialog, scrolledtext, simpledialog, ttk

from leaderelection2connected import LeaderElection2Connected
from network import GraphNetwork


class VisualNetwork:
    """
    Visual wrapper around GraphNetwork. Sends are enqueued as pending records; GUI
    actions deliver them into the underlying GraphNetwork queues on the asyncio loop.
    """

    def __init__(self, graph_path: str, loop, gui_logger=None, verbose: bool = False, logfile: str = "log.txt"):
        self.loop = loop
        self.inner = GraphNetwork.from_json(graph_path, verbose=verbose, logfile=logfile)
        self.graph = self.inner.graph
        self.routing = self.inner.routing
        self.port_queues = self.inner.port_queues
        self.num_processes = self.inner.num_processes
        self.pending = []
        self.pending_lock = threading.Lock()
        self.gui_logger = gui_logger

    def log(self, message):
        self.inner.log(message)
        if self.gui_logger:
            self.gui_logger("NETWORK: " + message)

    async def send(self, sender_id: int, port: int, message=None):
        record = {"sender": sender_id, "port": port, "message": message, "time": time.time()}
        with self.pending_lock:
            self.pending.append(record)
        self.log(f"Enqueued (pending) from {sender_id} -> port {port}")
        await asyncio.sleep(0)

    def _deliver_record(self, record):
        sender = record["sender"]
        port = record["port"]
        key = (sender, port)
        if key not in self.inner.routing:
            self.log(f"Dropping message from {sender} on unknown port {port}")
            return
        receiver, receiver_port, _ = self.inner.routing[key]
        queue = self.inner.port_queues[receiver][receiver_port]
        queue.put_nowait(record["message"])
        self.log(f"Delivered from {sender} -> {receiver} via port {port}")

    def deliver_one_random(self):
        with self.pending_lock:
            if not self.pending:
                return None
            idx = random.randrange(len(self.pending))
            record = self.pending.pop(idx)
        self.loop.call_soon_threadsafe(self._deliver_record, record)
        return record

    def get_pending_snapshot(self):
        with self.pending_lock:
            return list(self.pending)

    def receive(self, receiver_id: int, port: int):
        return self.inner.receive(receiver_id, port)


class SimulatorGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("General Graph Visualizer - Leader Election")
        self.geometry("1100x820")
        self.configure(bg="white")

        self.graph_path = "grid4x4.json"
        self.node_radius = 30
        self.pending_dot_radius = 5
        self.animation_speed = 0.02
        self.playing = False
        self.play_interval = 0.4
        self.network = None
        self.process_objects = []
        self.process_tasks = []
        self.loop = None
        self.loop_thread = None
        self.positions = {}
        self.edges = []
        self.node_circle_items = {}
        self.node_id_texts = {}
        self.pending_dot_items = {}

        self._build_ui()
        self.after(150, self.ui_update_loop)

    def _build_ui(self):
        control = tk.Frame(self, bg="white")
        control.pack(side=tk.TOP, fill=tk.X, padx=6, pady=6)

        tk.Label(control, text="Graph:", bg="white").pack(side=tk.LEFT)
        self.graph_entry = tk.Entry(control, width=30)
        self.graph_entry.insert(0, self.graph_path)
        self.graph_entry.pack(side=tk.LEFT, padx=4)
        tk.Button(control, text="Browse", command=self.browse_graph, relief="flat").pack(side=tk.LEFT, padx=4)

        tk.Button(control, text="Start", command=self.start_experiment, relief="flat").pack(side=tk.LEFT, padx=6)
        tk.Button(control, text="Play/Pause", command=self.toggle_play, relief="flat").pack(side=tk.LEFT, padx=6)
        tk.Button(control, text="Step", command=self.step_delivery, relief="flat").pack(side=tk.LEFT, padx=6)
        tk.Button(control, text="Stop", command=self.stop_experiment, relief="flat").pack(side=tk.LEFT, padx=6)

        tk.Label(control, text="Speed(s):", bg="white").pack(side=tk.LEFT, padx=6)
        self.speed = tk.DoubleVar(value=self.play_interval)
        speed_scale = tk.Scale(
            control, variable=self.speed, from_=0.01, to=2.0, resolution=0.01, orient="horizontal", command=self.on_speed_change
        )
        speed_scale.pack(side=tk.LEFT, padx=6)

        # canvas
        self.canvas_size = 760
        self.canvas = tk.Canvas(self, width=self.canvas_size, height=self.canvas_size, bg="white")
        self.canvas.pack(padx=8, pady=4)

        # log
        self.logbox = scrolledtext.ScrolledText(self, width=120, height=10)
        self.logbox.pack(padx=8, pady=4)
        self.logbox.configure(state="disabled")

    def gui_log(self, msg):
        ts = time.strftime("%H:%M:%S")
        self.logbox.configure(state="normal")
        self.logbox.insert("end", f"[{ts}] {msg}\n")
        self.logbox.see("end")
        self.logbox.configure(state="disabled")

    def browse_graph(self):
        path = filedialog.askopenfilename(filetypes=[("JSON", "*.json"), ("All files", "*.*")])
        if path:
            self.graph_entry.delete(0, tk.END)
            self.graph_entry.insert(0, path)

    def _ensure_loop(self):
        if self.loop and self.loop.is_running():
            return

        def loop_runner(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=loop_runner, args=(self.loop,), daemon=True)
        self.loop_thread.start()

    def load_graph(self, path):
        with open(path, "r") as f:
            data = json.load(f)
        nodes = data.get("nodes", [])
        edges = data.get("edges", [])
        self.positions = {}
        for n in nodes:
            node_id = n["id"]
            x = n.get("x")*2
            y = n.get("y")*2
            if x is None or y is None:
                # fallback circle layout
                idx = len(self.positions)
                angle = 2 * math.pi * idx / max(1, len(nodes))
                x = self.canvas_size / 2 + 250 * math.cos(angle)
                y = self.canvas_size / 2 + 250 * math.sin(angle)
            self.positions[node_id] = (x, y)
        self.edges = [(e["source"], e["target"]) for e in edges]
        return nodes

    def draw_graph(self):
        self.canvas.delete("all")
        self.node_circle_items.clear()
        self.node_id_texts.clear()
        self.pending_dot_items.clear()
        # edges
        for u, v in self.edges:
            x0, y0 = self.positions[u]
            x1, y1 = self.positions[v]
            self.canvas.create_line(x0, y0, x1, y1, fill="#ccc", width=2, arrow=tk.LAST)
            self.canvas.create_line(x1, y1, x0, y0, fill="#ccc", width=2, arrow=tk.LAST)
        # nodes
        for node_id, (x, y) in self.positions.items():
            circ = self.canvas.create_oval(
                x - self.node_radius,
                y - self.node_radius,
                x + self.node_radius,
                y + self.node_radius,
                width=2,
                fill="lightgray",
            )
            txt = self.canvas.create_text(x, y, text=str(node_id), font=("Helvetica", 12, "bold"))
            self.node_circle_items[node_id] = circ
            self.node_id_texts[node_id] = txt
            self.pending_dot_items[node_id] = []

    def start_experiment(self):
        self.stop_experiment()
        self.graph_path = self.graph_entry.get()
        nodes = self.load_graph(self.graph_path)
        self.draw_graph()
        self._ensure_loop()
        self.network = VisualNetwork(self.graph_path, loop=self.loop, gui_logger=self.gui_log)
        node_ids = sorted([n["id"] for n in nodes])
        n = len(node_ids)
        ids = random.sample(range(1, 3 * n + 1), n)
        self.process_objects = [
            LeaderElection2Connected(
                node_id=node_ids[i],
                identifier=node_ids[i],
                network=self.network,
                upper_bound_n=n+2,
                yield_time=random.uniform(0.0001, 0.001),
                verbose=False,
            )
            for i in range(n)
        ]
        self.process_tasks = [
            asyncio.run_coroutine_threadsafe(proc.run(), self.loop) for proc in self.process_objects
        ]
        self.gui_log(f"Started {len(self.process_objects)} async processes using LeaderElection2Connected on {os.path.basename(self.graph_path)}")

    def stop_experiment(self):
        self.playing = False
        for fut in getattr(self, "process_tasks", []):
            try:
                fut.cancel()
            except Exception:
                pass
        self.process_tasks = []
        if self.network:
            with self.network.pending_lock:
                self.network.pending.clear()
        self.gui_log("Experiment stopped and pending cleared")

    def toggle_play(self):
        self.playing = not self.playing
        if self.playing:
            self.gui_log("Play")
            threading.Thread(target=self.play_loop, daemon=True).start()
        else:
            self.gui_log("Paused")

    def play_loop(self):
        while self.playing:
            rec = None
            if self.network:
                rec = self.network.deliver_one_random()
            if rec:
                self.animate_delivery(rec)
            else:
                time.sleep(0.05)
            time.sleep(self.play_interval)

    def step_delivery(self):
        if not self.network:
            self.gui_log("No network running")
            return
        rec = self.network.deliver_one_random()
        if rec:
            self.animate_delivery(rec)
        else:
            self.gui_log("No pending messages to deliver (step)")

    def animate_delivery(self, record):
        sender = record["sender"]
        port = record["port"]
        routing = self.network.inner.routing.get((sender, port))
        if not routing:
            return
        receiver, receiver_port, _ = routing
        x0, y0 = self.positions[sender]
        x1, y1 = self.positions[receiver]
        dot = self.canvas.create_oval(x0 - 6, y0 - 6, x0 + 6, y0 + 6, fill="yellow", outline="")
        steps = 18

        def anim():
            for i in range(1, steps + 1):
                t = i / steps
                xi = x0 + (x1 - x0) * t
                yi = y0 + (y1 - y0) * t
                try:
                    self.canvas.coords(dot, xi - 6, yi - 6, xi + 6, yi + 6)
                except Exception:
                    pass
                time.sleep(self.animation_speed)
            try:
                self.canvas.delete(dot)
            except Exception:
                pass
            try:
                if self.pending_dot_items.get(sender):
                    d = self.pending_dot_items[sender].pop()
                    self.canvas.delete(d)
            except Exception:
                pass
            self.gui_log(f"Delivered pending from {sender} (port {port}) to {receiver} (port {receiver_port})")

        threading.Thread(target=anim, daemon=True).start()

    def on_speed_change(self, val):
        try:
            self.play_interval = float(val)
        except Exception:
            pass

    def ui_update_loop(self):
        if self.network:
            pending = self.network.get_pending_snapshot()
            counts = {}
            for rec in pending:
                counts[rec["sender"]] = counts.get(rec["sender"], 0) + 1
            for sender, dots in self.pending_dot_items.items():
                desired = counts.get(sender, 0)
                current = len(dots)
                if desired > current:
                    for _ in range(desired - current):
                        x, y = self.positions[sender]
                        angle = random.uniform(0, 2 * math.pi)
                        r = self.node_radius + 14 + random.uniform(0, 10)
                        dx = r * math.cos(angle)
                        dy = r * math.sin(angle)
                        dot = self.canvas.create_oval(
                            x + dx - self.pending_dot_radius,
                            y + dy - self.pending_dot_radius,
                            x + dx + self.pending_dot_radius,
                            y + dy + self.pending_dot_radius,
                            fill="blue",
                            outline="",
                        )
                        dots.append(dot)
                elif desired < current:
                    for _ in range(current - desired):
                        d = dots.pop()
                        try:
                            self.canvas.delete(d)
                        except Exception:
                            pass

            for proc in self.process_objects:
                if proc.status == "Leader":
                    self.canvas.itemconfig(self.node_circle_items[proc.node_id], fill="green")
                elif proc.status == "Non-Leader":
                    self.canvas.itemconfig(self.node_circle_items[proc.node_id], fill="red")

        self.after(150, self.ui_update_loop)


if __name__ == "__main__":
    random.seed()
    app = SimulatorGUI()
    app.mainloop()
