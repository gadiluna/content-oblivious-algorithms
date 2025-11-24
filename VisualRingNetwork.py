import asyncio
import math
import random
import threading
import time
import tkinter as tk
from tkinter import scrolledtext, simpledialog, ttk

from network import RingNetwork
from orientedalgorithm import OrientedAlgorithmLogarithmic
from unorientedalgorithm import UnorientedAlgorithm


class VisualNetwork:
    """
    Visual wrapper around RingNetwork that decouples send (enqueue) from delivery.
    Messages are enqueued into a pending list; GUI actions trigger delivery into
    the underlying RingNetwork queues on the asyncio loop.
    """

    def __init__(self, num_processes, loop, gui_logger=None, verbose=False, logfile="log.txt"):
        self.num_processes = num_processes
        self.loop = loop
        self.inner = RingNetwork(num_processes, verbose=verbose, logfile=logfile)
        self.pending = []
        self.pending_lock = threading.Lock()
        self.gui_logger = gui_logger

    def log(self, message):
        self.inner.log(message)
        if self.gui_logger:
            self.gui_logger("NETWORK: " + message)

    async def send(self, sender_id, ID, orientation, message, direction=0):
        record = {
            "sender": sender_id,
            "ID": ID,
            "orientation": orientation,
            "message": message,
            "direction": direction,
            "time": time.time(),
        }
        with self.pending_lock:
            self.pending.append(record)
        self.log(f"Enqueued (pending) from {sender_id}:{ID} -> '{message}' dir={direction}")
        await asyncio.sleep(0)

    def _deliver_record_to_inner(self, record):
        # executed on the asyncio loop thread
        sender_id = record["sender"]
        direction = record["direction"]
        orientation = record["orientation"]
        message = record["message"]
        mydirection = (direction + orientation) % 2
        if mydirection == 0:
            receiver_id = (sender_id + 1) % self.num_processes
            target_queue = self.inner.ports_one[receiver_id]
        else:
            receiver_id = (sender_id - 1) % self.num_processes
            target_queue = self.inner.ports_zero[receiver_id]
        target_queue.put_nowait(message)
        self.log(
            f"Delivered from {sender_id}:{record['ID']} -> '{message}' dir={direction} (recv {receiver_id})"
        )

    def deliver_one_random(self):
        with self.pending_lock:
            if not self.pending:
                return None
            idx = random.randrange(len(self.pending))
            record = self.pending.pop(idx)
        self.loop.call_soon_threadsafe(self._deliver_record_to_inner, record)
        return record

    def get_pending_snapshot(self):
        with self.pending_lock:
            return list(self.pending)

    def receive(self, receiver_id, ID, orientation, direction):
        return self.inner.receive(receiver_id, ID, orientation, direction)

    def double_receive(self, receiver_id, ID, orientation):
        return self.inner.double_receive(receiver_id, ID, orientation)


# ----------------------------- GUI Simulator -----------------------------
class SimulatorGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Simulatore Visuale - Leader Election")
        self.geometry("1000x780")
        self.configure(bg="white")

        # state
        self.num_processes = 8
        self.node_radius = 30
        self.pending_dot_radius = 6
        self.animation_speed = 0.02
        self.process_objects = []
        self.process_tasks = []
        self.network = None
        self.playing = False
        self.play_interval = 0.5
        self.loop = None
        self.loop_thread = None

        # UI
        control_frame = tk.Frame(self, bg="white")
        control_frame.pack(side=tk.TOP, fill=tk.X, padx=6, pady=6)

        tk.Label(control_frame, text="Algoritmo:", bg="white").pack(side=tk.LEFT)
        self.algo_choice = tk.StringVar(value="OrientedAlgorithmLogarithmic")
        self.algo_menu = ttk.Combobox(
            control_frame,
            textvariable=self.algo_choice,
            values=["OrientedAlgorithmLogarithmic", "UnorientedAlgorithm"],
            state="readonly",
        )
        self.algo_menu.pack(side=tk.LEFT, padx=6)

        tk.Button(control_frame, text="Start", command=self.start_experiment, relief="flat").pack(side=tk.LEFT, padx=6)
        tk.Button(control_frame, text="Play/Pause", command=self.toggle_play, relief="flat").pack(
            side=tk.LEFT, padx=6
        )
        tk.Button(control_frame, text="Step", command=self.step_delivery, relief="flat").pack(side=tk.LEFT, padx=6)
        tk.Button(control_frame, text="Stop", command=self.stop_experiment, relief="flat").pack(side=tk.LEFT, padx=6)

        tk.Label(control_frame, text="Speed(s):", bg="white").pack(side=tk.LEFT, padx=6)
        self.speed = tk.DoubleVar(value=0.5)
        self.speed_scale = tk.Scale(
            control_frame, variable=self.speed, from_=0.01, to=2.0, resolution=0.01, orient="horizontal"
        )
        self.speed_scale.pack(side=tk.LEFT, padx=6)
        self.speed_scale.configure(command=self.on_speed_change)

        # canvas
        self.canvas_size = 720
        self.canvas = tk.Canvas(self, width=self.canvas_size, height=self.canvas_size, bg="white")
        self.canvas.pack(padx=8, pady=4)

        # log
        self.logbox = scrolledtext.ScrolledText(self, width=120, height=10)
        self.logbox.pack(padx=8, pady=4)
        self.logbox.configure(state="disabled")

        # drawing state
        self.node_positions = []
        self.node_circle_items = []
        self.node_id_texts = []
        self.node_pi_texts = []
        self.pending_dot_items = {}

        self.draw_nodes(self.num_processes)
        self.after(150, self.ui_update_loop)

    def gui_log(self, msg):
        ts = time.strftime("%H:%M:%S")
        self.logbox.configure(state="normal")
        self.logbox.insert("end", f"[{ts}] {msg}\n")
        self.logbox.see("end")
        self.logbox.configure(state="disabled")

    def draw_nodes(self, n):
        self.canvas.delete("all")
        self.node_positions.clear()
        self.node_circle_items.clear()
        self.node_id_texts.clear()
        self.node_pi_texts.clear()
        self.pending_dot_items.clear()

        cx = self.canvas_size / 2
        cy = self.canvas_size / 2
        r = min(cx, cy) - 120
        for i in range(n):
            angle = 2 * math.pi * i / n - math.pi / 2
            x = cx + r * math.cos(angle)
            y = cy + r * math.sin(angle)
            self.node_positions.append((x, y))
            circ = self.canvas.create_oval(
                x - self.node_radius, y - self.node_radius, x + self.node_radius, y + self.node_radius, width=2, fill="lightgray"
            )
            idtxt = self.canvas.create_text(x, y, text=str(i), font=("Helvetica", 12, "bold"))
            pitxt = self.canvas.create_text(x, y + self.node_radius + 12, text=f"p_{i}", font=("Helvetica", 10))
            self.node_circle_items.append(circ)
            self.node_id_texts.append(idtxt)
            self.node_pi_texts.append(pitxt)
            self.pending_dot_items[i] = []
            self.canvas.tag_bind(circ, "<Button-1>", lambda ev, idx=i: self.on_node_click(idx))
            self.canvas.tag_bind(idtxt, "<Button-1>", lambda ev, idx=i: self.on_node_click(idx))
            self.canvas.tag_bind(pitxt, "<Button-1>", lambda ev, idx=i: self.on_node_click(idx))

    def on_node_click(self, idx):
        new_id = simpledialog.askinteger("Set ID", f"Set ID for process p_{idx}:", parent=self)
        if new_id is None:
            return
        self.canvas.itemconfigure(self.node_id_texts[idx], text=str(new_id))
        if idx < len(self.process_objects):
            try:
                self.process_objects[idx].ID = new_id
                self.gui_log(f"Updated process {idx} ID -> {new_id}")
            except Exception as e:
                self.gui_log(f"Errore aggiornando ID processo: {e}")

    def _ensure_loop(self):
        if self.loop and self.loop.is_running():
            return

        def loop_runner(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=loop_runner, args=(self.loop,), daemon=True)
        self.loop_thread.start()

    def start_experiment(self):
        # stop existing
        self.stop_experiment()
        self.gui_log("Starting experiment")
        n = self.num_processes
        self.draw_nodes(n)
        ids = random.sample(range(1, 10 * n), n)
        for i in range(n):
            self.canvas.itemconfigure(self.node_id_texts[i], text=str(ids[i]))

        self._ensure_loop()
        self.network = VisualNetwork(n, loop=self.loop, gui_logger=self.gui_log)
        algo = self.algo_choice.get()
        if algo == "OrientedAlgorithmLogarithmic":
            self.process_objects = [
                OrientedAlgorithmLogarithmic(
                    networkID=i,
                    ID=ids[i],
                    size=n,
                    network=self.network,
                    yield_time=random.uniform(0.0001, 0.001),
                    verbose=True,
                )
                for i in range(n)
            ]
        else:
            self.process_objects = [
                UnorientedAlgorithm(
                    networkID=i,
                    ID=ids[i],
                    network=self.network,
                    yield_time=random.uniform(0.0001, 0.001),
                    verbose=True,
                )
                for i in range(n)
            ]

        self.process_tasks = [
            asyncio.run_coroutine_threadsafe(proc.run(), self.loop) for proc in self.process_objects
        ]
        self.gui_log(f"Started {len(self.process_objects)} asyncio tasks using {algo}")

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
        orientation = record["orientation"]
        direction = record["direction"]
        mydirection = (direction + orientation) % 2
        if mydirection == 0:
            receiver = (sender + 1) % self.num_processes
        else:
            receiver = (sender - 1) % self.num_processes

        x0, y0 = self.node_positions[sender]
        x1, y1 = self.node_positions[receiver]
        dot = self.canvas.create_oval(x0 - 8, y0 - 8, x0 + 8, y0 + 8, fill="yellow", outline="")
        steps = 18

        def anim():
            for i in range(1, steps + 1):
                t = i / steps
                xi = x0 + (x1 - x0) * t
                yi = y0 + (y1 - y0) * t
                try:
                    self.canvas.coords(dot, xi - 8, yi - 8, xi + 8, yi + 8)
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
            self.gui_log(f"Delivered '{record['message']}' from p_{sender} to p_{receiver}")

        threading.Thread(target=anim, daemon=True).start()

    def on_speed_change(self, val):
        try:
            v = float(val)
            self.play_interval = v
        except Exception:
            pass

    def ui_update_loop(self):
        if self.network:
            pending = self.network.get_pending_snapshot()
            counts = {i: 0 for i in range(self.num_processes)}
            for rec in pending:
                counts[rec["sender"]] += 1
            for sender, dots in self.pending_dot_items.items():
                desired = counts.get(sender, 0)
                current = len(dots)
                if desired > current:
                    for _ in range(desired - current):
                        x, y = self.node_positions[sender]
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
            for i in range(0, len(self.process_objects)):
                if self.process_objects[i].status == "Leader":
                    self.canvas.itemconfig(self.node_circle_items[i], fill="green")
                elif self.process_objects[i].status == "Non-Leader":
                    self.canvas.itemconfig(self.node_circle_items[i], fill="red")

        self.after(150, self.ui_update_loop)


# ----------------------------- Run -----------------------------
if __name__ == "__main__":
    random.seed()
    app = SimulatorGUI()
    app.mainloop()
