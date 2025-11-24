import asyncio
import json
import random
from typing import Dict, Tuple, Optional

import networkx as nx


class RingNetwork:
    """
    Threadless network simulator.
    Messages are delivered with an adversarial asynchronous delay that is modeled
    by scheduling the enqueue on the destination port at a later time on the
    asyncio event loop.
    """

    def __init__(
        self,
        num_processes: int,
        verbose: bool = False,
        logfile: str = "log.txt",
        delivery_delay_range: Tuple[float, float] = (0.0, 0.0),
    ):
        self.num_processes = num_processes
        self.ports_zero: Dict[int, asyncio.Queue] = {i: asyncio.Queue() for i in range(num_processes)}
        self.ports_one: Dict[int, asyncio.Queue] = {i: asyncio.Queue() for i in range(num_processes)}
        self.verbose = verbose
        self.logfile = logfile
        self.delivery_delay_range = delivery_delay_range
        if verbose:
            with open(logfile, "w") as file:
                file.write("")

    def log(self, message: str) -> None:
        if self.verbose:
            with open(self.logfile, "a") as log_file:
                log_file.write("NETWORK:" + message + "\n")

    async def send(self, sender_id: int, ID: int, orientation: int, message, direction: int = 0) -> None:
        """
        Schedule a message delivery on the receiver's incoming port with a random delay.
        """
        mydirection = (direction + orientation) % 2
        if mydirection == 0:
            receiver_id = (sender_id + 1) % self.num_processes  # Send to the next process in the ring
            target_queue = self.ports_one[receiver_id]
        else:
            receiver_id = (sender_id - 1) % self.num_processes  # Send to the previous process in the ring
            target_queue = self.ports_zero[receiver_id]

        delay = random.uniform(*self.delivery_delay_range)
        loop = asyncio.get_running_loop()

        def deliver() -> None:
            target_queue.put_nowait(message)
            self.log(
                f"Process {sender_id}:{ID} sent message to Process {receiver_id}: {message} using port {direction}"
            )

        loop.call_later(delay, deliver)
        # Yield control so the scheduler can interleave processes fairly.
        await asyncio.sleep(0)

    def receive(self, receiver_id: int, ID: int, orientation: int, direction: int):
        """
        Non-blocking receive: returns None if no message is available on the requested ports.
        """
        if direction == orientation:
            if not self.ports_zero[receiver_id].empty():
                message = self.ports_zero[receiver_id].get_nowait()
                self.log(f"Process {receiver_id}:{ID} received message: {message} on Port {direction}")
                return (0, message)
        if direction == (orientation + 1) % 2:
            if not self.ports_one[receiver_id].empty():
                message = self.ports_one[receiver_id].get_nowait()
                self.log(f"Process {receiver_id}:{ID} received message: {message} on Port {direction}")
                return (1, message)

        if direction == -1:
            if random.randint(0, 1):
                if not self.ports_zero[receiver_id].empty():
                    message = self.ports_zero[receiver_id].get_nowait()
                    self.log(
                        f"Process {receiver_id}:{ID} received message: {message} on Port {0} when checking any port"
                    )
                    return (orientation, message)
                if not self.ports_one[receiver_id].empty():
                    message = self.ports_one[receiver_id].get_nowait()
                    self.log(
                        f"Process {receiver_id}:{ID} received message: {message} on Port {1} when checking any port"
                    )
                    return ((orientation + 1) % 2, message)
            else:
                if not self.ports_one[receiver_id].empty():
                    message = self.ports_one[receiver_id].get_nowait()
                    self.log(
                        f"Process {receiver_id}:{ID} received message: {message} on Port {1} when checking any port"
                    )
                    return ((orientation + 1) % 2, message)
                if not self.ports_zero[receiver_id].empty():
                    message = self.ports_zero[receiver_id].get_nowait()
                    self.log(
                        f"Process {receiver_id}:{ID} received message: {message} on Port {0} when checking any port"
                    )
                    return (orientation, message)

        self.log(f"Process {receiver_id}:{ID} received no message checking direction {direction}")
        return None

    def double_receive(self, receiver_id: int, ID: int, orientation: int):
        if random.randint(0, 1):
            if self.ports_zero[receiver_id].qsize() > 1:
                message1 = self.ports_zero[receiver_id].get_nowait()
                message2 = self.ports_zero[receiver_id].get_nowait()
                self.log(
                    f"Process {receiver_id}:{ID} received message: {message1} and {message2} on Port {0} when checking any port"
                )
                return (orientation, message1, message2)

            if self.ports_one[receiver_id].qsize() > 1:
                message1 = self.ports_one[receiver_id].get_nowait()
                message2 = self.ports_one[receiver_id].get_nowait()
                self.log(
                    f"Process {receiver_id}:{ID} received message: {message1} and {message2} on Port {1} when checking any port"
                )
                return ((orientation + 1) % 2, message1, message2)
        else:
            if self.ports_one[receiver_id].qsize() > 1:
                message1 = self.ports_one[receiver_id].get_nowait()
                message2 = self.ports_one[receiver_id].get_nowait()
                self.log(
                    f"Process {receiver_id}:{ID} received message: {message1} and {message2} on Port {1} when checking any port"
                )
                return ((orientation + 1) % 2, message1, message2)
            if self.ports_zero[receiver_id].qsize() > 1:
                message1 = self.ports_zero[receiver_id].get_nowait()
                message2 = self.ports_zero[receiver_id].get_nowait()
                self.log(
                    f"Process {receiver_id}:{ID} received message: {message1} and {message2} on Port {0} when checking any port"
                )
                return (orientation, message1, message2)
        self.log(f"Process {receiver_id}:{ID} received no message checking all for double receive")
        return None


class GraphNetwork:
    """
    General asynchronous network simulator backed by a graph.
    Nodes have local integer ports; edges specify the port on each endpoint and a max delay.
    Messages are delivered after a random uniform delay in [0, edge_delay].
    """

    def __init__(self, graph: nx.Graph, verbose: bool = False, logfile: str = "log.txt"):
        self.graph = graph
        self.num_processes = graph.number_of_nodes()
        self.verbose = verbose
        self.logfile = logfile
        if verbose:
            with open(logfile, "w") as file:
                file.write("")

        # (node, port) -> (neighbor, neighbor_port, max_delay)
        self.routing: Dict[Tuple[int, int], Tuple[int, int, float]] = {}
        self.port_queues: Dict[int, Dict[int, asyncio.Queue]] = {}
        for u, v, data in graph.edges(data=True):
            port_u = data["port_u"]
            port_v = data["port_v"]
            max_delay = float(data.get("delay", 0.0))
            self._ensure_port_queue(u, port_u)
            self._ensure_port_queue(v, port_v)
            self.routing[(u, port_u)] = (v, port_v, max_delay)
            self.routing[(v, port_v)] = (u, port_u, max_delay)

    @classmethod
    def from_json(cls, path: str, verbose: bool = False, logfile: str = "log.txt") -> "GraphNetwork":
        with open(path, "r") as f:
            data = json.load(f)
        graph = nx.Graph()
        for node in data.get("nodes", []):
            node_id = node["id"]
            attrs = {k: v for k, v in node.items() if k != "id"}
            graph.add_node(node_id, **attrs)
        for edge in data.get("edges", []):
            u = edge["source"]
            v = edge["target"]
            port_u = edge["port_source"]
            port_v = edge["port_target"]
            delay = float(edge.get("delay", 0.0))
            graph.add_edge(u, v, port_u=port_u, port_v=port_v, delay=delay)
        return cls(graph, verbose=verbose, logfile=logfile)

    def _ensure_port_queue(self, node: int, port: int) -> None:
        if node not in self.port_queues:
            self.port_queues[node] = {}
        if port not in self.port_queues[node]:
            self.port_queues[node][port] = asyncio.Queue()

    def log(self, message: str) -> None:
        if self.verbose:
            with open(self.logfile, "a") as log_file:
                log_file.write("NETWORK:" + message + "\n")

    async def send(self, sender_id: int, port: int, message) -> None:
        """
        Send a message on a specific local port. The port determines the neighbor and target port.
        """
        if (sender_id, port) not in self.routing:
            self.log(f"Process {sender_id} attempted send on unknown port {port}")
            await asyncio.sleep(0)
            return

        receiver_id, receiver_port, max_delay = self.routing[(sender_id, port)]
        target_queue = self.port_queues[receiver_id][receiver_port]
        delay = random.uniform(0.0, max_delay)
        loop = asyncio.get_running_loop()

        def deliver() -> None:
            target_queue.put_nowait(message)
            self.log(
                f"Process {sender_id} sent message to Process {receiver_id} on port {port} (recv port {receiver_port}) msg={message}"
            )

        loop.call_later(delay, deliver)
        await asyncio.sleep(0)

    def receive(self, receiver_id: int, port: int):
        """
        Non-blocking receive. If port >= 0, check that port. If port == -1, check any port.
        Returns (port, message) or None if no message is available.
        """
        if receiver_id not in self.port_queues:
            self.log(f"Process {receiver_id} has no ports configured")
            return None

        if port >= 0:
            queue_map = self.port_queues[receiver_id]
            if port in queue_map and not queue_map[port].empty():
                message = queue_map[port].get_nowait()
                self.log(f"Process {receiver_id} received message: {message} on Port {port}")
                return (port, message)
        else:
            ports = list(self.port_queues[receiver_id].keys())
            random.shuffle(ports)
            for p in ports:
                if not self.port_queues[receiver_id][p].empty():
                    message = self.port_queues[receiver_id][p].get_nowait()
                    self.log(f"Process {receiver_id} received message: {message} on Port {p}")
                    return (p, message)

        self.log(f"Process {receiver_id} received no message checking port {port}")
        return None
