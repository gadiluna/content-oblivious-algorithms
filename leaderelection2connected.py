import argparse
import asyncio
import os
import random
from typing import Dict, List, Optional, Set

from network import GraphNetwork


class LeaderElection2Connected:
    """
    Async implementation of the non-uniform leader election algorithm for 2-edge-connected graphs.
    The logic follows the pseudocode in the provided proof, preserving the counting and DFS
    notification phases with content-oblivious pulses.
    """

    def __init__(
        self,
        node_id: int,
        identifier: int,
        network: GraphNetwork,
        upper_bound_n: int,
        yield_time: float = 0.0005,
        verbose: bool = False,
    ):
        self.node_id = node_id
        self.original_id = identifier
        self.N = upper_bound_n
        self.id_value = identifier * self.N  # force identifiers to multiples of N
        self.network = network
        self.verbose = verbose
        self.yield_time = yield_time

        self.port_list: List[int] = sorted(self.network.port_queues[self.node_id].keys())
        self.sigma: Dict[int, int] = {p: 0 for p in self.port_list}
        self.rho: Dict[int, int] = {p: 0 for p in self.port_list}

        self.state: Optional[str] = None  # None during synchronized counting, then "Leader"/"Non-Leader"
        self.leader_id: Optional[int] = None
        self.parent_port: Optional[int] = None
        self.counter: int = 0
        self.status: Optional[str] = None  # mirror of state for external inspection

    def log(self, message: str) -> None:
        if self.verbose:
            print(f"[node {self.node_id}] {message}")

    async def yield_control(self) -> None:
        await asyncio.sleep(self.yield_time)

    async def send_pulse(self, port: int) -> None:
        await self.network.send(self.node_id, port, message=None)
        self.sigma[port] += 1

    async def send_all(self, count: int = 1) -> None:
        for _ in range(count):
            for p in self.port_list:
                await self.send_pulse(p)

    async def send_pulses_until(self, port: int, target: int) -> None:
        while self.sigma[port] < target:
            await self.send_pulse(port)

    def min_rho(self) -> int:
        return min(self.rho.values()) if self.rho else 0

    async def wait_any_message(self) -> int:
        while True:
            msg = self.network.receive(self.node_id, -1)
            if msg is not None:
                port, _ = msg
                self.rho[port] = self.rho.get(port, 0) + 1
                return port
            await self.yield_control()

    async def wait_for_rho(self, port: int, target: int) -> None:
        while self.rho.get(port, 0) < target:
            await self.wait_any_message()

    async def synchronized_counting(self) -> None:
        await self.send_all(1)
        while self.state is None:
            port = await self.wait_any_message()
            if self.rho[port] == self.min_rho():
                self.counter += 1
                await self.send_all(1)
                if self.counter == self.id_value:
                    self.state = "Leader"
                    self.leader_id = self.id_value
                    self.log("Event: start (leader)")
                    break
            if self.rho[port] - self.sigma[port] > 1:
                self.state = "Non-Leader"
                self.parent_port = port
                self.leader_id = (self.counter // self.N) * self.N
                await self.wait_for_rho(port, self.leader_id + self.N + 2)
                self.log(f"Event: receiveexplore on port {port}")
                break

    async def notify_phase(self) -> None:
        P: Set[int] = set(self.port_list)
        if self.state == "Non-Leader" and self.parent_port in P:
            P.remove(self.parent_port)

        while P:
            j = min(P)
            self.log(f"Event: sendexplore on port {j}")
            await self.wait_for_rho(j, self.leader_id + 1)
            await self.send_pulses_until(j, self.leader_id + self.N + 2)

            while j in P:
                # Check if any port in P has reached the done threshold.
                triggered = False
                for h in list(P):
                    if self.rho.get(h, 0) >= self.leader_id + self.N + 2:
                        if h == j:
                            P.remove(j)
                            self.log(f"Event: receivedone on port {j}")
                        else:
                            P.remove(h)
                            self.log(f"Event: receiveexplore on port {h}")
                            self.log(f"Event: senddone on port {h}")
                            await self.send_pulses_until(h, self.leader_id + self.N + 2)
                        triggered = True
                        break
                if triggered:
                    continue
                await self.wait_any_message()

        if self.state == "Non-Leader" and self.parent_port is not None:
            self.log(f"Event: senddone on parent port {self.parent_port}")
            await self.send_pulses_until(self.parent_port, self.leader_id + self.N + 2)

    async def run(self) -> None:
        await self.synchronized_counting()
        await self.notify_phase()
        self.status = self.state


async def run_experiments(topology_path: str, trials: int = 1, verbose: bool = False) -> None:
    for exp in range(1, trials + 1):
        network = GraphNetwork.from_json(topology_path, verbose=False, logfile=f"2conn_exp_{exp}")
        node_ids = sorted(network.graph.nodes())
        n = len(node_ids)
        ids = random.sample(range(1, 3 * n + 1), n)
        processes = [
            LeaderElection2Connected(
                node_id=node_ids[i],
                identifier=ids[i],
                network=network,
                upper_bound_n=n,
                yield_time=random.uniform(0.0001, 0.001),
                verbose=verbose,
            )
            for i in range(n)
        ]
        tasks = [asyncio.create_task(p.run()) for p in processes]
        await asyncio.gather(*tasks)

        statuses = [p.status for p in processes]
        num_leaders = statuses.count("Leader")
        print(f"Experiment {exp} on {os.path.basename(topology_path)} -> leaders: {num_leaders}/{n}")
        if num_leaders != 1:
            print("Failure: expected exactly one leader.")
            for p in processes:
                print(f"Node {p.node_id} (id={p.id_value}) status={p.status}")
            break


def main() -> None:
    parser = argparse.ArgumentParser(description="Leader election for 2-edge-connected graphs (async).")
    parser.add_argument("--topology", default="grid4x4.json", help="Path to JSON topology file.")
    parser.add_argument("--trials", type=int, default=1, help="Number of experiments to run.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose per-node logging.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed.")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
    asyncio.run(run_experiments(args.topology, trials=args.trials, verbose=args.verbose))


if __name__ == "__main__":
    main()
