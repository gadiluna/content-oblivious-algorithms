import asyncio
import random
from network import RingNetwork


class UnorientedAlgorithm:
    def __init__(self, networkID, ID, network, yield_time=0.1, verbose=False, logfile="log.txt"):
        self.process_id = networkID
        self.ID = 2 * ID  # Doubling the ID as per the algorithm
        self.status = None  # "Leader" or "Non-Leader"
        self.network = network
        self.verbose = verbose
        self.orientation = random.randint(0, 1)
        self.yield_time = yield_time
        self.logfile = logfile

    def log(self, message):
        if self.verbose:
            with open(self.logfile, "a") as log_file:
                log_file.write(message + "\n")

    async def send_message(self, message, direction):
        await self.network.send(self.process_id, self.ID, self.orientation, message, direction=direction)
        await self.yield_control()

    async def receive_message(self, direction):
        msg = self.network.receive(self.process_id, self.ID, self.orientation, direction)
        while msg is None:
            await self.yield_control()
            msg = self.network.receive(self.process_id, self.ID, self.orientation, direction)
        return msg

    async def yield_control(self):
        await asyncio.sleep(random.uniform(0, self.yield_time))

    async def run(self):
        counter = [0, 0]
        for _ in range(1, self.ID + 1):
            await self.send_message("fase1message", 0)
            await self.send_message("fase1message", 1)

            await self.receive_message(direction=0)
            counter[0] = counter[0] + 1
            await self.receive_message(direction=1)
            counter[1] = counter[1] + 1

        self.log(f"Process {self.process_id} with ID {self.ID} executing ChekAlone")

        await self.send_message("checkAlone", 0)
        await self.send_message("checkAlone", 0)
        await self.receive_message(direction=1)
        counter[1] = counter[1] + 1
        msg = await self.receive_message(direction=-1)
        port = msg[0]
        counter[port] = counter[port] + 1

        if port == 1:
            await self.send_message("LEADER", direction=0)
            await self.receive_message(direction=1)

            counter[1] = counter[1] + 1
            self.status = "Leader"
            self.log(f"Process {self.process_id}:{self.ID} status: {self.status}")
            return
        else:
            self.log(f"Process {self.process_id}:{self.ID} executing CancelMessages")
            await self.send_message("cancel", 1)
            await self.send_message("cancel", 1)
            await self.receive_message(direction=0)

            counter[0] = counter[0] + 1
            await self.receive_message(direction=1)

            counter[1] = counter[1] + 1

            self.status = "Non-Leader"
            self.log(f"Process {self.process_id}:{self.ID} executing UnorientedRelayAndWaitTermination")
            counter[0] = 0
            counter[1] = 0
            while True:
                msg = await self.receive_message(direction=-1)

                port = msg[0]
                counter[port] = counter[port] + 1
                await self.send_message(msg[1], (port + 1) % 2)
                if abs(counter[0] - counter[1]) >= 3:
                    self.log(f"Process {self.process_id}:{self.ID} status: {self.status}")
                    return


async def _run_experiments():
    random.seed()
    expcount = 0

    while expcount < 20:
        expcount = expcount + 1
        num_processes = random.randint(2, 30)
        max_id = random.randint(num_processes + 1, 3 * num_processes)
        min_id = 1
        print(f"Experiment {expcount} Processes:{num_processes} Max_ID: {max_id}")
        IDS = random.sample(range(min_id, max_id), num_processes)
        network = RingNetwork(num_processes, verbose=False, logfile=f"unexp_{expcount}")
        processes = [
            UnorientedAlgorithm(
                networkID=i,
                ID=IDS[i],
                network=network,
                yield_time=random.uniform(0.00001, 0.0001),
                verbose=False,
            )
            for i in range(num_processes)
        ]

        tasks = [asyncio.create_task(process.run()) for process in processes]
        await asyncio.gather(*tasks)

        result = [process.status for process in processes]
        num_leader = result.count("Leader")
        if num_leader == 1:
            print(f"Experiment {expcount} completed")
        else:
            print(f"Experiment {expcount} FAIL")
            for process in processes:
                print(f"Process {process.process_id}:{process.ID} status: {process.status}")
            break


if __name__ == "__main__":
    asyncio.run(_run_experiments())
