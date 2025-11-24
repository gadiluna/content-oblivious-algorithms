
import asyncio
import random
from network import RingNetwork


class OrientedAlgorithmLogarithmic:
    def __init__(self, networkID, ID, network, size, yield_time=0.0, verbose=False):
        self.process_id = networkID
        self.ID = ID
        self.size = size
        self.status = "Undecided"
        self.network = network
        self.verbose = verbose
        self.yield_time = yield_time
        self.receivedOnPort1 = 0
        self.orientation = 0

    def log(self, message):
        if self.verbose:
            print(message)

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

    async def synchronize(self, rounds):
        msg = (1, "sync " + str(self.ID))
        for _ in range(rounds):
            await self.send_message(msg[1], 1)
            msg = await self.receive_message(0)

    async def relay(self, threshold,dir_kill=-1,num_kill=1):
        consecutiveOnPort0 = 0
        killed=0
        while True:
            msg = await self.receive_message(-1)
            if msg[0]== dir_kill and killed < num_kill:
                killed+=1
                continue
            if msg[0] == 1:
                consecutiveOnPort0 = 0
            else:
                consecutiveOnPort0 += 1

            await self.send_message(msg[1], 1 - msg[0])

            if abs(consecutiveOnPort0) >= threshold:
                break

    def encode_integer(self, i):
        binary_i = bin(i)[2:]  # Binary representation without '0b'
        n = len(binary_i)  # Number of bits
        encoded_binary = binary_i + '0' + '1' * n  # Encoding pattern
        return int(encoded_binary, 2)  # Convert back to integer

    async def run(self):
        i = 0
        self.receivedOnPort1 = 0
        myID = self.encode_integer(self.ID)

        while self.status == "Undecided":
            i += 1
            b = myID % 2
            msg = (0, f"compete with ID {self.ID} first phase")
            self.log(
                f"Process {self.process_id} with ID: {self.ID}, myID: {myID}, b: {b} executing round: {i} with receivedOnPort1: {self.receivedOnPort1}")

            while self.receivedOnPort1 < (i) * self.size:
                await self.send_message(msg[1], 0)
                msg = await self.receive_message(1)
                self.receivedOnPort1 += 1

            if b == 0:
                self.log(f"Process {self.process_id} with ID: {self.ID}, b: {b} executing round: {i} SYNCHRONIZING")
                await self.synchronize(1)#self.size)
                if myID == 0:
                    self.log(
                        f"Process {self.process_id} with ID: {self.ID}, b: {b} executing round: {i} LEADER-LASTSYNC")
                    self.status = "Leader"
                    await self.synchronize(1)
                    self.log(f"Process {self.process_id} with ID: {self.ID}, b: {b} executing round: {i} LEADER-END")
                    return
                else:
                    myID = myID // 2
            else:
                self.log(f"Process {self.process_id} with ID: {self.ID}, b: {b} executing round: {i} second phase")
                msg = (1, f"compete with ID {self.ID} second phase")
                while True:
                    await self.send_message(msg[1], 0)
                    msg = await self.receive_message(-1)
                    if msg[0] == 1:
                        self.receivedOnPort1 += 1
                    if msg[0] == 0 or self.receivedOnPort1 == (i + 1) * self.size:
                        break

                if msg[0] == 0:
                    self.status = "Non-Leader"
                    self.log(
                        f"Process {self.process_id} with ID: {self.ID}, b: {b} executing round: {i} LOSER BALANCING")
                    await self.send_message(msg[1], 1)
                    #msg = self.receive_message(1)
                    self.log(
                        f"Process {self.process_id} with ID: {self.ID}, b: {b} executing round: {i} LOSER RELAYING")
                    await self.relay(1+1,dir_kill=1,num_kill=1)#self.size+1,dir_kill=1,num_kill=1)
                    return
                else:
                #    self.log(
                #        f"Process {self.process_id} with ID: {self.ID}, b: {b} executing round: {i}  SYNCHRONIZING 2")
                #    self.synchronize(self.size)
                    myID = myID // 2


async def _run_experiments():
    random.seed()
    expcount = 0

    #if needed increase expcount
    while expcount <= 100:
        expcount = expcount + 1
        num_processes = random.randint(2,100)
        max_id = random.randint(num_processes+1,3*num_processes)
        min_id = 1
        print(f"Experiment {expcount} Processes:{num_processes} Max_ID: {max_id}")
        IDS = random.sample(range(min_id,max_id),num_processes)
        network = RingNetwork(num_processes, verbose=True, logfile=f"exp_{expcount}")
        #the line below tests the non-uniform oriented algorithm that has O(nUlog(ID_min)) complexity
        processes = [OrientedAlgorithmLogarithmic(networkID=i, ID=IDS[i], size=num_processes, network=network,
                                                 yield_time=random.uniform(0.00001,0.0001), verbose=False) for i in
                     range(num_processes)]
        
        tasks = [asyncio.create_task(process.run()) for process in processes]
        await asyncio.gather(*tasks)

        result = []
        for process in processes:
            result.append(process.status)

        num_leader = result.count("Leader")
        if num_leader == 1:
            print(f"Experiment {expcount} completed")
            # for process in processes:
            #    print(f"Process {process.process_id}:{process.ID} status: {process.status}")
        else:
            print(f"Experiment {expcount} FAIL")
            print(f"Process {process.process_id}:{process.ID} status: {process.status}")
            for process in processes:
                print(f"Process {process.process_id}:{process.ID} status: {process.status}")
            break


if __name__ == "__main__":
    asyncio.run(_run_experiments())
