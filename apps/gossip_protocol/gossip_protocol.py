import asyncio
import math
import random
from datetime import datetime
from typing import Dict, Tuple, List


class Node:
    # Constants
    HEARTBEAT_RATE_MAX_SECONDS = 7
    HEARTBEAT_RATE_MIN_SECONDS = 1

    # Attributes
    uuid: int
    registry: Dict[int, Tuple[int, datetime]]

    # Tunable Config
    heartbeat_rate: int

    def __init__(self):
        self.uuid = hash(self)
        self.registry = {}
        self.heartbeat_rate = random.randint(self.HEARTBEAT_RATE_MIN_SECONDS, self.HEARTBEAT_RATE_MAX_SECONDS)
        self.registry[self.uuid] = (0, datetime.utcnow())

    def __repr__(self):
        return str(self.uuid)[-3:]

    def __str__(self):
        return str(self.uuid)[-3:]

    def heartbeat(self):
        self.registry[self.uuid] = (self.registry[self.uuid][0] + 1, datetime.utcnow())


class Coordinator:
    # Attributes
    node_list: List[Node]
    node_map: Dict[int, Node]
    node_queue: asyncio.Queue
    subset_amount: int
    total_gossips: int
    gossip_history = []

    # Constants
    SUBSET_STEP = 5
    GOSSIP_LIMIT = 25
    GOSSIP_RATE = 1
    DEFAULT_QUEUE_SIZE = 50

    def __init__(self, queue_size=DEFAULT_QUEUE_SIZE):
        self.node_list = []
        self.node_map = {}
        self.node_queue = asyncio.Queue(maxsize=queue_size)
        self.subset_amount = 1
        self.total_gossips = 0

    def add(self):
        node = Node()
        self.node_list.append(node)
        self.node_map[node.uuid] = node
        # Update node count that each node holds in their registry
        self.subset_amount = math.ceil(len(self.node_list) / self.SUBSET_STEP)

    def remove(self, node):
        self.node_list.remove(node)
        self.node_map.pop(node)
        # Update node count that each node holds in their registry
        self.subset_amount = math.ceil(len(self.node_list) / self.SUBSET_STEP)

    async def heartbeat_task(self, node):
        while True:
            node.heartbeat()
            print(f' [Heartbeater {node}] Sleeping {node.heartbeat_rate} seconds')
            await asyncio.sleep(node.heartbeat_rate)

    async def gossip_worker(self):
        while True:
            gossiper_node = self.gossip_history[-1]
            receiver_node = await self.node_queue.get()

            print(f' [Gossiper {gossiper_node}] Gossiping {len(gossiper_node.registry)} nodes info to {receiver_node}...')

            # Gossip
            self.total_gossips += 1
            receiver_node.registry.update(gossiper_node.registry)

            # Delay
            await asyncio.sleep(self.GOSSIP_RATE)

            # Receiver becomes gossiper
            self.gossip_history.append(receiver_node)

            # Choose next receiver
            if self.total_gossips < self.GOSSIP_LIMIT:
                exclude_list = [gossiper_node, receiver_node]
                self.node_queue.put_nowait(
                    random.choice([node for node in self.node_list if node not in exclude_list])
                )

            # Signal for next task
            self.node_queue.task_done()

    async def process(self):
        # Select the initial node that will gossip
        initial_gossiper_node = random.choice(self.node_list)
        self.gossip_history.append(initial_gossiper_node)

        # Select the initial node that will receive the gossip
        initial_receiver_node = random.choice([node for node in self.node_list if node != initial_gossiper_node])

        # Use the asyncio.Queue to handle adding next receiver node dynamically
        self.node_queue.put_nowait(initial_receiver_node)

        # Schedule one gossiper task and a heartbeat task per node
        tasks = [asyncio.create_task(self.gossip_worker())]
        for node in self.node_list:
            tasks.append(asyncio.create_task(self.heartbeat_task(node)))

        # Once the queue is processed, cancel all running tasks
        await self.node_queue.join()
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    def run(self):
        asyncio.run(self.process())
