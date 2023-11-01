import asyncio
import json
import logging
import random
import time
import uuid
from asyncio import Queue
from collections import deque

from aiokafka import AIOKafkaConsumer

import config.config as config
from config.config import app_config
from modules.validation.player import Player
from modules.worker import Worker, WorkerState

logger = logging.getLogger(__name__)
GLOBAL_SPEED = deque(maxlen=10)
GLOBAL_BROKEN_WORKERS = dict()
COUNT_MANAGER = 0


class Manager:
    def __init__(self, proxies: list[str]):
        global COUNT_MANAGER
        COUNT_MANAGER += 1
        # Initialize the Manager with a unique name and provided proxies
        self.name = str(uuid.uuid4())[-8:]
        self.proxies: list[str] = proxies
        self.workers: list[Worker] = []

        min_size = 100
        max_size = len(proxies) * 4
        max_size = min_size if max_size < min_size else max_size
        self.message_queue = Queue(maxsize=max_size)

    async def destroy(self):
        logger.info(f"{self.name} - destroying")
        # Stop the consumer and clean up workers upon destruction
        await self.consumer.stop()

        # Cleanup workers
        for worker in self.workers:
            await worker.destroy()

    async def initialize(self):
        # Initialize the workers and consumer for Kafka
        logger.info(f"{self.name} - initiating workers")
        self.workers = await asyncio.gather(
            *[Worker(proxy, self.message_queue).initialize() for proxy in self.proxies]
        )

        logger.info(f"{self.name} - initiating consumer")
        consumer = AIOKafkaConsumer(
            bootstrap_servers=app_config.KAFKA_HOST,
            group_id="scraper",
            auto_offset_reset="earliest",
        )
        consumer.subscribe(["player"])

        logger.info(f"{self.name} - starting consumer")
        await consumer.start()

        self.consumer = consumer

    async def run(self):
        global GLOBAL_SPEED
        global COUNT_MANAGER
        # Start the Manager, initializing necessary components and processing rows from Kafka
        await self.initialize()
        try:
            if GLOBAL_SPEED.maxlen != COUNT_MANAGER * 4:
                GLOBAL_SPEED = deque(maxlen=COUNT_MANAGER * 4)
            asyncio.ensure_future(self.get_rows_from_kafka())
            self.process_rows()
            await self.start_logger()
        except Exception as error:
            logger.error(f"{self.name} - {type(error).__name__}: {str(error)}")
        finally:
            await self.destroy()

    async def get_rows_from_kafka(self):
        # Consume rows from Kafka and process them
        logger.info(f"{self.name} - start consuming rows from kafka")
        try:
            async for msg in self.consumer:
                player = msg.value.decode()
                player = json.loads(player)
                player = Player(**player)
                if len(player.name) <= 13:
                    await self.message_queue.put(player)
                await self.consumer.commit()
        except Exception as error:
            logger.error(f"{self.name} - {type(error).__name__}: {str(error)}")
        finally:
            logger.warning(f"{self.name} - stopping consumer")
            await self.consumer.stop()

    async def restart_worker(self, broken_workers: list):
        if not broken_workers:
            return

        worker: Worker = random.choice(broken_workers)
        await worker.destroy()

        proxy = worker.proxy
        chance = random.randint(1, 2)

        if chance == 1:
            logger.info(f"{self.name} - re-enabeling - {worker.name=}")
            self.workers.remove(worker)
            del worker

            worker = Worker(proxy, self.message_queue)
            worker = await worker.initialize()

            asyncio.ensure_future(worker.run())

            self.workers.append(worker)
            print(len(self.workers), worker.name, [w.name for w in self.workers])
        return

    def log_worker_status(
        self, sum_rows: int, sum_time: int, real_its: int, broken_workers: list
    ):
        available_workers = len(self.workers) - len(broken_workers)
        qsize = self.message_queue.qsize()
        logger.info(
            f"{self.name} - {qsize=} - "
            f"{sum_rows}/{sum_time} - {real_its:.2f} it/s - "
            f"available_workers={available_workers} - broken_workers={len(broken_workers)}"
        )
        return

    def log_global_speed(self):
        global GLOBAL_SPEED
        global COUNT_MANAGER
        global GLOBAL_BROKEN_WORKERS

        if (len(GLOBAL_SPEED) // COUNT_MANAGER) == 0:
            return

        _global_speed = sum(GLOBAL_SPEED) / (len(GLOBAL_SPEED) // COUNT_MANAGER)
        broken_workers = sum(v for v in GLOBAL_BROKEN_WORKERS.values())
        logger.info(
            f"{self.name} - "
            f"GLOBAL_SPEED={_global_speed:.2f} - "
            f"{COUNT_MANAGER=} - {len(GLOBAL_SPEED)=} - "
            f"GLOBAL_BROKEN_WORKERS={broken_workers}"
        )
        return

    async def start_logger(self):
        start_time = int(time.time()) - 1
        while True:
            # for worker in self.workers:
            #     logger.info((worker.name, worker.state, worker.errors, worker.count_tasks))

            broken_workers = [w for w in self.workers if w.is_broken()]

            sum_rows = sum([w.count_tasks for w in self.workers])
            sum_time = int(time.time()) - start_time

            if sum_time == 0:
                continue

            real_its = sum_rows / sum_time

            GLOBAL_SPEED.append(real_its)
            GLOBAL_BROKEN_WORKERS[self.name] = len(broken_workers)

            self.log_worker_status(
                sum_rows=sum_rows,
                sum_time=sum_time,
                real_its=real_its,
                broken_workers=broken_workers,
            )

            self.log_global_speed()

            # random chance to restart a broken worker if there are any
            await self.restart_worker(broken_workers)
            await asyncio.sleep(10)

    def process_rows(self):
        # Process rows from the message queue using available workers
        logger.info(f"{self.name} - start processing rows")

        for worker in self.workers:
            asyncio.ensure_future(worker.run())
