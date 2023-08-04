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
        # Start the Manager, initializing necessary components and processing rows from Kafka
        await self.initialize()
        try:
            asyncio.ensure_future(self.get_rows_from_kafka())
            await self.process_rows()
        except Exception as e:
            logger.error(e)
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
        except Exception as e:
            logger.error(e)
        finally:
            logger.warning(f"{self.name} - stopping consumer")
            await self.consumer.stop()

    async def process_rows(self):
        global GLOBAL_SPEED
        global COUNT_MANAGER
        # Process rows from the message queue using available workers
        logger.info(f"{self.name} - start processing rows")
        start_time = int(time.time()) - 1
        init = False
        for worker in self.workers:
            asyncio.ensure_future(worker.run())

        while True:
            qsize = self.message_queue.qsize()
            available_workers = [w for w in self.workers if w.state != WorkerState.FREE]
            broken_workers = [w for w in self.workers if w.state == WorkerState.BROKEN]
            sum_rows = sum([w.count_tasks for w in self.workers])
            sum_time = int(time.time()) - start_time
            real_its = sum_rows / sum_time
            GLOBAL_SPEED.append(real_its)
            logger.info(
                f"{self.name} - {qsize=} - "
                f"{sum_rows}/{sum_time} - {real_its:.2f} it/s - "
                f"available_workers={len(available_workers)} - broken_workers={len(broken_workers)}"
            )

            if GLOBAL_SPEED.maxlen != COUNT_MANAGER * 4:
                GLOBAL_SPEED = deque(maxlen=COUNT_MANAGER * 4)
            
            if not init:
                init = True
                await asyncio.sleep(10)
                continue

            global_speed = sum(GLOBAL_SPEED) / (len(GLOBAL_SPEED) // COUNT_MANAGER)
            logger.info(
                f"{self.name} - GLOBAL_SPEED={global_speed:.2f} - {COUNT_MANAGER=} - {len(GLOBAL_SPEED)=}"
            )
            await asyncio.sleep(10)
