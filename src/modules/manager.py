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


class Manager:
    def __init__(self, proxies: list[str]):
        # Initialize the Manager with a unique name and provided proxies
        self.name = str(uuid.uuid4())[-8:]
        self.proxies: list[str] = proxies
        self.workers: list[Worker] = []
        self.message_queue = Queue(maxsize=len(proxies) * 4)

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
            *[Worker(proxy).initialize() for proxy in self.proxies]
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
            asyncio.ensure_future(self.process_rows())
            asyncio.ensure_future(self.get_rows_from_kafka())
            while True:
                await asyncio.sleep(1)
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
        # Process rows from the message queue using available workers
        logger.info(f"{self.name} - start processing rows")
        last_send_time = int(time.time())
        batch = []
        total_rows = deque(maxlen=100)
        total_time = deque(maxlen=100)

        while True:
            message: Player = await self.message_queue.get()
            batch.append(message)
            delta_send_time = int(time.time()) - last_send_time

            available_workers = [w for w in self.workers if w.state == WorkerState.FREE]

            if not available_workers or not batch:
                await asyncio.sleep(1)
                continue
            
            if len(batch) >= len(available_workers) or delta_send_time > 60:
                num_tasks = min(len(available_workers), len(batch))

                tasks = [
                    worker.scrape_player(player)
                    for worker, player in zip(available_workers, batch)
                ]
                await asyncio.gather(*tasks)

                last_send_time = int(time.time())
                qsize = self.message_queue.qsize()
                broken_workers = [
                    w for w in self.workers if w.state == WorkerState.BROKEN
                ]

                total_rows.append(num_tasks)
                total_time.append(delta_send_time)
                real_its = sum(total_rows) / sum(total_time)

                logger.info(
                    f"{self.name} - {qsize=} - {real_its:.2f} it/s - {len(available_workers)=} - {len(broken_workers)=}"
                )
                batch = batch[num_tasks:]
            self.message_queue.task_done()
