import asyncio
import logging

import config.config as config
from config.config import app_config
from modules.api.bot_detector_api import botDetectorApi
from modules.worker import Worker, WorkerState
from modules.validation.player import Player
from aiokafka import AIOKafkaConsumer, TopicPartition, ConsumerRecord
import json
import time
import uuid
from asyncio import Queue
import random

logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, proxies: list):
        self.name = str(uuid.uuid4())[-8:]
        self.proxies: list = proxies
        self.workers: list[Worker] = []
        self.message_queue = Queue(maxsize=len(proxies) * 4)


    async def destroy(self):
        await self.consumer.stop()

        # Cleanup workers
        for worker in self.workers:
            await worker.destroy()

    async def initialize(self):
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
        logger.info(f"{self.name} - start consuming rows from kafka")
        try:
            async for msg in self.consumer:
                player = msg.value.decode()
                player = json.loads(player)
                player = Player(**player)
                await self.message_queue.put(player)
                await self.consumer.commit()
        except Exception as e:
            logger.error(e)
        finally:
            logger.warning(f"{self.name} - stopping consumer")
            await self.consumer.stop()

    async def process_rows(self):
        logger.info(f"{self.name} - start processing rows")
        count = 1
        start_time = int(time.time())

        while True:
            message: Player = await self.message_queue.get()
            available_workers = [
                w for w in self.workers if w.state != WorkerState.BROKEN
            ]
            
            if not available_workers:
                await asyncio.sleep(1)
                continue

            worker = random.choice(available_workers)
            asyncio.ensure_future(worker.scrape_player(message))
            self.message_queue.task_done()

            if count % 100 == 0:
                qsize = self.message_queue.qsize()
                delta = int(time.time()) - start_time
                logger.info(f"{self.name} - {qsize=} - {count/delta:.2f} it/s - {len(available_workers)=}")
            count += 1
