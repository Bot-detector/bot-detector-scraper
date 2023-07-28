import asyncio
import logging

import config.config as config
from config.config import app_config
from modules.api.bot_detector_api import botDetectorApi
from modules.worker import Worker, WorkerState
from modules.validation.player import Player
from aiokafka import AIOKafkaConsumer, TopicPartition
import json
import random
import time
import uuid

logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, proxies: list):
        self.name = str(uuid.uuid4())[-8:]
        self.proxies: list = proxies
        self.api: botDetectorApi = botDetectorApi(
            app_config.ENDPOINT,
            app_config.QUERY_SIZE,
            app_config.TOKEN,
            app_config.MAX_BYTES,
        )
        self.workers: list[Worker]

    async def _process_batch(self, batch: list[Player]):
        for idx, player in enumerate(batch):
            available_workers = [w for w in self.workers if w.state == WorkerState.FREE]

            # breakout if no available workers
            if not available_workers:
                # logger.info("no available workers.")
                await asyncio.sleep(0.01)
                continue

            if (idx % 100 == 0) or (len(available_workers) % 50 == 0):
                working_workers = [
                    w for w in self.workers if w.state != WorkerState.BROKEN
                ]
                logger.info(
                    f"available: {len(available_workers)} / total: {len(working_workers)}"
                )

            _worker = random.choice(available_workers)
            asyncio.ensure_future(_worker.scrape_player(player))
            await asyncio.sleep(0.01)

    async def _process(self):
        sleep = 1
        batch = []
        send_time = time.time()

        LIMIT = 1_500
        # set max records dynamically but with limit
        max_records = len(self.workers) * 30
        max_records = max_records if max_records < LIMIT else LIMIT

        while any(worker.state != WorkerState.BROKEN for worker in self.workers):
            msgs = await self.consumer.getmany(max_records=max_records)

            # capped exponential sleep
            if msgs == {}:
                logger.info(f"{self.name} - no messages, sleeping")
                await asyncio.sleep(sleep)
                sleep = sleep * 2 if sleep * 2 < 60 else 60
                continue

            # parsing all messages
            for topic, messages in msgs.items():
                logger.info(f"{self.name} - {topic=}, {len(messages)=}, {len(batch)=}")
                data: list[Player] = [
                    Player(**json.loads(msg.value.decode())) for msg in messages
                ]
                batch.extend(data)

                if len(batch) > len(self.workers) or send_time + 60 < time.time():
                    start_time = time.time()
                    await self._process_batch(batch)
                    send_time = time.time()
                    delta_time = send_time - start_time
                    working_workers = [
                        w for w in self.workers if w.state != WorkerState.BROKEN
                    ]
                    logger.info(
                        f"{self.name} - scraping: {len(batch)} took {delta_time} seconds, {len(batch)/delta_time:.2f} it/s, workers: {len(working_workers)}"
                    )
                    batch = []

                # commit the latest seen message
                msg = messages[-1]
                tp = TopicPartition(msg.topic, msg.partition)
                await self.consumer.commit({tp: msg.offset + 1})

            # reset sleep
            sleep = 1
        else:
            raise Exception("Crashing the container")

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

    async def destroy(self):
        await self.consumer.stop()

        # Cleanup workers
        for worker in self.workers:
            await worker.destroy()

    async def run(self):
        await self.initialize()
        try:
            await self._process()
        except Exception as e:
            logger.error(str(e))
        finally:
            await self.destroy()
