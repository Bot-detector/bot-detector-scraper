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
        len_batch = len(batch)
        
        while len_batch > 0:
            available_workers = [w for w in self.workers if w.state != WorkerState.BROKEN]
            for player, worker in zip(batch, available_workers):
                asyncio.ensure_future(worker.scrape_player(player))
                len_batch -= 1
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.01)
        return

    async def _process_batch_and_log_stats(self, batch: list):
        len_batch = len(batch)

        start_time = time.time()
        await self._process_batch(batch)
        end_time = time.time()

        delta_time = end_time - start_time

        msg_time = f"scraping: {len_batch} took {delta_time:.2f} seconds, {len(batch)/delta_time:.2f}it/s"

        working_workers = [w for w in self.workers if w.state != WorkerState.BROKEN]
        logger.info(f"{self.name} - {msg_time}, workers: {len(working_workers)}")
        return

    async def _parse_messages_and_commit(self, msgs: dict, batch: list):
        msgs: dict[TopicPartition, list[ConsumerRecord]]
        for topic, messages in msgs.items():
            logger.info(f"{self.name} - {topic=}, {len(messages)=}, {len(batch)=}")
            data = [Player(**json.loads(msg.value.decode())) for msg in messages]

            batch.extend(data)

            # Commit the latest seen message
            msg:ConsumerRecord = messages[-1]
            tp = TopicPartition(msg.topic, msg.partition)
            await self.consumer.commit({tp: msg.offset + 1})

        return batch

    async def _handle_no_messages(self, sleep: int, delta:int=0):
        logger.info(f"{self.name} - no messages, sleeping {sleep}")
        await asyncio.sleep(sleep)
        return sleep + delta

    async def _process(self):
        sleep = 2.9
        batch = []
        send_time = time.time()

        LIMIT = 1_500
        # Set max records dynamically but with limit
        max_records = len(self.workers) * 30
        max_records = min(max_records, LIMIT)

        while any(worker.state != WorkerState.BROKEN for worker in self.workers):
            msgs = await self.consumer.getmany(max_records=max_records)

            if not msgs:
                sleep = await self._handle_no_messages(sleep)
                continue

            batch = await self._parse_messages_and_commit(msgs, batch)

            # should_process_batch
            if len(batch) > len(self.workers) or send_time + 60 < time.time():
                await self._process_batch_and_log_stats(batch)
                send_time = time.time()
                batch = []
        raise Exception("Crashing the container")

    async def initialize(self):
        logger.info(f"{self.name} - initiating workers")
        # self.workers = await asyncio.gather(
        #     *[Worker(proxy).initialize() for proxy in self.proxies]
        # )
        
        self.workers = []
        for proxy in self.proxies:
            worker = Worker(proxy)
            worker = await worker.initialize()
            self.workers.append(worker)
        
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
