import asyncio
import logging

import config.config as config
from config.config import app_config
from modules.bot_detector_api import botDetectorApi
from modules.worker import Worker, WorkerState
from modules.validation.player import Player
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
import random
import time

logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, proxies: list):
        self.proxies: list = proxies
        self.api: botDetectorApi = botDetectorApi(
            app_config.ENDPOINT,
            app_config.QUERY_SIZE,
            app_config.TOKEN,
            app_config.MAX_BYTES,
        )
        self.player_fetch_interval: int = 180
        self.workers: list[Worker]

    
    async def _process(self):
        sleep = 1
        while any(worker.state != WorkerState.BROKEN for worker in self.workers):
            msgs = await self.consumer.getmany(max_records=500)
            
            # capped exponential sleep
            if msgs == {}:
                logger.info("no messages, sleeping")
                await asyncio.sleep(sleep)
                sleep = sleep *2 if sleep*2 < 60 else 60
                continue
            sleep = 1
            
            # parsing all messages
            for topic, messages in msgs.items():
                logger.info(f"{topic=}")
                players = [Player(**json.loads(msg.value.decode())) for msg in messages]
            
            for idx, player in enumerate(players):
                available_workers = [
                    w for w in self.workers if w.state == WorkerState.FREE
                ]

                # breakout if no available workers
                if not available_workers:
                    logger.info("no available workers.")
                    await asyncio.sleep(1)
                    continue
                
                if (idx % 100 == 0) or (len(available_workers) % 50 == 0):
                    logger.info(f"{len(available_workers)=}")

                _worker = random.choice(available_workers)
                asyncio.ensure_future(_worker.scrape_player(player))
                await asyncio.sleep(0.01)
        else:
            raise Exception("Crashing the container")

    async def initialize(self, post_interval):
        logger.info("Running manager")
        self.post_interval = post_interval

        logger.info("initiating workers")
        self.workers = [await Worker(proxy).initialize() for proxy in self.proxies]

        logger.info("initiating consumer")
        consumer = AIOKafkaConsumer(
            bootstrap_servers=app_config.KAFKA_HOST,
            group_id="scraper",
            auto_offset_reset="earliest",
        )
        consumer.subscribe(["player"])

        logger.info("initialize the periodic tasks")
        asyncio.ensure_future(self.fetch_players_periodically())
        asyncio.ensure_future(self.post_scraped_players())

        logger.info("starting consumer")
        await consumer.start()
        
        self.consumer = consumer

    async def destroy(self):
        await self.consumer.stop()

        # Cleanup workers
        for worker in self.workers:
            await worker.destroy()

    async def run(self, post_interval):
        await self.initialize(post_interval)
        try:
            await self._process()
        finally:
            await self.destroy()

    async def fetch_players(self):
        producer = AIOKafkaProducer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )

        players = await self.api.get_players_to_scrape()

        await producer.start()
        # Produce the fetched players to the "player" topic
        for player in players:
            await producer.send(
                topic="player", key=player.name.encode(), value=player.dict()
            )
        await producer.stop()

    async def fetch_players_periodically(self):
        while True:
            await self.fetch_players()
            await asyncio.sleep(self.player_fetch_interval)

    async def post_scraped_players(self):
        consumer = AIOKafkaConsumer(
            bootstrap_servers=app_config.KAFKA_HOST, group_id="scraper"
        )

        consumer.subscribe(["scraper"])
        await consumer.start()

        start_time = time.time()
        sleep = 1

        try:
            while True:
                msgs = await consumer.getmany(timeout_ms=1_000)

                if msgs == {}:
                    _sleep = sleep + random.randint(0,5)
                    await asyncio.sleep(_sleep)
                    sleep = sleep*2 if sleep*2 < 60 else 60
                    continue
                
                sleep = 1

                for topic, messages in msgs.items():
                    batch = [json.loads(msg.value.decode()) for msg in messages]
                    logger.info(len(batch))
                    await self.api.post_scraped_players(batch)
        finally:
            await consumer.stop()
