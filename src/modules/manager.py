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

logger = logging.getLogger(__name__)

class Manager:
    def __init__(self, proxies):
        self.proxies = proxies
        self.api: botDetectorApi = botDetectorApi(
            app_config.ENDPOINT,
            app_config.QUERY_SIZE,
            app_config.TOKEN,
            app_config.MAX_BYTES,

        )
        self.player_fetch_interval = 180

    async def run(self, post_interval):
        logger.info("Running manager")
        self.post_interval = post_interval

        logger.info("initiating workers")
        workers = [await Worker(proxy).initialize() for proxy in self.proxies]

        logger.info("initiating consumer")
        consumer = AIOKafkaConsumer(
            bootstrap_servers=app_config.KAFKA_HOST,
            group_id="scraper",
            auto_offset_reset='earliest',
        )
        consumer.subscribe(["player"])

        logger.info("initialize the periodic tasks")
        asyncio.ensure_future(self.fetch_players_periodically())
        asyncio.ensure_future(self.post_scraped_players_periodically())

        logger.info("starting consumer")
        await consumer.start()
        try:
            async for msg in consumer:
                if not any(worker.state != WorkerState.BROKEN for worker in workers):
                    raise Exception("Crashing the container")

                # Extract the player from the message
                player = msg.value.decode()
                player = json.loads(player)
                player = Player(**player)

                count_broken = len([w for w in workers if w.state == WorkerState.BROKEN])
                available_workers = [w for w in workers if w.state == WorkerState.FREE]

                if count_broken > 0:
                    logger.warning(f"Broken workers: {count_broken}")

                if not available_workers:
                    logger.info("no available workers.")
                    await asyncio.sleep(10)
                    continue

                worker = random.choice(available_workers)
                assert worker.state == WorkerState.FREE
                asyncio.ensure_future(worker.scrape_player(player))
                await asyncio.sleep(0.1)
        finally:
            await consumer.stop()
            for worker in workers:
                await worker.destroy()


    async def fetch_players(self):
        producer = AIOKafkaProducer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )

        players = await self.api.get_players_to_scrape()

        await producer.start()
        # Produce the fetched players to the "player" topic
        for player in players:
            await producer.send(topic="player", key=player.name.encode(), value=player.dict())
        await producer.stop()
    
    async def fetch_players_periodically(self):
        while True:
            await self.fetch_players()
            await asyncio.sleep(self.player_fetch_interval)

    async def post_scraped_players(self):
        consumer = AIOKafkaConsumer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            group_id="scraper"
        )

        consumer.subscribe(["scraper"])
        await consumer.start()

        try:
            while True:
                batch = []
                async for msg in consumer:
                    msg = msg.value.decode()
                    batch.append(json.loads(msg))
                    
                    if len(batch) == 1000:
                        await self.api.post_scraped_players(batch)
                        batch = []  # Reset batch after processing

                    if not consumer.assignment():
                        logger.debug("break")
                        break  # No more messages in the topic

                if batch:
                    await self.api.post_scraped_players(batch)  # Process remaining players in the last batch
                    batch = []  # Reset batch after processing

                if not consumer.assignment():
                    logger.debug("break")
                    break  # No more messages in the topic
        finally:
            await consumer.stop()

    async def post_scraped_players_periodically(self):
        while True:
            await self.post_scraped_players()
            await asyncio.sleep(self.post_interval)
