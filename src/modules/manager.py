import asyncio
import logging

import config.config as config
from config.config import app_config
from modules.bot_detector_api import botDetectorApi
from modules.worker import Worker
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json

logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, proxies):
        self.proxies = proxies
        self.workers = []
        self.api: botDetectorApi = botDetectorApi(
            app_config.ENDPOINT,
            app_config.QUERY_SIZE,
            app_config.TOKEN,
            app_config.MAX_BYTES,

        )
        self.player_fetch_interval = 60

    async def run(self, post_interval):
        logger.info("Running manager")
        self.post_interval = post_interval

        # Start workers for each proxy
        tasks = []
        for proxy in self.proxies:
            worker = Worker(proxy, self)
            self.workers.append(worker)
            tasks.append(asyncio.create_task(worker.run(timeout=app_config.SESSION_TIMEOUT)))
        
        # Start the task to fetch players periodically
        tasks.append(asyncio.create_task(self.fetch_players_periodically()))
        
        # Start the task to post scraped players via API periodically
        tasks.append(asyncio.create_task(self.post_scraped_players_periodically()))

        await asyncio.gather(*tasks)

    def remove_worker(self, worker):
        self.workers.remove(worker)

    async def fetch_players(self):
        producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9094",  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )

        players = await self.api.get_players_to_scrape()


        await producer.start()
        # Produce the fetched players to the "player" topic
        for player in players:
            await producer.send(topic="player", value=player.dict())
        await producer.stop()
    
    async def fetch_players_periodically(self):
        while True:
            await self.fetch_players()
            await asyncio.sleep(self.player_fetch_interval)

    async def post_scraped_players(self):
        consumer = AIOKafkaConsumer(
            "player",  # Topic to consume from
            bootstrap_servers="localhost:9094",  # Kafka broker address
        )
        
        data = []
        
        await consumer.start()
        # Consume players from the "scraper" topic
        async for msg in consumer:
            msg = msg.value.decode()
            data.append(json.loads(msg))
        await consumer.stop()

        await self.api.post_scraped_players(data)

    async def post_scraped_players_periodically(self):
        while True:
            await self.post_scraped_players()
            await asyncio.sleep(self.post_interval)
