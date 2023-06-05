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
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
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
