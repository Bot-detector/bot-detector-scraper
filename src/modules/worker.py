import asyncio
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import ClientSession
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

import config.config as config
from config.config import app_config
from modules.scraper import Scraper
from modules.validation.player import Player

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from modules.manager import Manager


class Worker:
    def __init__(self, proxy, manager):
        self.name = str(uuid.uuid4())
        self.consumer = None
        self.producer = None
        self.scraper = None
        self.proxy: str = proxy
        self._retry_backoff_time = 1
        self.is_active:bool = True

        self.manager: Manager = manager

    async def initialize(self):
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            client_id=self.name,
            group_id="scraper",
            auto_offset_reset='earliest',
            # max_poll_records=1,
            # max_poll_interval_ms=1000
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )

        self.scraper = Scraper(self.proxy)

    async def run(self, timeout: int):
        # Call initialize method before running
        await self.initialize()

        error = f"consumer must be of type AIOKafkaConsumer, received: {type(self.consumer)}."
        assert isinstance(self.consumer, AIOKafkaConsumer), error

        async with aiohttp.ClientSession(timeout=timeout) as session:
            while self.is_active:
                try:
                    self.consumer.subscribe(["player"])
                    # Wait for the consumer and producer to connect
                    await self.consumer.start()
                    await self.producer.start()
                    async for msg in self.consumer:
                        # Commit the consumed message to mark it as processed
                        tp = TopicPartition(msg.topic, msg.partition)
                        # await self.consumer.commit({tp: msg.offset + 1})

                        # Extract the player from the message
                        player = msg.value.decode()
                        player = json.loads(player)

                        # Scrape the player and produce the result
                        await self.scrape_data(session, Player(**player))
                except Exception as e:
                    logger.warning(f"{str(e)}")
                finally:
                    await self.consumer.stop()
                    await self.producer.stop()
                    # Exponential backoff
                    await asyncio.sleep(2**self._retry_backoff_time)
                    self._retry_backoff_time = self._retry_backoff_time**2

    async def scrape_data(self, session: ClientSession, player: Player):
        hiscore = await self.scraper.lookup_hiscores(player, session)

        if hiscore == "ClientHttpProxyError":
            logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
            self.manager.remove_worker(self)
            self.is_active = False
            return

        if hiscore is None:
            logger.warning(f"Hiscore is empty for {player.name}")
            return

        if "error" in hiscore:
            player.possible_ban = 1
            player.confirmed_player = 0
            player = await self.scraper.lookup_runemetrics(player, session)
        else:
            player.possible_ban = 0
            player.confirmed_ban = 0
            player.label_jagex = 0
            player.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        if player is None:
            logger.warning(f"Player is None, Player_id: {hiscore.get('Player_id')}")
            return

        if player == "ClientHttpProxyError":
            logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
            self.manager.remove_worker(self)
            self.is_active = False
            return

        output = {
            "player": player.dict(),
            "hiscores": None if "error" in hiscore else hiscore,
        }
        await self.producer.send(topic="scraper", value=output)
