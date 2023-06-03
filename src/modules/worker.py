import json
import logging
import time
import uuid
from typing import TYPE_CHECKING
import asyncio
import aiohttp
from aiohttp import ClientSession
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

import config.config as config
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

        self.manager: Manager = manager

    async def initialize(self):
        self.consumer = AIOKafkaConsumer(
            "player",  # Topic to consume from
            bootstrap_servers="localhost:9094",  # Kafka broker address
            # group_id=f"scraper-group-{self.name}",  # Consumer group ID
            client_id=self.name,
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9094",  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )

        # Wait for the consumer and producer to connect
        await self.consumer.start()
        await self.producer.start()

        self.scraper = Scraper(self.proxy)

    async def run(self, timeout: int):
        # Call initialize method before running
        await self.initialize()

        assert isinstance(
            self.consumer, AIOKafkaConsumer
        ), f"consumer myst be of type AIOKafkaConsumer, received: {type(self.consumer)}.s"

        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async for msg in self.consumer:
                    # Commit the consumed message to mark it as processed
                    tp = TopicPartition(msg.topic, msg.partition)
                    await self.consumer.commit({tp: msg.offset + 1})

                    # Extract the player from the message
                    player = msg.value.decode()
                    player = json.loads(player)

                    # Scrape the player and produce the result
                    await self.scrape_data(session, Player(**player))
            finally:
                await self.consumer.stop()
                await self.producer.stop()

    async def scrape_data(self, session: ClientSession, player: Player):
        hiscore = await self.scraper.lookup_hiscores(player, session)

        if hiscore == "ClientHttpProxyError":
            logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
            self.manager.remove_worker(self)
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
            return

        output = {
            "player": player.dict(),
            "hiscores": None if "error" in hiscore else hiscore,
        }
        await self.producer.send(topic="scraper", value=output)

