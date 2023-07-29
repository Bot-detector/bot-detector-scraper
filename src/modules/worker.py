import asyncio
import json
import logging
import time
import uuid
from enum import Enum

import aiohttp
from aiohttp.client_exceptions import ClientHttpProxyError
from aiokafka import AIOKafkaProducer

import config.config as config
from config.config import app_config
from modules.scraper import Scraper
from modules.validation.player import Player
from aiohttp.client_exceptions import (
    ServerTimeoutError,
    ServerDisconnectedError,
    ClientConnectorError,
    ContentTypeError,
    ClientOSError,
)
logger = logging.getLogger(__name__)


class WorkerState(Enum):
    FREE = "free"
    WORKING = "working"
    BROKEN = "broken"


class Worker:
    def __init__(self, proxy: str):
        self.name = str(uuid.uuid4())[-8:]
        self.state: WorkerState = WorkerState.FREE
        self.proxy: str = proxy
        self.errors = 0

    async def initialize(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )
        await self.producer.start()
        self.scraper = Scraper(proxy=self.proxy, worker_name=self.name)
        self.session = aiohttp.ClientSession(timeout=app_config.SESSION_TIMEOUT)
        return self

    async def destroy(self):
        await self.session.close()
        await self.producer.stop()

    async def send_player(self, player: Player):
        producer = AIOKafkaProducer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )
        await producer.start()
        await producer.send(topic="player", value=player.dict())
        await producer.stop()
        return
    
    async def scrape_player(self, player: Player):
        self.state = WorkerState.WORKING
        hiscore = None

        if self.errors > 5:
            logger.error(f"{self.name} - to many errors, killing worker")
            self.state = WorkerState.BROKEN

        try:
            player, hiscore = await self.scraper.lookup_hiscores(player, self.session)
        except (
            ServerTimeoutError,
            ServerDisconnectedError,
            ClientConnectorError,
            ContentTypeError,
            ClientOSError,
        ) as e:
            logger.error(f"{e}")
            logger.warning(f"{self.name} - invalid response, from lookup_hiscores\n\t{player.dict()}")
            await self.send_player(player)
            await asyncio.sleep(10)
            self.state = WorkerState.FREE
            self.errors += 1
            return
        except ClientHttpProxyError:
            logger.warning(f"{self.name} - ClientHttpProxyError killing worker")
            await self.send_player(player)
            self.state = WorkerState.BROKEN
            self.errors += 1
            return

        assert isinstance(
            player, Player
        ), f"{self.name} - expected the variable player to be of class Player,\n\t{player=}"

        output = {"player": player.dict(), "hiscores": hiscore}
        asyncio.ensure_future(self.producer.send(topic="scraper", value=output))
        self.state = WorkerState.FREE
        self.errors = 0
        return
