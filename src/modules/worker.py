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
        self.name = str(uuid.uuid4())
        self.state: WorkerState = WorkerState.FREE
        self.proxy: str = proxy

    async def initialize(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )
        await self.producer.start()
        self.scraper = Scraper(self.proxy)
        self.session = aiohttp.ClientSession(timeout=app_config.SESSION_TIMEOUT)
        return self

    async def destroy(self):
        await self.session.close()
        await self.producer.stop()

    async def scrape_player(self, player: Player):
        self.state = WorkerState.WORKING
        hiscore = None
        try:
            player, hiscore = await self.scraper.lookup_hiscores(player, self.session)
            player.possible_ban = 0
            player.confirmed_ban = 0
            player.label_jagex = 0
            player.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        except (
            ServerTimeoutError,
            ServerDisconnectedError,
            ClientConnectorError,
            ContentTypeError,
            ClientOSError,
        ) as e:
            logger.error(f"{e}")
            logger.warning(f"invalid response, from lookup_hiscores\n\t{player.dict()}")
            await self.producer.send(topic="player", value=player.dict())
            await asyncio.sleep(10)
            self.state = WorkerState.FREE
            return
        except ClientHttpProxyError:
            logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
            await self.producer.send(topic="player", value=player.dict())
            self.state = WorkerState.BROKEN
            return

        assert isinstance(
            player, Player
        ), f"expected the variable player to be of class Player, but got {player}"

        output = {"player": player.dict(), "hiscores": hiscore}
        await self.producer.send(topic="scraper", value=output)
        self.state = WorkerState.FREE
        return
