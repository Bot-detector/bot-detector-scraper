import asyncio
import json
import logging
import random
import uuid
from asyncio import Queue
from enum import Enum

import aiohttp
from aiohttp.client_exceptions import (
    ClientConnectorError,
    ClientHttpProxyError,
    ClientOSError,
    ContentTypeError,
    ServerDisconnectedError,
    ServerTimeoutError,
)
from aiokafka import AIOKafkaProducer

import config.config as config
from config.config import app_config
from modules.scraper import Scraper
from modules.validation.player import Player
from utils.http_exception_handler import InvalidResponse

logger = logging.getLogger(__name__)


class WorkerState(Enum):
    FREE = "free"
    WORKING = "working"
    BROKEN = "broken"


class Worker:
    def __init__(self, proxy: str, message_queue: Queue):
        self.name = str(uuid.uuid4())[-8:]
        self.state: WorkerState = WorkerState.FREE
        self.proxy: str = proxy
        self.message_queue = message_queue
        self.errors = 0
        self.count_tasks = 0

    async def initialize(self):
        await asyncio.sleep(random.randint(1, 10))
        logger.info(f"{self.name} - initializing worker")
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
        await self.producer.send(topic="player", value=player.dict())
        await self.producer.flush()
        return

    async def run(self):
        while True:
            if self.scraper.sleeping:
                await asyncio.sleep(1)
                continue

            player: Player = await self.message_queue.get()

            asyncio.ensure_future(self.scrape_player(player))
            # await self.scrape_player(player)
            self.message_queue.task_done()
            self.count_tasks += 1

    async def scrape_player(self, player: Player):
        self.state = WorkerState.WORKING
        hiscore = None

        if self.errors > 5:
            logger.error(f"{self.name} - to many errors, killing worker")
            await self.send_player(player)
            await asyncio.sleep(60)
            self.state = WorkerState.BROKEN
            return

        try:
            player, hiscore = await self.scraper.lookup_hiscores(player, self.session)
        except (
            ServerTimeoutError,
            ServerDisconnectedError,
            ClientConnectorError,
            ContentTypeError,
            ClientOSError,
            InvalidResponse,
        ) as e:
            logger.error(f"{self.name} - {str(e)}")
            logger.warning(
                f"{self.name} - invalid response, from lookup_hiscores {player.name=}"
            )
            await self.send_player(player)
            await asyncio.sleep(max(self.errors * 2, 1))
            self.state = WorkerState.FREE
            self.errors += 1
            return
        except ClientHttpProxyError:
            logger.warning(f"{self.name} - ClientHttpProxyError")
            await asyncio.sleep(max(self.errors * 2, 5))
            await self.send_player(player)
            self.errors += 1
            return

        err = f"{self.name} - expected the variable player to be of class Player,\n\t{player=}"
        assert isinstance(player, Player), err

        data = {"player": player.dict(), "hiscores": hiscore}
        asyncio.ensure_future(self.producer.send(topic="scraper", value=data))
        self.state = WorkerState.FREE
        self.errors = 0
        return
