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
        self.tasks = []
        self.semaphore = asyncio.Semaphore(value=5)

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
        logger.error(f"{self.name} - destroying")
        await asyncio.sleep(60)
        await self.session.close()
        await self.producer.stop()

    async def send_player(self, player: Player):
        await self.producer.send(topic="player", value=player.dict())
        await self.producer.flush()
        return

    async def run(self):
        buffer = []
        while True:
            for task in self.tasks:
                if task.done():
                    self.tasks.remove(task)
            
            if len(self.tasks) > 5 or self.errors > 5 or self.scraper.sleeping:
                await asyncio.sleep(1)
                continue

            if self.state == WorkerState.BROKEN:
                logger.error(f"{self.name} - breaking")
                for task in self.tasks:
                    inputs = task.get_coro().cr_frame.f_locals['player']
                    buffer.append(inputs)
                    await self.message_queue.put(inputs)
                    task.cancel()
                break
            
            if buffer:
                player: Player = buffer.pop()
            else:
                player: Player = await self.message_queue.get()

            async with self.semaphore:
                task = asyncio.ensure_future(self.scrape_player(player))
                self.tasks.append(task)
                # print(len(self.tasks), self.semaphore._value)
    
            # await self.scrape_player(player)
            self.message_queue.task_done()

        # await self.destroy()

    async def scrape_player(self, player: Player):
        if self.errors > 5 or self.state == WorkerState.BROKEN:
            logger.error(f"{self.name} - to many errors, killing worker")
            self.state = WorkerState.BROKEN
            await self.send_player(player)
            return

        hiscore = None
        self.state = WorkerState.WORKING

        try:
            # raise ServerTimeoutError("THIS IS FOR TASTING ^.^")
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
                f"{self.name} - invalid response, {player.name=} - {self.errors=}"
            )
            await self.send_player(player)
            await asyncio.sleep(max(self.errors * 2, 1))

            if self.state != WorkerState.BROKEN:
                self.state = WorkerState.FREE

            self.errors += 1
            return
        except ClientHttpProxyError:
            logger.warning(f"{self.name} - ClientHttpProxyError - {self.errors=}")
            await asyncio.sleep(max(self.errors * 2, 5))
            await self.send_player(player)

            if self.state != WorkerState.BROKEN:
                self.state = WorkerState.FREE

            self.errors += 1
            return

        err = f"{self.name} - expected class Player, Received: {player=}"
        assert isinstance(player, Player), err

        data = {"player": player.dict(), "hiscores": hiscore}
        asyncio.ensure_future(self.producer.send(topic="scraper", value=data))

        self.state = WorkerState.FREE
        self.count_tasks += 1
        self.errors = 0
        return
