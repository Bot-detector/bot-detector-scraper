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


ERROR_TYPES = (
    ServerTimeoutError,
    ServerDisconnectedError,
    ClientConnectorError,
    ContentTypeError,
    ClientOSError,
    InvalidResponse,
    ClientHttpProxyError,
)


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
        logger.error(f"{self.name} - destroying worker")
        await asyncio.sleep(60)
        await self.session.close()
        await self.producer.stop()

    async def send_player(self, player: Player):
        await self.producer.send(topic="player", value=player.dict())
        await self.producer.flush()
        return

    def update_state(self, state: WorkerState) -> None:
        self.state = state
        return

    def is_broken(self) -> bool:
        return self.state == WorkerState.BROKEN

    def cleanup_tasks(self):
        for task in self.tasks:
            if task.done():
                self.tasks.remove(task)
        return
    
    def cancel_tasks(self):
        if not self.tasks:
            return
        
        for task in self.tasks:
            input = task.get_coro().cr_frame.f_locals["player"]
            asyncio.ensure_future(
                self.message_queue.put(input)
            )
            task.cancel()
        return
    
    async def run(self):
        while True:
            self.cleanup_tasks()

            if self.state == WorkerState.BROKEN:
                logger.error(f"{self.name} - breaking")
                self.cancel_tasks()
                break

            player: Player = await self.message_queue.get()

            async with self.semaphore:
                task = asyncio.ensure_future(self.scrape_player(player))
                self.tasks.append(task)
                # print(len(self.tasks), self.semaphore._value)

            # await self.scrape_player(player)
            self.message_queue.task_done()
            await asyncio.sleep(0.01)

        # await self.destroy()

    async def handle_errors(self, player: Player, error_type, sleep_time, error):
        logger.error(f"{self.name} - {error_type.__name__}: {str(error)}")
        logger.debug(
            f"{self.name} - invalid response, {player.name=} - {self.errors=}"
        )
        await self.send_player(player)
        await asyncio.sleep(sleep_time)

        if not self.is_broken():
            self.state = WorkerState.FREE

        self.errors += 1
        return

    async def scrape_player(self, player: Player):
        if self.is_broken():
            logger.warning("is broken")
            self.update_state(WorkerState.BROKEN)
            return

        if self.errors > 5:
            logger.error(f"{self.name} - to many errors, killing worker")
            self.update_state(WorkerState.BROKEN)
            await self.send_player(player)
            return

        hiscore = None
        self.state = WorkerState.WORKING

        try:
            # raise ServerTimeoutError("THIS IS FOR TASTING ^.^")
            player, hiscore = await self.scraper.lookup_hiscores(player, self.session)
        except ERROR_TYPES as error:
            error_type = type(error)
            sleep_time = 1 if error_type != ClientHttpProxyError else 5
            sleep_time = max(self.errors * 2, sleep_time)
            await self.handle_errors(player, error_type, sleep_time, error)
            return

        data = {"player": player.dict(), "hiscores": hiscore}
        asyncio.ensure_future(self.producer.send(topic="scraper", value=data))

        self.update_state(WorkerState.FREE)
        self.count_tasks += 1
        self.errors = 0
        return
