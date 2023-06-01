import asyncio
import logging
import time
from collections import deque

import config.config as config
from config.config import app_config
from modules.worker import NewWorker
from modules.validation.player import Player

logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, proxies: list[str]) -> None:
        self.proxies = proxies
        self.queue_players = deque([])
        self.queue_players_highscores = deque([])
        self.last_player_request = 0
        self.last_post_request = 0
        self.get_lock = False
        self.post_lock = False

    async def run(self, post_interval: int):
        logger.info("Running manager")
        self.post_interval = post_interval
        # start workers for each proxy
        tasks = list()
        for proxy in self.proxies:
            worker = NewWorker(proxy, self)
            tasks.append(
                asyncio.create_task(worker.run(timeout=app_config.SESSION_TIMEOUT))
            )
        await asyncio.gather(*tasks)

    async def get_players_task(self) -> bool:
        if not len(self.queue_players) < app_config.QUERY_SIZE:
            return False

        if self.get_lock:
            return False

        now = int(time.time())
        # check if it is time to make another request
        if self.last_player_request + 60 > now:
            return False

        self.last_player_request = now
        self.get_lock = True
        return True

    async def get_post_task(self) -> bool:
        delay = 10
        if not len(self.queue_players_highscores) > self.post_interval:
            return False

        if self.post_lock:
            return False

        now = int(time.time())
        # check if it is time to make another request
        if self.last_post_request + delay > now:
            return False

        self.last_post_request = now
        self.post_lock = True
        return True

    async def add_players(self, players: list[Player]):
        # add players to queue_players if not already in it
        count = 0
        queue = asyncio.Queue()
        # this did not like to be a pydantic class
        _ = [await queue.put(p.dict()) for p in players]

        while not queue.empty():
            _player = await queue.get()
            if _player in self.queue_players:
                continue
            self.queue_players.append(_player)
            count += 1

        logger.info(f"added {count}, total size: {len(self.queue_players)}")
        self.get_lock = False
        return

    async def add_highscores(self, highscore_data: dict):
        self.queue_players_highscores.append(highscore_data)
        if len(self.queue_players_highscores) % 100 == 0:
            logger.info(f"# scraped highscores: {len(self.queue_players_highscores)}")
        return

    async def get_player(self) -> Player:
        if len(self.queue_players) == 0:
            return None
        return self.queue_players.popleft()

    async def get_post_data(self) -> list[dict]:
        data = list(self.queue_players_highscores)
        self.queue_players_highscores = deque([])
        return data
