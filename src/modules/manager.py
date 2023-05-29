import asyncio
import logging
import time
from collections import deque

import config.config as config
from config.config import app_config
from modules.worker import NewWorker

logger = logging.getLogger(__name__)


class Manager:
    # initialize queues, request times and bot detector api
    queue_players = deque([])
    queue_players_highscores = deque([])
    last_player_request = 0
    last_post_request = 0
    get_lock = False
    post_lock = False

    def __init__(self, proxies: list[str]) -> None:
        # initialize proxies
        self.proxies = proxies

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

    async def add_players(self, players: list[dict]):
        # add players to queue_players if not already in it
        _players = [p for p in players if p not in self.queue_players]

        _ = [self.queue_players.append(p) for p in _players]
        logger.info(f"added {len(_players)}, total size: {len(self.queue_players)}")
        self.get_lock = False
        return

    async def add_highscores(self, highscore_data: dict):
        self.queue_players_highscores.append(highscore_data)
        if len(self.queue_players_highscores) % 100 == 0:
            logger.info(f"# scraped highscores: {len(self.queue_players_highscores)}")
        return

    async def get_player(self) -> dict:
        if len(self.queue_players) == 0:
            return None
        return self.queue_players.popleft()

    async def get_post_data(self) -> list[dict]:
        data = list(self.queue_players_highscores)
        self.queue_players_highscores.clear()
        return data

    async def remove_post_data(self, data: list[dict]):
        _ = [self.queue_players_highscores.popleft() for _ in data]
        self.post_lock = False
        return
