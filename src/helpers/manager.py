import asyncio
import logging
import time
from collections import deque

import config
from helpers.api import botDetectorApi
from helpers.worker import NewWorker
from helpers.timer import timer

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
                asyncio.create_task(worker.run(timeout=config.SESSION_TIMEOUT))
            )
        await asyncio.gather(*tasks)
    
    async def get_players_task(self) -> bool:
        if not len(self.queue_players) < config.QUERY_SIZE:
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
        if not len(self.queue_players_highscores) > self.post_interval:
            return False
        
        if self.post_lock:
            return False
        
        now = int(time.time())
        # check if it is time to make another request
        if self.last_post_request + 60 > now:
            return False

        self.last_post_request = now
        self.post_lock = True
        return True
    
    async def add_players(self, players: list[dict]):
        # add players to queue_players if not already in it
        _players = [p for p in players if p not in self.queue_players]

        _ = [self.queue_players.append(p) for p in _players]
        logger.info(
            f"added {len(_players)}, total size: {len(self.queue_players)}"
        )
        self.get_lock = False
        return

    async def add_highscores(self, highscore_data: dict):
        self.queue_players_highscores.append(highscore_data)
        logger.info(f"{len(self.queue_players_highscores)=}")
        return

    async def get_player(self) -> dict:
        if len(self.queue_players) == 0:
            return None
        return self.queue_players.popleft()

    async def get_post_data(self) -> list[dict]:
        return list(self.queue_players_highscores).copy()
    
    async def remove_post_data(self, data:list[dict]):
        _ = [self.queue_players_highscores.popleft() for _ in data]
        self.post_lock = False
        return