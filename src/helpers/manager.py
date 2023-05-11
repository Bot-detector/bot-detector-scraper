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
    queue_players = deque()
    queue_players_highscores = deque()
    last_player_request = 0
    last_post_request = 0
    api = botDetectorApi(
        config.ENDPOINT, config.QUERY_SIZE, config.TOKEN, config.MAX_BYTES
    )

    def __init__(self, proxies: list[str]) -> None:
        # initialize proxies
        self.proxies = proxies

    async def run(self, post_interval: int):
        logger.info("Running manager")
        # start workers for each proxy
        tasks = list()
        for proxy in self.proxies:
            worker = NewWorker(proxy, self)
            tasks.append(
                asyncio.create_task(worker.run(timeout=config.SESSION_TIMEOUT))
            )
        asyncio.gather(*tasks)

        while True:
            # if queue_players has less items than the post_interval
            if len(self.queue_players) < config.QUERY_SIZE:
                now = int(time.time())
                # check if it is time to make another request
                if self.last_player_request + 60 > now:
                    continue
                self.last_player_request = now
                asyncio.create_task(self._get_players_to_scrape())
            # if queue_players_highscores has more items than the post_interval
            elif len(self.queue_players_highscores) > post_interval:
                now = int(time.time())
                # check if it is time to make another request
                if self.last_post_request + 60 > now:
                    continue
                self.last_post_request = now
                # post scraped players to bot detector API
                asyncio.create_task(self._post_scraped_players())

            await asyncio.sleep(10)
    @timer
    async def _get_players_to_scrape(self) -> list[dict]:
        try:
            logger.info("get players to scrape")
            # get players from bot detector API
            players = await self.api.get_players_to_scrape()
        except Exception as e:
            logger.error(e)
            # wait for 60 seconds and try again
            await asyncio.sleep(60)
            return
        
        if players is None:
            await asyncio.sleep(60)
            return
        
        # add players to queue_players if not already in it
        _players = [p for p in players if p not in self.queue_players]
        _ = [self.queue_players.append(p) for p in _players]
        logger.info(
            f"added {len(_players)}, total size: {len(self.queue_players)}"
        )
        return players
    
    @timer
    async def _post_scraped_players(self) -> None:
        try:
            logger.info("post scraped players")
            # get a copy of the highscores data to be posted to bot detector API
            data = list(self.queue_players_highscores).copy()
            # post the data to bot detector API
            await self.api.post_scraped_players(data)
            # remove the players from queue_players that were successfully posted
            _ = [self.queue_players.popleft() for _ in data]
        except Exception as e:
            logger.error(e)
            # wait for 60 seconds and try again
            await asyncio.sleep(60)
            await self._post_scraped_players()
            return
        return

    def add_scraped_highscore(self, highscore_data: dict):
        # add highscore data to queue_players_highscores
        self.queue_players_highscores.append(highscore_data)
        return

    def get_new_player(self) -> dict:
        if len(self.queue_players) == 0:
            return None
        # remove and return the first player from queue_players
        return self.queue_players.popleft()
