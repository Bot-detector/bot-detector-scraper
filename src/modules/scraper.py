import asyncio
import logging
import time
from collections import deque
from aiohttp import ClientSession
from modules.validation.player import Player, PlayerDoesNotExistException
from modules.api.highscore_api import HighscoreApi
from modules.api.runemetrics_api import RuneMetricsApi
from typing import Union

logger = logging.getLogger(__name__)

class Scraper:
    def __init__(self, proxy: str, worker_name:str, calls_per_minute: int = 60) -> None:
        self.proxy = proxy
        self.worker_name = worker_name
        self.history = deque(maxlen=calls_per_minute)
        self.highscore_api = HighscoreApi(proxy=proxy)
        self.runemetrics_api = RuneMetricsApi(proxy)

    async def rate_limit(self):
        """
        Rate limits the scraper to 60 calls a minute.
        """
        self.history.append(int(time.time()))
        maxlen = self.history.maxlen
        if len(self.history) == maxlen:
            head = self.history[0]
            tail = self.history[-1]
            span = tail - head
            if span < 60:
                sleep = 60 - span
                if sleep % 10 == 0:
                    logger.warning(f"{self.worker_name} - Rate limit reached, sleeping {sleep} seconds")
                await asyncio.sleep(sleep)
        return

    async def lookup_hiscores(self, player: Player, session: ClientSession) -> Union[Player, dict]:
        await self.rate_limit()
        highscore = None
        try:
            highscore = await self.highscore_api.lookup_hiscores(player=player, session=session)
            player.possible_ban = 0
            player.confirmed_ban = 0
            player.label_jagex = 0
        except PlayerDoesNotExistException:
            player.possible_ban = 1
            player.confirmed_player = 0
            player = await self.runemetrics_api.lookup_runemetrics(player=player, session=session)


        player.updated_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        return player, highscore

    async def lookup_runemetrics(self, player: Player, session: ClientSession) -> dict:
        return await self.runemetrics_api.lookup_runemetrics(player=player, session=session)
