import asyncio
import logging
import time
from collections import deque
from typing import Union

from aiohttp import ClientSession

from modules.api.highscore_api import HighscoreApi
from modules.api.runemetrics_api import RuneMetricsApi
from modules.validation.player import Player, PlayerDoesNotExistException

logger = logging.getLogger(__name__)


class Scraper:
    def __init__(
        self, proxy: str, worker_name: str, calls_per_minute: int = 60
    ) -> None:
        self.proxy = proxy
        self.worker_name = worker_name
        self.history = deque(maxlen=calls_per_minute)
        self.highscore_api = HighscoreApi(proxy=proxy)
        self.runemetrics_api = RuneMetricsApi(proxy)
        self.sleeping = False

    async def rate_limit(self):
        """
        Rate limits the scraper to 60 calls a minute.
        """
        self.history.append(int(time.time()))
        maxlen = self.history.maxlen
        MINUTE = 60
        if not len(self.history) == maxlen:
            return

        head = self.history[0]
        tail = self.history[-1]
        span = tail - head

        if span < MINUTE:
            sleep = MINUTE - span
            if sleep % 10 == 0:
                logger.warning(
                    f"{self.worker_name} - Rate limit reached, sleeping {sleep} seconds"
                )
            self.sleeping = True
            await asyncio.sleep(sleep)
            self.sleeping = False
        return

    async def lookup(
        self, player: Player, session: ClientSession
    ) -> Union[Player, dict]:
        ...

    # async def lookup(
    #     self, player: Player, session: ClientSession
    # ) -> Union[Player, dict]:
    #     await self.rate_limit()
    #     highscore = None
    #     try:
    #         highscore = await self.highscore_api.lookup_hiscores(
    #             player=player, session=session
    #         )
    #         player.possible_ban = 0
    #         player.confirmed_ban = 0
    #         player.label_jagex = 0
    #     except PlayerDoesNotExistException:
    #         player.possible_ban = 1
    #         player.confirmed_player = 0
    #         player = await self.runemetrics_api.lookup_runemetrics(
    #             player=player, session=session
    #         )

    #     player.updated_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    #     return player, highscore

    # async def lookup_hiscores(
    #     self, player: Player, session: ClientSession
    # ) -> Union[Player, dict]:
    #     await self.rate_limit()
    #     highscore = None
    #     try:
    #         highscore = await self.highscore_api.lookup_hiscores(
    #             player=player, session=session
    #         )
    #         player.possible_ban = 0
    #         player.confirmed_ban = 0
    #         player.label_jagex = 0
    #     except PlayerDoesNotExistException:
    #         logger.warn(msg=f"{player.name} does not exist")

    #     player.updated_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    #     return player, highscore

    # async def lookup_runemetrics(self, player: Player, session: ClientSession) -> dict:
    #     await self.rate_limit()
    #     return await self.runemetrics_api.lookup_runemetrics(
    #         player=player, session=session
    #     )


class HighScoreScraper(Scraper):
    def __init__(
        self, proxy: str, worker_name: str, calls_per_minute: int = 60
    ) -> None:
        super().__init__(proxy, worker_name, calls_per_minute)
        self.highscore_api = HighscoreApi(proxy=proxy)

    async def lookup(
        self, player: Player, session: ClientSession
    ) -> Union[Player, dict]:
        await self.rate_limit()
        highscore = None
        try:
            highscore = await self.highscore_api.lookup_hiscores(
                player=player, session=session
            )
            player.possible_ban = 0
            player.confirmed_ban = 0
            player.label_jagex = 0
        except PlayerDoesNotExistException:
            player.possible_ban = 1
            player.confirmed_player = 0

        player.updated_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        return player, highscore


class RuneMetricsScraper(Scraper):
    def __init__(
        self, proxy: str, worker_name: str, calls_per_minute: int = 60
    ) -> None:
        super().__init__(proxy, worker_name, calls_per_minute)
        self.runemetrics_api = RuneMetricsApi(proxy)

    async def lookup(self, player: Player, session: ClientSession) -> dict:
        await self.rate_limit()
        return await self.runemetrics_api.lookup_runemetrics(
            player=player, session=session
        )
