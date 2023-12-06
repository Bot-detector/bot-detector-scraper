import asyncio
import logging
import time
from collections import deque
from typing import Union

from aiohttp import ClientSession

from modules.api.highscore_api import HighscoreApi
from modules.scraper import Scraper
from modules.validation.player import Player, PlayerDoesNotExistException

logger = logging.getLogger(__name__)


class HighScoreScraper(Scraper):
    def __init__(
        self, proxy: str, worker_name: str, calls_per_minute: int = 60
    ) -> None:
        super().__init__(proxy, worker_name, calls_per_minute)
        self.highscore_api = HighscoreApi(proxy=proxy)

    async def lookup_hiscores(
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
