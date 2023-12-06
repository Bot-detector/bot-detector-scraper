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
