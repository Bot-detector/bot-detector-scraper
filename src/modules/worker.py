import asyncio
import logging
import time
import uuid
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import ClientSession

import config.config as config
from config.config import app_config
from modules.bot_detector_api import botDetectorApi
from modules.scraper import Scraper
from modules.validation.player import Player

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from modules.manager import Manager


class NewWorker:
    def __init__(self, proxy, manager) -> None:
        self.scraper: Scraper = Scraper(proxy)
        self.manager: Manager = manager
        self.name: str = str(uuid.uuid4())
        self.api: botDetectorApi = botDetectorApi(
            app_config.ENDPOINT,
            app_config.QUERY_SIZE,
            app_config.TOKEN,
            app_config.MAX_BYTES,
        )
        self.active: bool = True

    async def run(self, timeout: int):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while self.active:
                if await self.manager.get_players_task():
                    await self._get_data()
                elif await self.manager.get_post_task():
                    data = await self.manager.get_post_data()
                    await self._post_data(data)
                    self.manager.post_lock = False
                else:
                    player = await self.manager.get_player()
                    
                    if player is None:
                        logger.info(f"worker={self.name} is idle.")
                        await asyncio.sleep(60)
                    else:
                        await self._scrape_data(session, Player(**player))
                await asyncio.sleep(1)

    async def _scrape_data(self, session: ClientSession, player: Player):
        hiscore = await self.scraper.lookup_hiscores(player, session)

        if hiscore == "ClientHttpProxyError":
            logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
            self.active = False
            return

        if hiscore is None:
            logger.warning(f"Hiscore is empty for {player.name}")
            return

        if "error" in hiscore:
            player.possible_ban = 1
            player.confirmed_player = 0
            player = await self.scraper.lookup_runemetrics(player, session)
        else:
            player.possible_ban = 0
            player.confirmed_ban = 0
            player.label_jagex = 0
            player.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        if player is None:
            logger.warning(f"Player is None, Player_id: {hiscore.get('Player_id')}")
            return

        if player == "ClientHttpProxyError":
            logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
            self.active = False
            return
        
        output = {
            "player": player.dict(),
            "hiscores": None if "error" in hiscore else hiscore,
        }
        await self.manager.add_highscores(output)

    async def _post_data(self, data):
        try:
            logger.info(f"worker={self.name} post scraped players")
            await self.api.post_scraped_players(data)
        except Exception as e:
            logger.error(str(e))
            await asyncio.sleep(60)

    async def _get_data(self):
        try:
            logger.info(f"worker={self.name} get players to scrape")
            players = await self.api.get_players_to_scrape()
            await self.manager.add_players(players)
        except Exception as e:
            logger.error(str(e))
            await asyncio.sleep(60)
