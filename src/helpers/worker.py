import uuid

from helpers.scraper import Scraper
import aiohttp
import asyncio
import logging
import time
from helpers.api import botDetectorApi
import config
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from helpers.manager import Manager


class NewWorker:
    def __init__(self, proxy, manager) -> None:
        self.scraper = Scraper(proxy)
        self.manager: Manager = manager
        self.name = str(uuid.uuid4())
        self.api = botDetectorApi(
            config.ENDPOINT, config.QUERY_SIZE, config.TOKEN, config.MAX_BYTES
        )

    async def run(self, timeout: int):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                # check if it has to get new players
                # check if it has to post players
                # scrape players
                if await self.manager.get_players_task():
                    # asyncio.create_task(self._get_data())
                    await self._get_data()
                elif await self.manager.get_post_task():
                    data = await self.manager.get_post_data()
                    await self._post_data(data)
                    # asyncio.create_task(self._post_data(data))
                else:
                    player: dict = await self.manager.get_player()
                    if player is None:
                        logger.info(f"worker={self.name} is idle.")
                        await asyncio.sleep(60)
                    else:
                        scraped_data = await self._scrape_data(session, player)
                        if scraped_data is not None:
                            await self.manager.add_highscores(scraped_data)
                await asyncio.sleep(1)

    async def _scrape_data(self, session: aiohttp.ClientSession, player: dict):
        hiscore = await self.scraper.lookup_hiscores(player, session)

        # data validation
        if hiscore is None:
            logger.warning(f"Hiscore is empty for {player.get('name')}")
            return

        # player is not on the hiscores
        if "error" in hiscore:
            # update additional metadata
            player["possible_ban"] = 1
            player["confirmed_player"] = 0
            player = await self.scraper.lookup_runemetrics(player, session)
        else:
            # update additional metadata
            player["possible_ban"] = 0
            player["confirmed_ban"] = 0
            player["label_jagex"] = 0
            player["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        # data validation
        if player is None:
            logger.warning(f"Player is None, Player_id: {hiscore.get('Player_id')}")
            return

        output = {
            "player": player,
            "hiscores": None if "error" in hiscore else hiscore,
        }
        return output

    async def _post_data(self, data):
        try:
            logger.info(f"worker={self.name} post scraped players")
            # get a copy of the highscores data to be posted to bot detector API
            # post the data to bot detector API
            await self.api.post_scraped_players(data)
        except Exception as e:
            logger.error(f"{str(e)}")
            # wait for 60 seconds and try again
            await asyncio.sleep(60)
        return

    async def _get_data(self):
        try:
            logger.info(f"worker={self.name} get players to scrape")
            # get players from bot detector API
            players = await self.api.get_players_to_scrape()
            await self.manager.add_players(players)
        except Exception as e:
            logger.error(f"{str(e)}")
            # wait for 60 seconds and try again
            await asyncio.sleep(60)
            return
        return
