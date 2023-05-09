
import uuid

from helpers.scraper import Scraper
import aiohttp
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

class NewWorker:
    def __init__(self, proxy, manager) -> None:
        self.scraper = Scraper(proxy)
        self.manager = manager
        self.name = str(uuid.uuid4())

    async def run(self, timeout: int):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                player: dict = self.manager.get_new_player()
                if player is None:
                    logger.info(f"worker={self.name} is idle.")
                    await asyncio.sleep(10)
                    continue

                hiscore = await self.scraper.lookup_hiscores(player, session)

                # data validation
                if hiscore is None:
                    logger.warning(f"Hiscore is empty for {player.get('name')}")
                    continue

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
                    player["updated_at"] = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.gmtime()
                    )

                # data validation
                if player is None:
                    logger.warning(
                        f"Player is None, Player_id: {hiscore.get('Player_id')}"
                    )
                    continue

                output = {
                    "player": player,
                    "hiscores": None if "error" in hiscore else hiscore,
                }
                self.manager.add_scraped_highscore(output)
