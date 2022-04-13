import logging
from typing import List

import aiohttp

logger = logging.getLogger(__name__)


class botDetectorApi:
    """
    This class is used to interact with the bot detector api.
    """

    def __init__(self, endpoint, query_size, token) -> None:
        self.endpoint = endpoint
        self.query_size = query_size
        self.token = token

    async def get_players_to_scrape(self) -> List[dict]:
        """
        This method is used to get the players to scrape from the api.
        """
        url = f"{self.endpoint}/scraper/players/0/{self.query_size}/{self.token}"
        logger.info("fetching players to scrape")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"response status {response.status}")
                    logger.error(f"response body: {await response.text()}")
                    raise Exception("error fetching players")
                players = await response.json()
        logger.info(f"fetched {len(players)} players")
        return players

    async def post_scraped_players(self, data: List[dict]) -> List[dict]:
        """
        This method is used to post the scraped players to the api.
        """
        url = f"{self.endpoint}/scraper/hiscores/{self.token}"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data) as response:
                if response.status != 200:
                    logger.error(f"response status {response.status}")
                    logger.error(f"response body: {await response.text()}")
                    raise Exception("error posting scraped players")
                data = await response.json()
        logger.info(f"posted {len(data)} players")
        return data
