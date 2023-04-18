import logging
from typing import List
import json
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

    async def _split_data(self, data: list[dict], max_bytes: int) -> list[list[dict]]:
        # initialize the list of chunks to return
        chunks = []
        # initialize the current chunk being processed
        current_chunk = []
        # initialize the current size of the current chunk
        current_size = 0

        # loop through each item in the data list
        for item in data:
            # determine the size of the item
            item_size = len(json.dumps(item).encode('utf-8'))
            # if the size of the current chunk plus the size of the item exceeds the max bytes
            if current_size + item_size > max_bytes:
                # add the current chunk to the list of chunks
                chunks.append(current_chunk)
                # reset the current chunk and size
                current_chunk = []
                current_size = 0
            # add the item to the current chunk and update the current size
            current_chunk.append(item)
            current_size += item_size

        # if there are items left in the current chunk, add it to the list of chunks
        if current_chunk:
            chunks.append(current_chunk)

        # return the list of chunks
        return chunks


    async def get_players_to_scrape(self) -> list[dict]:
        """
        This method is used to get the players to scrape from the api.
        """
        url = f"{self.endpoint}/v1/scraper/players/0/{self.query_size}/{self.token}"
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

    async def post_scraped_players(self, data: list[dict]) -> list[dict]:
        """
        This method is used to post the scraped players to the api.
        """
        max_bytes = 5_000_000  # maximum payload size in bytes
        chunks = await self._split_data(data, max_bytes)
        for chunk in chunks:
            url = f"{self.endpoint}/v1/scraper/hiscores/{self.token}"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=chunk) as response:
                    if response.status != 200:
                        logger.error(f"response status {response.status}")
                        logger.error(f"response body: {await response.text()}")
                        raise Exception("error posting scraped players")
                    resp = await response.json()
            logger.info(f"posted {len(chunk)} players")
        return resp
