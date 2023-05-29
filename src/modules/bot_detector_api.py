import logging
import json
import aiohttp
import uuid
from utils.timer import timer
import asyncio

logger = logging.getLogger(__name__)


class botDetectorApi:
    """
    This class is used to interact with the bot detector api.
    """

    def __init__(
        self, endpoint: str, query_size: int, token: str, max_bytes: int
    ) -> None:
        self.endpoint = endpoint
        self.query_size = query_size
        self.token = token
        self.max_bytes = max_bytes

    @timer
    async def _split_data(self, data: list[dict]) -> list[list[dict]]:
        # initialize the list of chunks to return
        chunks = []
        # initialize the current chunk being processed
        current_chunk = []
        # initialize the current size of the current chunk
        current_size = 0

        # loop through each item in the data list
        for item in data:
            # determine the size of the item
            item_size = len(json.dumps(item).encode("utf-8"))
            # if the size of the current chunk plus the size of the item exceeds the max bytes
            if current_size + item_size > self.max_bytes:
                # add the current chunk to the list of chunks
                chunks.append(current_chunk)

                # logger.info(f"chunksize: {current_size}")

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

    @timer
    async def get_players_to_scrape(self) -> list[dict]:
        """
        This method is used to get the players to scrape from the api.
        """
        url = f"{self.endpoint}/v1/scraper/players/0/{self.query_size}/{self.token}"
        logger.info("fetching players to scrape")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(
                        f"response status {response.status}"
                        f"response body: {await response.text()}"
                    )
                    raise Exception("error fetching players")
                players = await response.json()
        logger.info(f"fetched {len(players)} players")
        return players

    @timer
    async def post_scraped_players(self, data: list[dict]) -> None:
        """
        This method is used to post the scraped players to the api.
        """
        uuid_string = str(uuid.uuid4())
        chunks = await self._split_data(data)
        logger.info(f"having {len(chunks)} chunks, {uuid_string=}")
        async with aiohttp.ClientSession() as session:
            tasks = list()
            for chunk in chunks:
                tasks.append(
                    asyncio.create_task(
                        self._post_scraped_players(session, chunk, uuid_string)
                    )
                )
            await asyncio.gather(*tasks)
        return

    @timer
    async def _post_scraped_players(
        self, session: aiohttp.ClientSession, chunk, uuid_string
    ):
        logger.info(f"rows in chunk: {len(chunk)}, {uuid_string=}")
        url = f"{self.endpoint}/v1/scraper/hiscores/{self.token}"
        async with session.post(url, json=chunk) as response:
            if response.status != 200:
                logger.error(
                    f"response status {response.status}\n"
                    f"response body: {await response.text()}"
                )
                raise Exception("error posting scraped players")
        logger.info(f"posted {len(chunk)} players, {uuid_string=}")
        return
