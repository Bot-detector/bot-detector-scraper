import asyncio
import copy
import logging
import sys
import time
from collections import deque
from typing import List

import aiohttp

import config
from helpers.Api import botDetectorApi
from helpers.job import Job
from helpers.Scraper import Scraper

logger = logging.getLogger(__name__)

# global variables
jobs = deque([Job("get_players_to_scrape")])
results = []
last_player_request = 0
last_post_request = 0


class Worker:
    def __init__(self, proxy) -> None:
        self.api = botDetectorApi(config.ENDPOINT, config.QUERY_SIZE, config.TOKEN, config.MAX_BYTES)
        self.scraper = Scraper(proxy)

    async def work(self):
        global jobs
        global results
        async with aiohttp.ClientSession(timeout=config.SESSION_TIMEOUT) as session:
            while True:
                current_time = int(time.time())

                # we got many results
                if (
                    len(results) > config.POST_INTERVAL
                    and last_post_request + 60 < current_time
                ):
                    if last_post_request + 60 < current_time:
                        jobs.appendleft(Job("post_scraped_players"))
                    else:
                        sleep = last_post_request + 60 - current_time
                        logger.info(f"Waiting to post players: {sleep} seconds")
                        await asyncio.sleep(sleep)
                        continue
                # we do not have enough players to scrape
                elif len(jobs) < config.POST_INTERVAL:
                    if last_player_request + 60 < current_time:
                        jobs.appendleft(Job("get_players_to_scrape"))
                    else:
                        sleep = last_player_request + 60 - current_time
                        logger.info(f"Waiting to get new players: {sleep} seconds")
                        await asyncio.sleep(sleep)
                        continue

                # take the first job
                job = jobs.popleft()

                match job.name:
                    case "get_players_to_scrape":
                        await self._get_players_to_scrape()
                    case "post_scraped_players":
                        await self._post_scraped_players(job)
                    case "process_hiscore":
                        if job.data:
                            await self._process_hiscore(job, session)

    async def _get_players_to_scrape(self) -> None:
        global jobs
        global last_player_request

        last_player_request = int(time.time())

        try:
            players = await self.api.get_players_to_scrape()
        except Exception as e:
            logger.error(e)
            await asyncio.sleep(60)
            await self._get_players_to_scrape()
            return

        _job = "process_hiscore"
        _ = [jobs.append(Job(_job, [player])) for player in players]
        return

    async def _post_scraped_players(self, job: Job) -> None:
        global jobs
        global results
        global last_post_request

        last_post_request = int(time.time())
        # copy the results
        job.data = copy.deepcopy(results)

        # posting data to api
        try:
            await self.api.post_scraped_players(job.data)
        except Exception as e:
            logger.error(e)
            await asyncio.sleep(60)
            await self._post_scraped_players(job)
            return
        results = []
        return

    async def _process_hiscore(self, job: Job, session: aiohttp.ClientSession) -> None:
        global results
        player = job.data[0]
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
        results.append(output.copy())


async def fetch_proxy_list(session: aiohttp.ClientSession) -> str:
    """
    Fetch the proxy list from webshare.io and return the response body as string.
    """
    async with session.get(config.PROXY_DOWNLOAD_URL) as response:
        if response.status != 200:
            logger.error(f"response status {response.status}")
            logger.error(f"response body: {await response.text()}")
            raise Exception("error fetching proxy list")
        return await response.text()


def parse_proxy_list(proxies_str: str) -> List[str]:
    """
    Parse the proxy list string and return a list of formatted proxy URLs.
    """
    proxies = proxies_str.splitlines()
    proxies = [proxy.split(":") for proxy in proxies]
    proxies = [
        f"http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}" for proxy in proxies
    ]
    logger.info(f"fetched {len(proxies)} proxies")
    return proxies


async def get_proxy_list() -> List[str]:
    """
    Return a list of formatted proxy URLs from webshare.io.
    """
    logger.info("fetching proxy list from webshare.io")
    async with aiohttp.ClientSession(trust_env=True) as session:
        try:
            proxies_str = await fetch_proxy_list(session)
            proxies = parse_proxy_list(proxies_str)
        except Exception as e:
            logger.error(str(e))
            raise Exception("error fetching proxy list")
    return proxies


async def main():
    """
    This function is the main function of the program.
    It creates a list of proxies and then creates a worker for each proxy.
    """
    logger.info(f"inserting in batches of {config.POST_INTERVAL}")
    proxies = await get_proxy_list()
    workers = [asyncio.create_task(Worker(proxy).work()) for proxy in proxies]
    await asyncio.gather(*workers)


if __name__ == "__main__":
    # from https://stackoverflow.com/questions/63347818/aiohttp-client-exceptions-clientconnectorerror-cannot-connect-to-host-stackover
    if (
        sys.platform.startswith("win")
        and sys.version_info[0] == 3
        and sys.version_info[1] >= 8
    ):
        logger.info("Set policy")
        policy = asyncio.WindowsSelectorEventLoopPolicy()
        asyncio.set_event_loop_policy(policy)
    asyncio.run(main())
