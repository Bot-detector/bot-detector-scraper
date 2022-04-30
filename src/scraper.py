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


class Worker:
    def __init__(self, proxy) -> None:
        self.api = botDetectorApi(config.ENDPOINT, config.QUERY_SIZE, config.TOKEN)
        self.scraper = Scraper(proxy)

    async def work(self):
        global jobs
        global results
        async with aiohttp.ClientSession(timeout=config.SESSION_TIMEOUT) as session:
            while True:
                if len(jobs) == 0:
                    await asyncio.sleep(1)
                    continue

                # take the first job
                job = jobs.popleft()

                if job.name == "get_players_to_scrape":
                    await self.__get_players_to_scrape(config.POST_INTERVAL)
                elif job.name == "post_scraped_players":
                    await self.__post_scraped_players(job)
                elif job.name == "process_hiscore" and job.data:
                    await self.__process_hiscore(job, session)

    async def __get_players_to_scrape(self, POST_INTERVAL: int) -> None:
        global jobs
        # get_players_to_scrape
        try:
            players = await self.api.get_players_to_scrape()
            if len(players) < 1000:
                await asyncio.sleep(300)
        except Exception as e:
            logger.error(e)
            await asyncio.sleep(5)
            await self.__get_players_to_scrape(POST_INTERVAL)
            return
        # for each player create a job to process the hiscore
        for i, player in enumerate(players):
            jobs.append(Job("process_hiscore", [player]))
            # add a job to post the scraped data to the api
            if i > 0 and i % POST_INTERVAL == 0:
                jobs.append(Job("post_scraped_players"))
        # add a job midway through the process hiscore jobs to get players to scrape
        if len(jobs) < 2 * int(config.QUERY_SIZE):
            jobs.insert(int(len(jobs) / 2), Job("get_players_to_scrape"))
        # add a post job at the end
        jobs.append(Job("post_scraped_players"))
        logger.debug(f"Length of jobs: {len(jobs)}")
        return

    async def __post_scraped_players(self, job: Job) -> None:
        global jobs
        global results
        # copy the results
        job.data = copy.deepcopy(results)
        # posting data to api
        try:
            await self.api.post_scraped_players(job.data)
        except Exception as e:
            logger.error(e)
            await asyncio.sleep(1)
            await self.__post_scraped_players(job)
            return
        results = []
        # add a job to get players to scrape
        jobs.append(Job("get_players_to_scrape"))
        return

    async def __process_hiscore(self, job: Job, session:aiohttp.ClientSession) -> None:
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

            # data validation
            if player is None:
                logger.warning(f"Player is None, Player_id: {hiscore.get('Player_id')}")
                return

            output = {}
            output["player"] = player
            output["hiscores"] = None
        else:
            # update additional metadata
            player["possible_ban"] = 0
            player["confirmed_ban"] = 0
            player["label_jagex"] = 0
            player["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

            output = {}
            output["player"] = player
            output["hiscores"] = hiscore
        results.append(output.copy())


async def get_proxy_list() -> List:
    """
    returns the proxy list from webshare.io
    output format: ['http://user:pass@ip:port', 'http://user:pass@ip:port', ...]
    """
    logger.info("fetching proxy list from webshare.io")
    async with aiohttp.ClientSession() as session:
        async with session.get(config.PROXY_DOWNLOAD_URL) as response:
            if response.status != 200:
                logger.error(f"response status {response.status}")
                logger.error(f"response body: {await response.text()}")
                raise Exception("error fetching proxy list")

            proxies = str(await response.text())
            proxies = proxies.splitlines()
            proxies = [proxy.split(":") for proxy in proxies]
            proxies = [
                f"http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}"
                for proxy in proxies
            ]
            logger.info(f"fetched {len(proxies)} proxies")
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
