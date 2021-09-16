import asyncio
import logging
import os
import re
import sys
import time
from multiprocessing import Queue

import logging_loki
from aiohttp import ClientSession
from dotenv import load_dotenv
from discord_webhook import DiscordWebhook
from discord_webhook.webhook import DiscordEmbed

from input_lists import hiscores_minigames, hiscores_skills

# setup logging
# loki_handler = logging_loki.LokiQueueHandler(
#     Queue(-1),
#     url="http://loki:3100/loki/api/v1/push",
#     tags={"service": "scraper_continuous"},
# )
logger = logging.getLogger()

file_handler = logging.FileHandler(filename="scraper.log", mode='a')
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(handler)

# logger.addHandler(loki_handler)
logger.setLevel(logging.DEBUG)
# loki uses urllib3 to ship the logs to the DB.  And urllib 3 has debug level messages every time it
# opens a connection, which causes a circular loop.  so this is a simple hack to avoid the problem
logging.getLogger('urllib3').setLevel(logging.INFO)

load_dotenv()


class SkipUsername(Exception):
    """
    used to indicate we want to pass this username off to the next available proxy to scrape
    """
    pass


async def get_proxy_list(session):
    """
    returns the proxy list from webshare.io
    output format: ['http://user:pass@ip:port', 'http://user:pass@ip:port', ...]
    """
    logger.info('fetching proxy list from webshare.io')
    async with session.get(os.getenv('PROXY_DOWNLOAD_URL')) as response:
        if response.status == 200:
            proxies = [proxy.split(':') for proxy in (await response.text()).splitlines()]
            proxies = [
                f'http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}' for proxy in proxies
            ]
            return proxies
        else:
            logger.error('error fetching proxy list')
            logger.error(f'response status {response.status}')
            logger.error(f'response body: {await response.text()}')
            raise Exception('error fetching proxy list')


async def hiscores_lookup(username, proxy: str, session: ClientSession, worker_name: str, retry: bool = False):
    """
    looks up a username on hiscores.  returns a dict summarizing the user
    username: username object
    proxy: proxy to use
    session: aiohttp.ClientSession() object to use
    worker_name: the name of the task
    """
    logger.debug(f"performing hiscores lookup on {username['name']}", extra={ "tags": {"worker": worker_name}})
    async with session.get(url=f"https://secure.runescape.com/m=hiscore_oldschool/index_lite.ws?player={username['name']}", proxy=proxy) as response:
        if response.status == 200:
            #logger.debug(f"found {username['name']} on hiscores", extra={"tags": {"worker": worker_name}})
            # serialize the data
            player_data = [x.split(',')[-1] for x in (await response.text()).split('\n')]
            player_data = dict(
                zip(hiscores_skills + hiscores_minigames, player_data)
            )

            # if their total isn't ranked, let's calculate and update it
            manual_total = sum(
                [int(player_data[skill]) for skill in hiscores_skills[1:] if int(player_data[skill]) != -1]
            )

            if manual_total > int(player_data['total']):
                logger.debug("manually updated total xp", extra={"tags": {"worker": worker_name}})
                player_data['total'] = str(manual_total)

            # recast every value from str to int
            player_data = {key: int(value) for key, value in player_data.items()}

            # update additional metadata and stash player_data
            username['possible_ban'] = 0
            username['confirmed_ban'] = 0
            username['updated_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            
            output = {}
            output['player'] = username
            output['hiscores'] = player_data
            return output
        elif response.status == 404:
            logger.debug(f"{username['name']} does not exist on hiscores.  trying runemetrics", extra={"tags": {"worker": worker_name}})

            output = {}
            output['player'] = await runemetrics_lookup(proxy=proxy, username=username, session=session, worker_name=worker_name)
            output['hiscores'] = None
            return output
        elif response.status == 502:
            logger.warning("502 proxy error", extra={"tags": {"worker": worker_name}})
            if not retry:
                await asyncio.sleep(6)
                return await hiscores_lookup(username=username, proxy=proxy, session=session, worker_name=worker_name, retry=True)

        elif response.status == 504:
            logger.warning("504 from hiscores", extra={"tags": {"worker": worker_name}})
            if not retry:
                await asyncio.sleep(6)
                return await hiscores_lookup(username=username, proxy=proxy, session=session, worker_name=worker_name, retry=True)
        else:
            logger.error(f"unhandled status code {response.status} from hiscores_lookup().  header: {response.headers}  body: {await response.text()}", extra={"tags": {"worker": worker_name}})
        raise SkipUsername()


async def runemetrics_lookup(username, proxy, session, worker_name):
    """
    looks up a username on runemetrics.  returns a string indicating the account status
    username: username to lookup
    proxy: proxy to use
    session: aiohttp.ClientSession() object to use
    worker_name: the name of the task
    """

    #logger.debug(f"performing runemetrics lookup on {username['name']}", extra={"tags": {"worker": worker_name}})
    async with session.get(url=f"https://apps.runescape.com/runemetrics/profile/profile?user={username['name']}", proxy=proxy) as response:
        if response.status == 200:
            logger.debug(f"found {username['name']} on runemetrics", extra={"tags": {"worker": worker_name}})
            if 'error' in await response.json():
                error = (await response.json())['error']
                if error == 'NO_PROFILE':
                    # username is not associated to an account
                    username['label_jagex'] = 1
                elif error == 'NOT_A_MEMBER':
                    username['label_jagex'] = 2  # account was perm banned
                    players_banned.append(username['name']) #add name to list to be broadcast in #bot-graveyard
                elif error == 'PROFILE_PRIVATE':
                    # runemetrics is set to private.  either they're too low level or they're banned.
                    username['label_jagex'] = 3
            else:
                # account is active, probably just too low stats for hiscores
                username['label_jagex'] = 0
            
            #API assigns this too, but jsut being safe
            username['updated_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())

            return username
        elif response.status == 502:
            logger.warning("502 proxy error", extra={"tags": {"worker": worker_name}})
        elif response.status == 504:
            logger.warning("504 returned from RuneMetrics", extra={"tags": {"worker": worker_name}})
        else:
            logger.error(f"unhandled status code {response.status} from RuneMetrics.  header: {response.headers}  body: {await response.text()}", extra={"tags": {"worker": worker_name}})
        raise SkipUsername()


async def create_worker(proxy: str, session, worker_name):
    """
    a standalone "worker".  it takes a username obj, does a lookup, and returns it
    proxy: the proxy for the worker to use
    session: the aiohttp.ClientSession() object to use
    name: the name of the worker.  Used for logging/debugging
    """
    # log only the proxy's ip and port
    _proxy_obfuscated = re.search(
        '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,6}', proxy)[0]
    logger.debug(f"Starting worker using proxy http://{_proxy_obfuscated}", extra={"tags": {"worker": worker_name}})
    while True:
        try:
            # pop a username to work on
            username = usernames.pop()
            # query the username
            data = await hiscores_lookup(username=username, proxy=proxy, session=session, worker_name=worker_name)
            results.append(data)

            # await asyncio.sleep(6)
        except IndexError or TypeError:
            logger.debug("No usernames left to scrape.  stopping", extra={"tags": {"worker": worker_name}})
            break
        except SkipUsername:
            # push username for another worker to pick up
            usernames.append(username)
            # await asyncio.sleep(6)
        except Exception as e:
            logger.error(f"unhandled exception while looking up {username['name']}: {e}", extra={"tags": {"worker": worker_name}})


async def fill_graveyard_plots():
    while True:
        num_pending_players = len(players_banned)
        # if is empty list break
        if not players_banned:
            logger.debug("No names to send to the graveyard.")
            break
        elif num_pending_players > 50:
            broadcast_size = 50
        else:
            broadcast_size = num_pending_players
            
        players_to_broadcast = []
        for i in range(broadcast_size):
                players_to_broadcast.append(players_banned.pop())

        webhook = DiscordWebhook(url=os.getenv('GRAVEYARD_WEBHOOK_URL'))
        embed = DiscordEmbed(title="All Ye Bots Lose All Hope", color="000000")

        embed.set_timestamp()
        embed.add_embed_field(name="Newly Departed", value=f"{', '.join(players_to_broadcast)}")
        embed.set_thumbnail(url="https://i.imgur.com/PPnZRHW.gif")

        webhook.add_embed(embed=embed)
        webhook.execute()
        asyncio.sleep(5) #Be mindful of the Discord rate limit

async def main():
    # stores the usernames to scrape.  .pop() and .append() methods used by all asyncio tasks
    global usernames
    global results
    global players_banned
    usernames = []
    results = []
    players_banned = []

    # get proxy list
    logger.info(f'fetching proxy list')
    # from https://stackoverflow.com/questions/63347818/aiohttp-client-exceptions-clientconnectorerror-cannot-connect-to-host-stackover
    async with ClientSession(trust_env=True) as session:
        proxies = await get_proxy_list(session=session)
        logger.info(f'fetched {len(proxies)} proxies')

    while True:
        # from https://stackoverflow.com/questions/63347818/aiohttp-client-exceptions-clientconnectorerror-cannot-connect-to-host-stackover
        async with ClientSession(trust_env=True) as session:
            # get usernames to query
            logger.info('getting usernames to query')
            async with session.get(f"{os.getenv('endpoint')}/scraper/players/0/{os.getenv('QUERY_SIZE')}/{os.getenv('TOKEN')}") as response:
                usernames = await response.json()

                if len(usernames) > 0:
                    # the api gives us the list in ORDER BY updated_at DESC
                    # when we pop() a name, it pops from the end, so if we want
                    # to query the oldest name first, the list needs to be reversed
                    usernames.reverse()
                    logger.info(f'added {len(usernames)} usernames to queue')

                    # create workers
                    logger.info('starting workers')
                    tasks = [asyncio.create_task(create_worker(
                        proxy=value, session=session, worker_name=f'worker_{str(key+1).rjust(len(str(len(proxies))), "0")}'), name=f'worker_{str(key+1).rjust(len(str(len(proxies))), "0")}') for key, value in enumerate(proxies)]
                    asyncio.create_task(fill_graveyard_plots())
                    await asyncio.gather(*tasks)
                    logger.info('all workers stopped')

                    # post the results to the api
                    logger.info(f'posting {len(results)} results to the api')
                    async with session.post(url=f"{os.getenv('endpoint')}/scraper/hiscores/{os.getenv('TOKEN')}", json=results) as response:
                        logger.info(
                            f'uploading {len(results)} scraped usernames to api')
                        if response.status == 200:
                            logger.debug(f'successfully uploaded')
                        else:
                            logger.error(f'error uploading.  status code: {response.status}  body: {await response.text()}')
                else:
                    logger.info(f'no usernames to query.  sleeping 60s')
                    await asyncio.sleep(60)
            
            # reset input/output
            usernames = []
            results = []


# from https://stackoverflow.com/questions/63347818/aiohttp-client-exceptions-clientconnectorerror-cannot-connect-to-host-stackover
if (1 == 1
        and sys.platform.startswith('win')
        and sys.version_info[0] == 3
        and sys.version_info[1] >= 8
    ):
    logger.info('Set policy')
    policy = asyncio.WindowsSelectorEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)

logger.info('Scraper starting')
asyncio.run(main())
