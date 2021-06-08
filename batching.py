import re
import asyncio
import logging
import os
import time
from logging.handlers import RotatingFileHandler

from aiohttp import ClientSession
from dotenv import load_dotenv

from input_lists import hiscores_minigames, hiscores_skills

# setup logging
logger = logging.getLogger()
rotating_file_handler = RotatingFileHandler('scraper.log', maxBytes=1073741824, backupCount=0)  # 1GB
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
rotating_file_handler.setFormatter(formatter)
logger.addHandler(rotating_file_handler)
logger.setLevel(logging.DEBUG)


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
            proxies = [f'http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}' for proxy in proxies]
            return proxies
        else:
            # by returning a blank list we'll be skipping the creation of any workers
            # effectively the loop will exit and we'll end up running this function again
            logging.error('error fetching proxy list')
            logging.error(f'response status {response.status}')
            logging.error(f'response body: {await response.text()}')
            await asyncio.sleep(60)  # to help with endless looping
            return []


async def hiscores_lookup(username, proxy: str, session: ClientSession, worker_name: str):
    """
    looks up a username on hiscores.  returns a dict summarizing the user
    username: username object
    proxy: proxy to use
    session: aiohttp.ClientSession() object to use
    worker_name: the name of the task
    """
    logging.debug(f"{worker_name}: performing hiscores lookup on {username['name']}")
    async with session.get(url=f"https://secure.runescape.com/m=hiscore_oldschool/index_lite.ws?player={username['name']}", proxy=proxy) as response:
        if response.status == 200:
            logger.debug(f"{worker_name}: found {username['name']} on hiscores")
            # serialize the data
            player_data = [x.split(',')[-1] for x in (await response.text()).split('\n')]
            player_data = dict(zip(hiscores_skills + hiscores_minigames, player_data))

            # if their total isn't ranked, let's calculate and update it
            manual_total = sum([int(player_data[skill]) for skill in hiscores_skills[1:] if int(player_data[skill]) != -1])
            if manual_total > int(player_data['total']):
                logger.debug(f"{worker_name}: manually updated total xp")
                player_data['total'] = str(manual_total)

            # recast every value from str to int
            player_data = {key: int(value) for key, value in player_data.items()}

            # update additional metadata and stash player_data
            username['possible_ban'] = 0
            username['confirmed_ban'] = 0
            username['updated_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            username['hiscores'] = player_data
            return username
        elif response.status == 404:
            logger.debug(f"{worker_name}: {username['name']} does not exist on hiscores.  trying runemetrics")
            # the api expects this key to be present every time, even if no hiscores data
            username['hiscores'] = None
            # either they're too low level, the username isn't tied to an account, or the account is banned
            # runemetrics lookup to try and find out
            return await runemetrics_lookup(proxy=proxy, username=username, session=session, worker_name=worker_name)
        elif response.status == 502:
            logger.warning(f"{worker_name}: 502 proxy error")
        elif response.status == 504:
            logger.warning(f"{worker_name}: 504 from hiscores")
        else:
            logger.error(f"{worker_name}: unhandled status code {response.status} from hiscores_lookup().  header: {response.headers}  body: {await response.text()}")
        raise SkipUsername()



async def runemetrics_lookup(username, proxy, session, worker_name):
    """
    looks up a username on runemetrics.  returns a string indicating the account status
    username: username to lookup
    proxy: proxy to use
    session: aiohttp.ClientSession() object to use
    worker_name: the name of the task
    """

    logging.debug(f"{worker_name}: performing runemetrics lookup on {username['name']}")
    async with session.get(url=f"https://apps.runescape.com/runemetrics/profile/profile?user={username['name']}", proxy=proxy) as response:
        if response.status == 200:
            logging.debug(f"{worker_name}: found {username['name']} on runemetrics")
            if 'error' in await response.json():
                error = (await response.json())['error']
                if error == 'NO_PROFILE':
                    username['label_jagex'] = 1 # username is not associated to an account
                elif error == 'NOT_A_MEMBER':
                    username['label_jagex'] = 2 # account was perm banned
                elif error == 'PROFILE_PRIVATE':
                    # runemetrics is set to private.  either they're too low level or they're banned.
                    username['label_jagex'] = 3
            else:
                # account is active, probably just too low stats for hiscores
                username['label_jagex'] = 0
            return username
        elif response.status == 502:
            logger.warning(f"{worker_name}: 502 proxy error")
        elif response.status == 504:
            logger.warning(f"{worker_name}: 504 returned from RuneMetrics")
        else:
            logger.error(f'{worker_name}: unhandled status code {response.status} from RuneMetrics.  header: {response.headers}  body: {await response.text()}')
        raise SkipUsername()


async def create_worker(proxy, session, worker_name):
    """
    a standalone "worker".  it takes a username obj, does a lookup, and returns it
    proxy: the proxy for the worker to use
    session: the aiohttp.ClientSession() object to use
    name: the name of the worker.  Used for logging/debugging
    """
    # log only the proxy's ip and port
    _proxy_obfuscated = re.search('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,6}', proxy)[0]
    logger.debug(f"{worker_name}: Starting worker using proxy http://{_proxy_obfuscated}")
    while True:
        try:
            # pop a username to work on
            username = usernames.pop()
            # query the username
            data = await hiscores_lookup(username=username, proxy=proxy, session=session, worker_name=worker_name)
            results.append(data)

            await asyncio.sleep(6)
        except IndexError:
            logger.debug(f'{worker_name}: No usernames left to scrape.  stopping')
            break
        except SkipUsername:
            # push username for another worker to pick up
            usernames.append(username)
            await asyncio.sleep(6)
        except Exception as e:
            logger.error(f"{worker_name}: unhandled exception while looking up {username['name']}: {e}")


async def main():
    # stores the usernames to scrape.  .pop() and .append() methods used by all asyncio tasks
    global usernames
    global results
    usernames = []
    results = []

    while True:
        async with ClientSession() as session:
            # get usernames to query
            logger.info('getting usernames to query')
            async with session.get(f"https://www.osrsbotdetector.com/dev/scraper/players/0/{os.getenv('QUERY_SIZE')}/{os.getenv('TOKEN')}") as response:
                usernames = await response.json()
                if len(usernames) > 0:
                    # the api gives us the list in ORDER BY updated_at DESC
                    # when we pop() a name, it pops from the end, so if we want
                    # to query the oldest name first, the list needs to be reversed
                    usernames.reverse()
                    logger.info(f'added {len(usernames)} usernames to queue')
                else:
                    logger.info(f'no usernames to query.  sleeping 60s')
                    await asyncio.sleep(60)                

            # get proxy list
            proxies = await get_proxy_list(session=session)
            logger.info(f'fetched {len(proxies)} proxies')

            # create workers
            logger.info('starting workers')
            tasks = [asyncio.create_task(create_worker(
                proxy=value, session=session, worker_name=f'worker_{key+1}'), name=f'worker_{key+1}') for key, value in enumerate(proxies)]
            await asyncio.gather(*tasks)
            logger.info('all workers stopped')

            # post the results to the api
            logging.info(f'posting {len(results)} results to the api')
            async with session.post(url=f"https://www.osrsbotdetector.com/dev/scraper/hiscores/{os.getenv('TOKEN')}", json=results) as response:
                logger.info(f'uploading {len(results)} scraped usernames to api')
                if response.status == 200:
                    logger.debug(f'successfully uploaded')
                else:
                    logger.error(f'error uploading.  status code: {response.status}  body: {await response.text()}')
            # reset input/output
            usernames = []
            results = []


logger.info('Scraper starting')
asyncio.run(main())
