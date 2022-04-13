import asyncio
import logging
import time

import aiohttp

from helpers.Inputs import Inputs

logger = logging.getLogger(__name__)


class SkipUsername(Exception):
    """
    used to indicate we want to pass this username off to the next available proxy to scrape
    """

    pass


class Scraper:
    def __init__(self, proxy) -> None:
        self.proxy = proxy

    async def lookup_hiscores(self, player: dict) -> dict:
        """
        Performs a hiscores lookup on the given player.

        :param player: a dictionary containing the player's name and id
        :return: a dictionary containing the player's hiscores.  if the player does not exist on hiscores, returns a dictionary of the player
        """
        logger.debug(f"performing hiscores lookup on {player.get('name')}")
        url = f"https://secure.runescape.com/m=hiscore_oldschool/index_lite.ws?player={player['name']}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, proxy=self.proxy) as response:
                if response.status == 200:
                    hiscore = await response.text()
                    hiscore = await self.__parse_hiscores(hiscore)
                    hiscore["Player_id"] = player["id"]
                    return hiscore
                elif response.status == 403:
                    # If we hit the bot challenge page just give up for now..
                    await asyncio.sleep(1)
                    raise SkipUsername
                elif response.status == 404:
                    logger.debug(
                        f"{player['name']} does not exist on hiscores. trying runemetrics"
                    )
                    return {"error": player}
                elif response.status == 502:
                    logger.warning("502 proxy error")
                    await asyncio.sleep(1)
                elif response.status == 504:
                    logger.warning("504 from hiscores")
                    await asyncio.sleep(1)
                else:
                    body = await response.text()
                    logger.error(
                        f"unhandled status code {response.status} from RuneMetrics.  header: {response.headers}  body: {body}"
                    )
                    await asyncio.sleep(1)

    async def __parse_hiscores(self, hiscore):
        """
        Parses the hiscores response into a dictionary.

        :param hiscore: the hiscores response
        :return: a dictionary containing the hiscores
        """
        # each row is seperated by a new line.
        # each value is seperated by a comma.
        # we only want the last value; the xp/kills
        hiscore = [x.split(",")[-1] for x in hiscore.split("\n")]

        # filter empty line (last line is empty)
        hiscore = list(filter(None, hiscore))

        # failsafe incase they update the hiscores
        expected_rows = len(Inputs.skills + Inputs.minigames + Inputs.bosses)
        if len(hiscore) != expected_rows:
            raise Exception(
                f"Unexpected hiscore size. Received: {len(hiscore)}, Expected: {expected_rows}"
            )

        hiscore = dict(zip(Inputs.skills + Inputs.minigames + Inputs.bosses, hiscore))

        # calculate the skills total as it might not be ranked
        hiscore["total"] = sum(
            [
                int(hiscore[skill])
                for skill in Inputs.skills[1:]
                if int(hiscore[skill]) != -1
            ]
        )

        # cast every value to integer
        hiscore = {k: int(v) for k, v in hiscore.items()}
        return hiscore

    async def lookup_runemetrics(self, player: dict) -> dict:
        """ "
        Performs a RuneMetrics lookup on the given player.

        :param player: a dictionary containing the player's name and id
        :return: a dictionary containing the player's RuneMetrics data
        """
        url = f"https://apps.runescape.com/runemetrics/profile/profile?user={player.get('name')}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, proxy=self.proxy) as response:
                    if response.status == 200:
                        logger.debug(f"found {player.get('name')} on runemetrics")
                        data = await response.json()
                        if "error" in data:
                            error = data["error"]
                            if error == "NO_PROFILE":
                                # username is not associated to an account
                                player["label_jagex"] = 1
                            elif error == "NOT_A_MEMBER":
                                player["label_jagex"] = 2  # account was perm banned
                            elif error == "PROFILE_PRIVATE":
                                # runemetrics is set to private.  either they're too low level or they're banned.
                                player["label_jagex"] = 3
                        else:
                            # account is active, probably just too low stats for hiscores
                            player["label_jagex"] = 0

                        # API assigns this too, but jsut being safe
                        player["updated_at"] = time.strftime(
                            "%Y-%m-%d %H:%M:%S", time.gmtime()
                        )
                        return player
                    elif response.status == 502:
                        logger.warning("502 proxy error")
                        await asyncio.sleep(1)
                    elif response.status == 504:
                        logger.warning("504 returned from RuneMetrics")
                        await asyncio.sleep(1)
                    else:
                        body = await response.text()
                        logger.error(
                            f"unhandled status code {response.status} from RuneMetrics.  header: {response.headers}  body: {body}"
                        )
                        await asyncio.sleep(1)
                    raise SkipUsername()
        except Exception as e:
            logger.warning(e)
            raise SkipUsername()
        pass
