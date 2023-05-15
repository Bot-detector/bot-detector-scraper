import asyncio
import logging
import time
from collections import deque

import aiohttp
import traceback
from typing import Union

logger = logging.getLogger(__name__)


class SkipUsername(Exception):
    """
    used to indicate we want to pass this username off to the next available proxy to scrape
    """

    pass


hiscore_mapper = {
    "league_points": "league",
    "clue_scrolls_all": "cs_all",
    "clue_scrolls_beginner": "cs_beginner",
    "clue_scrolls_easy": "cs_easy",
    "clue_scrolls_medium": "cs_medium",
    "clue_scrolls_hard": "cs_hard",
    "clue_scrolls_elite": "cs_elite",
    "clue_scrolls_master": "cs_master",
    "theatre_of_blood_hard_mode": "theatre_of_blood_hard",
    "tombs_of_amascut_expert_mode": "tombs_of_amascut_expert",
}


class Scraper:
    def __init__(self, proxy: str, calls_per_minute: int = 60) -> None:
        self.proxy = proxy
        self.history = deque(maxlen=calls_per_minute)

    async def rate_limit(self):
        """
        Rate limits the scraper to 60 calls a minute.
        """
        self.history.append(int(time.time()))
        maxlen = self.history.maxlen
        if len(self.history) == maxlen:
            head = self.history[0]
            tail = self.history[-1]
            span = tail - head
            if span < 60:
                sleep = 60 - span
                logger.debug(f"Rate limit reached, sleeping {sleep} seconds")
                await asyncio.sleep(sleep)
        return

    async def lookup_hiscores(
        self, player: dict, session: aiohttp.ClientSession
    ) -> dict:
        """
        Performs a hiscores lookup on the given player.

        :param player: a dictionary containing the player's name and id
        :return: a dictionary containing the player's hiscores.  if the player does not exist on hiscores, returns a dictionary of the player
        """
        await self.rate_limit()
        logger.debug(f"performing hiscores lookup on {player.get('name')}")
        url = f"https://secure.runescape.com/m=hiscore_oldschool/index_lite.json?player={player['name']}"
        try:
            async with session.get(url, proxy=self.proxy) as response:
                match response.status:
                    case 200:
                        hiscore = await response.json()
                        hiscore = await self._parse_hiscores(hiscore)
                        hiscore["Player_id"] = player["id"]
                        return hiscore
                    case 403:
                        logger.warning(
                            f"403 bot challenge received proxy: {self.proxy}"
                        )
                        # If we hit the bot challenge page just give up for now..
                        await asyncio.sleep(1)
                    case 404:
                        logger.debug(
                            f"{player.get('name')} does not exist on hiscores. trying runemetrics"
                        )
                        return {"error": player}
                    case 502:
                        logger.warning("502 proxy error")
                        await asyncio.sleep(1)
                    case (500, 504, 520, 524):
                        logger.warning(
                            f"{response.status} returned from hiscore_oldschool"
                        )
                        await asyncio.sleep(1)
                    case _:
                        body = await response.text()
                        logger.error(
                            f"unhandled status code {response.status} from hiscore_oldschool.  header: {response.headers}  body: {body}"
                        )
                        await asyncio.sleep(1)
                return None
        except Exception as e:
            tb_str = traceback.format_exc()  # get the stack trace as a string
            logger.error(f"{e}, player: {player}\n{tb_str}")
            await asyncio.sleep(10)
            return None

    def _parse_hiscore_name(self, name: str) -> str:
        name = name.lower()
        name = name.replace("'", "")
        name = name.replace(" - ", " ")
        name = name.replace("-", "_")
        name = name.replace(":", "")
        name = name.replace("(", "").replace(")", "")
        name = name.replace(" ", "_")
        #  replaces "name" with its corresponding abbreviation from "hiscore_mapper" dictionary,
        # if one exists, or keeps the original name if it does not
        name = hiscore_mapper.get(name, name)
        return name

    def _parse_hiscore_stat(self, stat: int) -> int:
        stat = 0 if stat == -1 else stat
        return stat

    async def _parse_hiscores(self, hiscore: dict) -> dict:
        # Extract skill data from hiscore dictionary and create a new dictionary
        skill_stats = {
            self._parse_hiscore_name(s["name"]): self._parse_hiscore_stat(s["xp"])
            for s in hiscore.get("skills")
            if s["name"] != "Overall"
        }

        # Calculate the sum of all skills and add it to the skills dictionary
        skill_stats["total"] = sum(
            [v for k, v in skill_stats.items() if k not in ("total", "overall")]
        )

        # Extract activity data from hiscore dictionary and create a new dictionary
        activity_stats = {
            self._parse_hiscore_name(a["name"]): self._parse_hiscore_stat(a["score"])
            for a in hiscore.get("activities")
        }

        # Merge the skills and activities dictionaries and return the result
        return skill_stats | activity_stats

    async def lookup_runemetrics(
        self, player: dict, session: aiohttp.ClientSession
    ) -> dict:
        """
        Performs a RuneMetrics lookup on the given player.

        :param player: a dictionary containing the player's name and id
        :return: a dictionary containing the player's RuneMetrics data
        """
        url = f"https://apps.runescape.com/runemetrics/profile/profile?user={player.get('name')}"
        try:
            async with session.get(url, proxy=self.proxy) as response:
                match response.status:
                    case 200:
                        logger.debug(f"found {player.get('name')} on runemetrics")
                        data: dict = await response.json()
                        match data.get("error"):
                            case "NO_PROFILE":
                                # username is not associated to an account
                                player["label_jagex"] = 1
                            case "NOT_A_MEMBER":
                                player["label_jagex"] = 2  # account was perm banned
                            case "PROFILE_PRIVATE":
                                # runemetrics is set to private.  either they're too low level or they're banned.
                                player["label_jagex"] = 3
                            case _:
                                # account is active, probably just too low stats for hiscores
                                player["label_jagex"] = 0

                        # API assigns this too, but just being safe
                        player["updated_at"] = time.strftime(
                            "%Y-%m-%d %H:%M:%S", time.gmtime()
                        )
                        return player

                    case 502:
                        logger.warning("502 proxy error")
                        await asyncio.sleep(1)

                    case (500, 504, 520, 524):
                        logger.warning(f"{response.status} returned from RuneMetrics")
                        await asyncio.sleep(1)

                    case _:
                        body = await response.text()
                        logger.error(
                            f"unhandled status code {response.status} from RuneMetrics. header: {response.headers} body: {body}"
                        )
                        await asyncio.sleep(1)

        except Exception as e:
            tb_str = traceback.format_exc()  # get the stack trace as a string
            logger.error(f"{e}, player: {player}\n{tb_str}")
            return None
