import asyncio
import logging
import time
from collections import deque
from http.client import responses
from aiohttp import ClientSession, ClientResponse
from utils.http_exception_handler import http_exception_handler, InvalidResponse
from modules.validation.player import Player, PlayerDoesNotExistException

logger = logging.getLogger(__name__)

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
                logger.warning(f"Rate limit reached, sleeping {sleep} seconds")
                await asyncio.sleep(sleep)
        return

    async def _handle_response_status(
        self, response: ClientResponse, player: Player, source_function: str
    ) -> dict:
        status = response.status
        status_code = responses.get(status)
        match status:
            case 200:
                data = await response.json()
                return data
            case 404:
                if source_function == "lookup_highscores":
                    logger.debug(f"{player.name} does not exist on hiscores.")
                    raise PlayerDoesNotExistException(f"Player {player.dict()} does not exist")
                logger.warning(f"{source_function} returned {status}-{status_code}")
            case 403, 502, 500, 504, 520, 524:
                logger.warning(f"{source_function} returned {status}-{status_code}")
            case _:
                body = await response.text()
                logger.error(
                    f"Unhandled status code {status} from hiscore_oldschool. Header: {response.headers} Body: {body}"
                )
        await asyncio.sleep(60)
        raise InvalidResponse()

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

    @http_exception_handler
    async def lookup_hiscores(self, player: Player, session: ClientSession) -> dict:
        await self.rate_limit()
        player_name = player.name
        logger.info(f"Performing hiscores lookup on {player_name}")
        base_url = "https://secure.runescape.com/m=hiscore_oldschool/index_lite.json"
        url = f"{base_url}?player={player_name}"

        assert isinstance(session, ClientSession)

        async with session.get(url, proxy=self.proxy) as response:
            data = await self._handle_response_status(response, player, "lookup_highscores")
            if data is None:
                return data
            hiscore = await self._parse_hiscores(data)
            hiscore["Player_id"] = player.id
            return hiscore

    @http_exception_handler
    async def lookup_runemetrics(
        self, player: Player, session: ClientSession
    ) -> dict:
        """
        Performs a RuneMetrics lookup on the given player.

        :param player: a dictionary containing the player's name and id
        :return: a dictionary containing the player's RuneMetrics data
        """
        player_name = player.name
        base_url = "https://apps.runescape.com/runemetrics/profile/profile"
        url = f"{base_url}?user={player_name}"

        async with session.get(url, proxy=self.proxy) as response:
            data: dict = await self._handle_response_status(response, player, "lookup_runemetrics")

            if data is None:
                return None

            logger.info(f"found {player_name} on runemetrics")

            match data.get("error"):
                case "NO_PROFILE":
                    # username is not associated to an account
                    player.label_jagex = 1
                case "NOT_A_MEMBER":
                    # account is perm banned
                    player.label_jagex = 2  
                case "PROFILE_PRIVATE":
                    # runemetrics is set to private. either they're too low level or they're banned.
                    player.label_jagex = 3
                case _:
                    # account is active, probably just too low stats for hiscores
                    player.label_jagex = 0
            # API assigns this too, but just being safe
            player.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            return player
