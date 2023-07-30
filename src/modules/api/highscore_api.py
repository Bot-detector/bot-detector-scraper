import asyncio
import logging
import time

from aiohttp import ClientResponse, ClientSession

from modules.validation.player import Player, PlayerDoesNotExistException
from utils.http_exception_handler import InvalidResponse

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


class HighscoreApi:
    def __init__(self, proxy: str = None) -> None:
        self.proxy = proxy
        self.base_url = (
            "https://secure.runescape.com/m=hiscore_oldschool/index_lite.json"
        )

    async def lookup_hiscores(self, player: Player, session: ClientSession) -> dict:
        logger.info(f"Performing hiscores lookup on {player.name}")
        url = f"{self.base_url}?player={player.name}"

        async with session.get(url, proxy=self.proxy) as response:
            data = await self._handle_response_status(response, player)
            assert data is not None, f"Data should not be None"
            hiscore = await self._parse_hiscores(data)
            assert hiscore is not None, f"hiscore should not be None"
            hiscore["Player_id"] = player.id
            hiscore["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            return hiscore

    async def _handle_response_status(
        self, response: ClientResponse, player: Player
    ) -> dict:
        status = response.status
        match status:
            # OK
            case 200:
                return await response.json()
            # OK
            case 404:
                logger.debug(f"{player.name} does not exist on hiscores.")
                raise PlayerDoesNotExistException(
                    f"Player {player.dict()} does not exist"
                )
            # NOK, but known
            # case 403, 502, 500, 504, 520, 524:
            #     pass
            # NOK
            case _:
                body = await response.text()
                logger.error(
                    f"Unhandled status code {status}.\n"
                    f"Header: {response.headers}\n"
                    f"Body: {body}"
                )
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
