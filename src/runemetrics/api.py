import logging

from aiohttp import ClientSession
from osrs.exceptions import Undefined, UnexpectedRedirection
from osrs.utils.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)


class RuneMetrics:
    BASE_URL = "https://apps.runescape.com/runemetrics"

    def __init__(
        self, proxy: str = "", rate_limiter: RateLimiter = RateLimiter()
    ) -> None:
        self.proxy = proxy
        self.rate_limiter = rate_limiter

    async def _make_request(self, session: ClientSession, url: str, params: dict):
        async with session.get(url, proxy=self.proxy, params=params) as response:
            # when the HS are down it will redirect to the main page.
            # after redirction it will return a 200, so we must check for redirection first
            if response.history and any(r.status == 302 for r in response.history):
                raise UnexpectedRedirection(
                    f"Redirection occured: {response.url} - {response.history[0].url}"
                )
            elif response.status != 200:
                # raises ClientResponseError
                response.raise_for_status()
                raise Undefined()
            return await response.json()

    async def get_profile(
        self, session: ClientSession, player_name: str, activities: int = 0
    ) -> dict:
        """
        activities, number of activities for that player
        """
        await self.rate_limiter.check()

        params = {"user": player_name, "activities": activities}
        params = {k: v for k, v in params.items() if v}
        url = f"{self.BASE_URL}/profile/profile"
        data = await self._make_request(session=session, url=url, params=params)
        return data

    async def get_monthly_xp(
        self, session: ClientSession, player_name: str, skill_id: int | None = None
    ) -> dict:
        await self.rate_limiter.check()

        params = {"searchName": player_name, "skillid": skill_id}
        params = {k: v for k, v in params.items() if v}

        url = f"{self.BASE_URL}/xp-monthly"
        data = await self._make_request(session=session, url=url, params=params)
        return data

    async def get_quests(self, session: ClientSession, player_name: str) -> dict:
        await self.rate_limiter.check()
        params = {"user": player_name}
        url = f"{self.BASE_URL}/quests"
        data = await self._make_request(session=session, url=url, params=params)
        return data
