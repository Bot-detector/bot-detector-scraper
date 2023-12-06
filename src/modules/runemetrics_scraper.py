from aiohttp import ClientSession

from modules.api.runemetrics_api import RuneMetricsApi
from modules.scraper import Scraper
from modules.validation.player import Player


class RuneMetricsScraper(Scraper):
    def __init__(
        self, proxy: str, worker_name: str, calls_per_minute: int = 60
    ) -> None:
        super().__init__(proxy, worker_name, calls_per_minute)
        self.runemetrics_api = RuneMetricsApi(proxy)

    async def lookup_runemetrics(self, player: Player, session: ClientSession) -> dict:
        await self.rate_limit()
        return await self.runemetrics_api.lookup_runemetrics(
            player=player, session=session
        )
