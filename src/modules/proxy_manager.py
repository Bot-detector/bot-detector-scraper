import aiohttp
import logging

logger = logging.getLogger(__name__)


class ProxyManager:
    def __init__(self, PROXY_DOWNLOAD_URL: str) -> None:
        self.PROXY_DOWNLOAD_URL = PROXY_DOWNLOAD_URL

    async def _fetch_proxy_list(self, session: aiohttp.ClientSession) -> str:
        """
        Fetch the proxy list from webshare.io and return the response body as string.
        """
        async with session.get(self.PROXY_DOWNLOAD_URL) as response:
            if response.status != 200:
                logger.error(f"response status {response.status}")
                logger.error(f"response body: {await response.text()}")
                raise Exception("error fetching proxy list")
            return await response.text()

    def _parse_proxy_list(self, proxies_str: str) -> list[str]:
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

    async def get_proxy_list(self) -> list[str]:
        """
        Return a list of formatted proxy URLs from webshare.io.
        """
        logger.info("fetching proxy list from webshare.io")
        async with aiohttp.ClientSession(trust_env=True) as session:
            try:
                proxies_str = await self._fetch_proxy_list(session)
                self.proxies = self._parse_proxy_list(proxies_str)
            except Exception as error:
                logger.error(f"{type(error).__name__}: {str(error)}")
                raise Exception("error fetching proxy list")
        return self.proxies
