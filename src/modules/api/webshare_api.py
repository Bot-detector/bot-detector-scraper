import asyncio
import logging

from aiohttp import ClientSession
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Ports(BaseModel):
    http: int
    socks5: int


class Proxy(BaseModel):
    username: str
    password: str
    proxy_address: str
    ports: Ports

    @property
    def url(self):
        return f"http://{self.username}:{self.password}@{self.proxy_address}:{self.ports.http}"


class Webshare:
    def __init__(self, api_key: str) -> None:
        referral = "https://www.webshare.io/?referral_code=qvpjdwxqsblt"
        print(f"to get an api key, please use our referral code: {referral}")
        self.api_key = api_key

    async def fetch_proxies(
        self, session: ClientSession, url: str, headers: dict
    ) -> tuple[list[Proxy], dict]:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                proxies: dict = await response.json()
                results = proxies.get("results", [])
                return [Proxy(**r) for r in results], proxies
            else:
                logger.error(
                    f"Failed to retrieve proxies. Status code: {response.status}"
                )
                return []

    async def get_proxies(self):
        URL = "https://proxy.webshare.io/api/proxy/list/"
        headers = {"Authorization": f"Token {self.api_key}"}
        all_results = []  # To store results from all pages
        next_url = URL  # Initialize with the first page URL

        async with ClientSession() as session:
            while next_url:
                proxies, resp = await self.fetch_proxies(session, next_url, headers)
                page_results = [proxy.url for proxy in proxies]
                all_results.extend(page_results)
                next = resp.get("next")
                next_url = f"https://proxy.webshare.io{next}" if next else None
                logger.info(f"{next_url=}")

        return all_results


async def main():
    webshare = Webshare(api_key="gks5qtavhlbtlyqnmy8upufsu31fn7qi4z9ldj7a")
    proxies = await webshare.get_proxies()
    [print(p) for p in proxies]


if __name__ == "__main__":
    asyncio.run(main())
