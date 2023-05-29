import asyncio
import logging

import config.config as config
from config.config import app_config
from modules.manager import Manager
from modules.proxy_manager import ProxyManager

logger = logging.getLogger(__name__)


async def main():
    """
    This function is the main function of the program.
    It creates a list of proxies and then creates a worker for each proxy.
    """
    logger.info(f"inserting in batches of {app_config.POST_INTERVAL}")
    logger.info(f"{app_config.ENDPOINT=}")

    proxy_manager = ProxyManager(app_config.PROXY_DOWNLOAD_URL)
    proxies = await proxy_manager.get_proxy_list()

    assert isinstance(proxies, list), "proxies must be a list"
    assert all(isinstance(item, str) for item in proxies), "proxies must contain only strings"

    general_manager = Manager(proxies)
    await general_manager.run(app_config.POST_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
