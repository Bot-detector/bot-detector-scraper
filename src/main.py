import asyncio
import logging

import config
from helpers.manager import Manager
from helpers.proxy import ProxyManager

logger = logging.getLogger(__name__)


async def main():
    """
    This function is the main function of the program.
    It creates a list of proxies and then creates a worker for each proxy.
    """
    logger.info(f"inserting in batches of {config.POST_INTERVAL}")

    proxy_manager = ProxyManager(config.PROXY_DOWNLOAD_URL)
    proxies = await proxy_manager.get_proxy_list()

    general_manager = Manager(proxies)
    await general_manager.run(config.POST_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
