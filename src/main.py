import asyncio
import logging
import threading
from multiprocessing import Process

import config.config as config
from config.config import app_config
from modules.manager import Manager
from modules.proxy_manager import ProxyManager

logger = logging.getLogger(__name__)


def batchify_list(input_list: list, batch_size: int) -> list[list]:
    return [
        input_list[i : i + batch_size] for i in range(0, len(input_list), batch_size)
    ]

def run_manager(proxy_list: list):
    asyncio.run(Manager(proxy_list).run())

def create_and_run_processes(proxy_batches: list[list]):
    processes: list[Process] = []

    for batch in proxy_batches:
        process = Process(target=run_manager, args=(batch,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()


def create_and_run_threads(proxy_batches: list[list]):
    threads = []

    for batch in proxy_batches:
        thread = threading.Thread(target=run_manager, args=(batch,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


async def main():
    logger.info(f"{app_config.ENDPOINT=}")
    logger.info(f"{app_config.KAFKA_HOST=}")

    BATCH_SIZE = 50

    proxy_manager = ProxyManager(app_config.PROXY_DOWNLOAD_URL)
    proxies = await proxy_manager.get_proxy_list()
    # proxies = proxies[:5] # debugging
    proxy_batches = batchify_list(proxies, BATCH_SIZE)

    # create_and_run_processes(proxy_batches)
    create_and_run_threads(proxy_batches)
    # await Manager(proxies).run()


if __name__ == "__main__":
    asyncio.run(main())
