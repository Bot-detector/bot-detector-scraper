import asyncio
import logging
import traceback
import uuid
from asyncio import Event, Queue

from aiohttp import ClientSession, ClientTimeout

from config.config import AppConfig
from modules import kafka
from modules.api.webshare_api import Webshare
from modules.scraper import HighScoreScraper, RuneMetricsScraper, Scraper
from modules.validation.player import Player

logger = logging.getLogger(__name__)


async def scrape(
    player: dict, scraper: Scraper, session: ClientSession
) -> tuple[str, str]:
    error = None

    try:
        player = Player(**player)
        player = await scraper.lookup(player=player, session=session)
    except Exception as error:
        error_type = type(error)
        logger.error(
            {
                "name": scraper.worker_name,
                "error_type": error_type.__name__,
                "error": error,
                "player_name": player,
            }
        )
        tb_str = traceback.format_exc()
        logger.error(f"{error}, \n{tb_str}")
    return player, error


async def process_messages(
    receive_queue: Queue,
    send_queue: Queue,
    error_queue: Queue,
    shutdown_event: Event,
    proxy: str,
):
    name = str(uuid.uuid4())[-8:]
    scraper = RuneMetricsScraper(proxy=proxy, worker_name=name)
    timeout = ClientTimeout(total=AppConfig().SESSION_TIMEOUT)

    async with ClientSession(timeout=timeout) as session:
        while not shutdown_event.is_set():
            if receive_queue.empty():
                await asyncio.sleep(1)
                continue

            data = await receive_queue.get()
            receive_queue.task_done()
            player, error = await scrape(player=data, scraper=scraper, session=session)
            player: Player

            if error is not None:
                await error_queue.put(data)
                continue

            await send_queue.put(player.dict())
    logger.info("shutdown")


async def get_proxies() -> list:
    webshare = Webshare(api_key=AppConfig().PROXY_API_KEY)
    proxy_list = await webshare.get_proxies()
    logger.info(f"gathered {len(proxy_list)} proxies")
    return proxy_list


async def main():
    shutdown_event = Event()
    consumer = await kafka.kafka_consumer(topic="scraper-runemetrics", group="scraper")
    producer = await kafka.kafka_producer()

    receive_queue = Queue(maxsize=500)
    send_queue = Queue(maxsize=100)
    error_queue = Queue(maxsize=500)

    asyncio.create_task(
        kafka.receive_messages(
            consumer=consumer,
            receive_queue=receive_queue,
            shutdown_event=shutdown_event,
        )
    )

    asyncio.create_task(
        kafka.send_messages(
            topic="scraper",
            producer=producer,
            send_queue=send_queue,
            shutdown_event=shutdown_event,
        )
    )

    asyncio.create_task(
        kafka.send_messages(
            topic="scraper-runemetrics",
            producer=producer,
            send_queue=error_queue,
            shutdown_event=shutdown_event,
        )
    )
    proxy_list = await get_proxies()
    tasks = []
    for proxy in proxy_list:
        task = asyncio.create_task(
            process_messages(
                send_queue=send_queue,
                receive_queue=receive_queue,
                error_queue=error_queue,
                shutdown_event=shutdown_event,
                proxy=proxy,
            )
        )
        tasks.append(task)
    # await task for completion (never)
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
        loop.run_until_complete(main())
    except RuntimeError:
        asyncio.run(main())
