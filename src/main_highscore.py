import asyncio
import logging
import time
import traceback
import uuid
from asyncio import Event, Queue

from aiohttp import ClientSession, ClientTimeout
from prometheus_client import Counter, Histogram, start_http_server

from config.config import AppConfig
from modules import _kafka
from modules.api.webshare_api import Webshare
from modules.scraper import HighScoreScraper, Scraper
from modules.validation.player import Player

logger = logging.getLogger(__name__)

start_http_server(8000)

# Define Prometheus metrics
total_counter = Counter(
    name="highscore_request_count",
    documentation="Count of request player stats fetches",
)
success_counter = Counter(
    name="highscore_success_count",
    documentation="Count of successful player stats fetches",
    labelnames=["proxy"],
)
error_counter = Counter(
    name="highscore_error_count",
    documentation="Count of failed player stats fetches",
    labelnames=["proxy"],
)
not_found_counter = Counter(
    name="highscore_not_found_count",
    documentation="Count of players not found",
    labelnames=["proxy"],
)
latency_histogram = Histogram(
    name="highscore_fetch_latency_seconds",
    documentation="Latency of player stats fetches",
    labelnames=["proxy"],
)


async def scrape(
    player: dict, scraper: HighScoreScraper, session: ClientSession
) -> tuple[Player, dict | None, str | None]:
    error = None
    highscore = None
    try:
        player = Player(**player)
        player, highscore = await scraper.lookup(player=player, session=session)
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
    return player, highscore, error


async def process_messages(
    receive_queue: Queue,
    send_queue: Queue,
    error_queue: Queue,
    runemetrics_send_queue: Queue,
    shutdown_event: Event,
    proxy: str,
):
    name = str(uuid.uuid4())[-8:]
    scraper = HighScoreScraper(proxy=proxy, worker_name=name)
    timeout = ClientTimeout(total=AppConfig().SESSION_TIMEOUT)

    _proxy = proxy.split("@")[1]
    async with ClientSession(timeout=timeout) as session:
        while not shutdown_event.is_set():
            if receive_queue.empty():
                await asyncio.sleep(1)
                continue

            data = await receive_queue.get()
            receive_queue.task_done()

            # increment total counter
            total_counter.inc()

            start_time = time.time()
            player, highscore, error = await scrape(
                player=data, scraper=scraper, session=session
            )
            player: Player  # can be cleaner probably
            latency = time.time() - start_time

            # Record latency, labeled by proxy
            latency_histogram.labels(proxy=_proxy).observe(latency)

            if error is not None:
                # increment error counter
                error_counter.labels(proxy=_proxy).inc()
                await error_queue.put(data)
                continue

            if highscore is None:
                # increment not found counter
                not_found_counter.labels(proxy=_proxy).inc()
                await runemetrics_send_queue.put(player.dict())
            else:
                # Increment success counter
                success_counter.labels(proxy=_proxy).inc()
                await send_queue.put({"player": player.dict(), "hiscores": highscore})
    logger.info("shutdown")


async def get_proxies() -> list:
    webshare = Webshare(api_key=AppConfig().PROXY_API_KEY)
    proxy_list = await webshare.get_proxies()
    logger.info(f"gathered {len(proxy_list)} proxies")
    return proxy_list


async def main():
    proxy_list = await get_proxies()

    shutdown_event = Event()
    consumer = await _kafka.kafka_consumer(topic="player", group="scraper")
    producer = await _kafka.kafka_producer()

    receive_queue = Queue(maxsize=500)
    send_queue = Queue(maxsize=100)
    error_queue = Queue(maxsize=500)
    runemetrics_send_queue = Queue(maxsize=100)

    asyncio.create_task(
        _kafka.receive_messages(
            consumer=consumer,
            receive_queue=receive_queue,
            shutdown_event=shutdown_event,
        )
    )

    asyncio.create_task(
        _kafka.send_messages(
            topic="scraper",
            producer=producer,
            send_queue=send_queue,
            shutdown_event=shutdown_event,
        )
    )

    asyncio.create_task(
        _kafka.send_messages(
            topic="player",
            producer=producer,
            send_queue=error_queue,
            shutdown_event=shutdown_event,
        )
    )

    asyncio.create_task(
        _kafka.send_messages(
            topic="scraper-runemetrics",
            producer=producer,
            send_queue=runemetrics_send_queue,
            shutdown_event=shutdown_event,
        )
    )

    tasks = []
    for proxy in proxy_list:
        task = asyncio.create_task(
            process_messages(
                send_queue=send_queue,
                receive_queue=receive_queue,
                error_queue=error_queue,
                shutdown_event=shutdown_event,
                proxy=proxy,
                runemetrics_send_queue=runemetrics_send_queue,
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
