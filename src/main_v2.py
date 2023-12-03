import asyncio
import json
import logging
import signal
import sys
import time
import traceback
import uuid
from asyncio import Queue
from contextlib import asynccontextmanager

import requests
from aiohttp import (
    ClientHttpProxyError,
    ClientResponseError,
    ClientProxyConnectionError,
    ClientSession,
    ClientTimeout,
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
from aiokafka.errors import KafkaConnectionError
from pydantic import BaseModel

from config.config import app_config
from modules.scraper import Scraper
from modules.validation.player import Player
from utils.http_exception_handler import InvalidResponse

logger = logging.getLogger(__name__)

# Create an asyncio.Event for the shutdown signal
shutdown_event = asyncio.Event()


class Ports(BaseModel):
    http: int
    socks5: int


class Proxy(BaseModel):
    username: str
    password: str
    proxy_address: str
    ports: Ports


def get_proxies():
    URL = "https://proxy.webshare.io/api/proxy/list/"
    headers = {
        "Authorization": f"Token {app_config.PROXY_API_KEY}",
    }
    all_results = []  # To store results from all pages
    next_url = URL  # Initialize with the first page URL

    while next_url:
        response = requests.get(next_url, headers=headers)
        if response.status_code == 200:
            proxies = response.json()
            results = proxies.get("results", [])
            results = [Proxy(**r) for r in results]
            page_results = [
                f"http://{r.username}:{r.password}@{r.proxy_address}:{r.ports.http}"
                for r in results
            ]
            all_results.extend(page_results)
            next = proxies.get("next")
            next_url = f"https://proxy.webshare.io{next}" if next else None
            logger.info(f"{next_url=}")
        else:
            logger.error(
                f"Failed to retrieve proxies. Status code: {response.status_code}"
            )
            return None  # Or handle the error as per your application's requirements

    return all_results


@asynccontextmanager
async def kafka_player_consumer():
    TOPIC = "player"
    GROUP = "scraper"

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=[app_config.KAFKA_HOST],
        group_id=GROUP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    try:
        await consumer.start()
        yield consumer
    finally:
        await consumer.stop()


@asynccontextmanager
async def kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=[app_config.KAFKA_HOST],
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    try:
        await producer.start()
        yield producer
    finally:
        await producer.stop()


def log_speed(counter: int, start_time: float, _queue: Queue) -> tuple[float, int]:
    end_time = time.time()
    delta_time = end_time - start_time
    speed = counter / delta_time
    logger.info(
        f"qsize={_queue.qsize()}, processed {counter} in {delta_time:.2f} seconds, {speed:.2f} msg/sec"
    )
    return time.time(), 0


async def scrape_data(
    player_receive_queue: Queue,
    player_send_queue: Queue,
    scraper_send_queue: Queue,
    proxy: str,
    shutdown_event: asyncio.Event,
):
    error_count = 0
    name = str(uuid.uuid4())[-8:]
    scrape_counter = 0
    start_time = time.time()
    scraper = Scraper(proxy=proxy, worker_name=name)
    async with ClientSession(
        timeout=ClientTimeout(total=app_config.SESSION_TIMEOUT)
    ) as session:
        while True:
            if shutdown_event.is_set():
                break
            if player_receive_queue.empty():
                # logger.info(f"{name=} - receive queue is empty")
                await asyncio.sleep(5)
                continue

            if error_count > 5:
                logger.warning(f"high error count: {error_count}, killing task")
                break

            player = await player_receive_queue.get()
            player_receive_queue.task_done()

            player = Player(**player)
            # logger.info(f"Scraping data for player: {player.name}")
            try:
                player, hiscore = await scraper.lookup_hiscores(player, session)
                player: Player
                hiscore: dict
                # logger.info(f"Scraped data for player: {player.name}")
                scrape_counter += 1
                if scrape_counter == 100:
                    end_time = time.time()
                    logger.info(
                        f"Scraped 100 players in {end_time - start_time} seconds"
                    )
                    start_time = time.time()
                    scrape_counter = 0
            except InvalidResponse as _:
                error_type = type(error)
                sleep_time = max(error_count * 2, 5)
                await player_send_queue.put(item=player.dict())
                await asyncio.sleep(sleep_time)
                continue
            except (
                ClientResponseError,
                ClientHttpProxyError,
                ClientProxyConnectionError,
            ) as error:
                session = ClientSession(
                    timeout=ClientTimeout(total=app_config.SESSION_TIMEOUT)
                )
                error_type = type(error)
                sleep_time = 35
                logger.error(
                    f"{name} - {error_type.__name__}: {str(error)} - {error_count=} - {player.name=}"
                )
                await player_send_queue.put(item=player.dict())
                await asyncio.sleep(sleep_time)
                continue
            except Exception as error:
                error_type = type(error)
                sleep_time = max(error_count * 2, 5)
                logger.error(
                    {
                        "name": name,
                        "error_type": error_type.__name__,
                        "error": error,
                        "error_count": error_count,
                        "player_name": player.name,
                    }
                )
                tb_str = traceback.format_exc()
                logger.error(f"{error}, \n{tb_str}")
                await player_send_queue.put(item=player.dict())
                await asyncio.sleep(sleep_time)
                continue

        data = {"player": player.dict(), "hiscores": hiscore}
        await scraper_send_queue.put(item=data)
        error_count = 0
    return


async def receive_messages(
    consumer: AIOKafkaConsumer,
    receive_queue: Queue,
    shutdown_event: asyncio.Event,
    batch_size: int = 200,
):
    while True:
        if shutdown_event.is_set():
            break
        batch = await consumer.getmany(timeout_ms=1000, max_records=batch_size)
        for tp, messages in batch.items():
            logger.info(f"Partition {tp}: {len(messages)} messages")
            await asyncio.gather(*[receive_queue.put(m.value) for m in messages])
            logger.info("done")
            await consumer.commit()


async def send_messages(
    topic: str,
    producer: AIOKafkaProducer,
    send_queue: Queue,
    shutdown_event: asyncio.Event,
):
    start_time = time.time()
    messages_sent = 0

    while True:
        if shutdown_event.is_set():
            break
        if send_queue.empty():
            if messages_sent > 0:
                start_time, messages_sent = log_speed(
                    counter=messages_sent, start_time=start_time, _queue=send_queue
                )
            await asyncio.sleep(1)
            continue
        message = await send_queue.get()
        await producer.send(topic, value=message)
        send_queue.task_done()

        if topic == "scraper":
            messages_sent += 1

        if topic == "scraper" and messages_sent >= 100:
            start_time, messages_sent = log_speed(
                counter=messages_sent, start_time=start_time, _queue=send_queue
            )


async def shutdown_sequence(
    player_receive_queue, player_send_queue, scraper_send_queue, producer
):
    while not player_receive_queue.empty():
        message = await player_receive_queue.get()
        await producer.send("player", value=message)

    while not player_send_queue.empty():
        message = await player_send_queue.get()
        await producer.send("player", value=message)

    while not scraper_send_queue.empty():
        message = await scraper_send_queue.get()
        await producer.send("scraper", value=message)

    # if for some reason all tasks are completed shutdown
    await producer.stop()
    logger.info("Shutdown complete")


async def main():
    player_receive_queue = Queue(maxsize=500)
    player_send_queue = Queue(maxsize=100)
    scraper_send_queue = Queue(maxsize=500)

    def signal_handler(signum, frame):
        logger.info("Initiating shutdown sequence...")
        loop = asyncio.get_event_loop()
        loop.create_task(
            shutdown_sequence(
                player_receive_queue, player_send_queue, scraper_send_queue, producer
            )
        )
        shutdown_event.set()

    # Register the signal handler
    signal.signal(signal.SIGTERM, signal_handler)

    # get kafka engine
    async with kafka_player_consumer() as player_consumer, kafka_producer() as producer:
        player_receive_queue = Queue(maxsize=500)
        player_send_queue = Queue(maxsize=100)
        scraper_send_queue = Queue(maxsize=500)

        asyncio.create_task(
            receive_messages(player_consumer, player_receive_queue, shutdown_event)
        )
        asyncio.create_task(
            send_messages(
                topic="player",
                producer=producer,
                send_queue=player_send_queue,
                shutdown_event=shutdown_event,
            )
        )
        asyncio.create_task(
            send_messages(
                topic="scraper",
                producer=producer,
                send_queue=player_send_queue,
                shutdown_event=shutdown_event,
            )
        )

        # get proxies
        proxy_list = get_proxies()
        logger.info(f"gathered {len(proxy_list)} proxies")

        if not proxy_list:
            logger.error("No proxies available. Exiting.")
            return

        # for each proxy create a task
        tasks = []
        for proxy in proxy_list:
            task = asyncio.create_task(
                scrape_data(
                    player_receive_queue=player_receive_queue,
                    player_send_queue=player_send_queue,
                    scraper_send_queue=scraper_send_queue,
                    proxy=proxy,
                    shutdown_event=shutdown_event,
                )
            )
            tasks.append(task)

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for exceptions
            exceptions = [result for result in results if isinstance(result, Exception)]

            if exceptions:
                # Handle the exceptions
                for exception in exceptions:
                    print(f"Task failed with exception: {exception}")
        finally:
            await shutdown_sequence(
                player_receive_queue=player_receive_queue,
                player_send_queue=player_send_queue,
                scraper_send_queue=scraper_send_queue,
                producer=producer,
            )

    # if for some reason all tasks are completed shutdown
    await producer.stop()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except RuntimeError:
        asyncio.run(main())
    except SystemExit:
        logger.info("SystemExit")
        sys.exit(0)
