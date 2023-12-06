import asyncio
import json
import logging
import time
import traceback
import uuid
from asyncio import Queue
import sys

import requests
from aiohttp import (
    ClientHttpProxyError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

from config.config import app_config
from modules.api.webshare_api import Webshare
from modules.gracefull_shutdown import GracefulShutdown
import argparse
from modules.highscore_scraper import HighScoreScraper
from modules.runemetrics_scraper import RuneMetricsScraper

from modules.validation.player import Player
from utils.http_exception_handler import InvalidResponse

logger = logging.getLogger(__name__)


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
    await consumer.start()
    return consumer


async def kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=[app_config.KAFKA_HOST],
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()
    return producer


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
    scraper: Scraper,
):
    error_count = 0
    name = str(uuid.uuid4())[-8:]
    scraper = Scraper(proxy=proxy, worker_name=name)
    session = ClientSession(timeout=ClientTimeout(total=app_config.SESSION_TIMEOUT))

    while not shutdown_event.is_set():
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
        try:
            player, hiscore = await scraper.lookup_hiscores(player, session)
            player: Player
            hiscore: dict
        except InvalidResponse as _:
            error_type = type(error)
            sleep_time = max(error_count * 2, 5)
            await player_send_queue.put(item=player.dict())
            await asyncio.sleep(sleep_time)
            continue
        except (ClientResponseError, ClientHttpProxyError) as error:
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
    else:
        await session.close()
        logger.info(f"proxy: {name}, shutdown")


async def receive_messages(
    consumer: AIOKafkaConsumer,
    receive_queue: Queue,
    shutdown_event: asyncio.Event,
    batch_size: int = 200,
):
    while not shutdown_event.is_set():
        batch = await consumer.getmany(timeout_ms=1000, max_records=batch_size)
        for tp, messages in batch.items():
            logger.info(f"Partition {tp}: {len(messages)} messages")
            await asyncio.gather(*[receive_queue.put(m.value) for m in messages])
            logger.info("done")
            await consumer.commit()
    else:
        logger.info("shutdown")


async def send_messages(
    topic: str,
    producer: AIOKafkaProducer,
    send_queue: Queue,
    shutdown_event: asyncio.Event,
):
    start_time = time.time()
    messages_sent = 0

    while not shutdown_event.is_set():
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
    else:
        logger.info("shutdown")


async def shutdown_sequence(
    player_receive_queue: Queue,
    player_send_queue: Queue,
    scraper_send_queue: Queue,
    producer: AIOKafkaProducer,
    consumer: AIOKafkaConsumer,
):
    logger.info("Starting shutdown sequence...")
    queues = [
        {
            "queue": player_receive_queue,
            "topic": "player",
            "name": "player_receive_queue",
        },
        {"queue": player_send_queue, "topic": "player", "name": "player_send_queue"},
        {
            "queue": scraper_send_queue,
            "topic": "scraper",
            "name": "scraper_send_queue",
        },
    ]
    for q_config in queues:
        queue: Queue = q_config.get("queue")
        topic = q_config.get("topic")
        name = q_config.get("name")

        logger.info(f"Queue, {name=}, qsize={queue.qsize()}, sending to {topic=}")
        while not queue.empty():
            message = await queue.get()
            await producer.send(topic=topic, value=message)

    # if for some reason all tasks are completed shutdown
    await producer.stop()
    logger.info("Producer stopped")

    await consumer.stop()
    logger.info("Consumer stopped")


async def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scraper", choices=["highscore", "runemetrics"], required=True
    )
    args = parser.parse_args()

    # Create the appropriate scraper based on the command-line argument
    if args.scraper == "highscore":
        uid = uuid.uuid4()  # Generate a random UUID
        worker_name = f"highscore_scraper_{uid}"
        scraper = HighScoreScraper(proxy=proxy, worker_name=worker_name)
    elif args.scraper == "runemetrics":
        uid = uuid.uuid4()  # Generate a random UUID
        worker_name = f"runemetrics_scraper_{uid}"
        scraper = RuneMetricsScraper(proxy=proxy, worker_name=worker_name)
    else:
        print(f"Invalid scraper: {args.scraper}")
        sys.exit(1)

    shutdown_event = asyncio.Event()
    # get kafka engine
    player_consumer = await kafka_player_consumer()
    producer = await kafka_producer()

    player_receive_queue = Queue(maxsize=500)
    player_send_queue = Queue(maxsize=100)
    scraper_send_queue = Queue(maxsize=500)

    killer = GracefulShutdown(
        shutdown_event=shutdown_event,
        shutdown_sequence=shutdown_sequence(
            player_receive_queue=player_receive_queue,
            player_send_queue=player_send_queue,
            scraper_send_queue=scraper_send_queue,
            producer=producer,
            consumer=player_consumer,
        ),
    )

    asyncio.create_task(
        receive_messages(
            consumer=player_consumer,
            receive_queue=player_receive_queue,
            shutdown_event=shutdown_event,
        )
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
            send_queue=scraper_send_queue,
            shutdown_event=shutdown_event,
        )
    )

    # get proxies
    webshare = Webshare(api_key=app_config.PROXY_API_KEY)
    proxy_list = await webshare.get_proxies()
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
                scraper=scraper,
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
