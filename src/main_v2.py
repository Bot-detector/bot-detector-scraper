from config.config import app_config
from pydantic import BaseModel
from modules.validation.player import Player
import asyncio
from modules.scraper import Scraper
import uuid
from aiohttp import ClientSession
import logging
import requests
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json
from asyncio import Queue
from time import time

logger = logging.getLogger(__name__)


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
    response = requests.get(URL, headers=headers)
    results = None
    if response.status_code == 200:
        proxies = response.json()
        results = proxies["results"]
        results = [Proxy(**r) for r in results]
        results = [
            f"http://{r.username}:{r.password}@{r.proxy_address}:{r.ports.http}"
            for r in results
        ]
    else:
        logger.error(f"Failed to retrieve proxies. Status code: {response.status_code}")
    return results


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


async def scrape_data(
    player_receive_queue: Queue,
    player_send_queue: Queue,
    scraper_send_queue: Queue,
    proxy: str,
):
    error_count = 0
    name = str(uuid.uuid4())[-8:]
    scraper = Scraper(proxy=proxy, worker_name=name)
    session = ClientSession(timeout=app_config.SESSION_TIMEOUT)

    while True:
        if player_receive_queue.empty():
            logger.info(f"{name=} - receive queue is empty")
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
        except Exception as error:
            error_type = type(error)
            sleep_time = max(error_count * 2, 5)
            logger.error(
                f"{name} - {error_type.__name__}: {str(error)} - {error_count=} - {player.name=}"
            )
            await player_send_queue.put(item=player.dict())
            await asyncio.sleep(sleep_time)
            continue

        data = {"player": player.dict(), "hiscores": hiscore}
        await scraper_send_queue.put(item=data)
        error_count = 0
    return


async def receive_messages(consumer: AIOKafkaConsumer, receive_queue: Queue):
    async for message in consumer:
        value = message.value
        await receive_queue.put(value)


async def send_messages(topic: str, producer: AIOKafkaProducer, send_queue: Queue):
    last_interval = time()
    messages_sent = 0

    while True:
        if send_queue.empty():
            await asyncio.sleep(1)
        message = await send_queue.get()
        await producer.send(topic, value=message)
        send_queue.task_done()

        if topic == "scraper":
            messages_sent += 1

        if topic == "scraper" and messages_sent >= 100:
            current_time = time()
            elapsed_time = current_time - last_interval
            speed = messages_sent / elapsed_time
            logger.info(
                f"processed {messages_sent} in {elapsed_time:.2f} seconds, {speed:.2f} msg/sec"
            )

            last_interval = time()
            messages_sent = 0


async def main():
    # get kafka engine
    player_consumer = await kafka_player_consumer()
    producer = await kafka_producer()

    player_receive_queue = Queue(maxsize=100)
    player_send_queue = Queue(maxsize=100)
    scraper_send_queue = Queue(maxsize=100)

    asyncio.create_task(receive_messages(player_consumer, player_receive_queue))
    asyncio.create_task(
        send_messages(topic="player", producer=producer, send_queue=player_send_queue)
    )
    asyncio.create_task(
        send_messages(topic="scraper", producer=producer, send_queue=scraper_send_queue)
    )

    # get proxies
    proxy_list = get_proxies()

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
            )
        )
        tasks.append(task)
    # await task for completion (never)
    await asyncio.gather(*tasks, return_exceptions=True)

    # if for some reason all tasks are completed shutdown
    await player_consumer.stop()
    await producer.stop()


if __name__ == "__main__":
    app_config.KAFKA_HOST = "localhost:9094"
    try:
        loop = asyncio.get_running_loop()
        loop.run_until_complete(main())
    except RuntimeError:
        asyncio.run(main())
