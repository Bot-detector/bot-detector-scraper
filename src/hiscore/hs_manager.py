import asyncio
import json
import logging
import time

from aiohttp import ClientResponseError, ClientSession
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from osrs.async_api.osrs.hiscores import Hiscore, Mode, RateLimiter
from osrs.exceptions import PlayerDoesNotExist, Undefined, UnexpectedRedirection
from prometheus_client import Counter, Histogram, start_http_server
from pydantic import BaseModel

from config.config import app_config
from modules.api.webshare_api import Webshare

logger = logging.getLogger(__name__)

KAFKA_TOPIC_HIGHSCORE = "player"

# Define Prometheus metrics
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


class Player(BaseModel):
    id: int
    name: str
    created_at: str | None
    updated_at: str | None
    possible_ban: int
    confirmed_ban: int
    confirmed_player: int
    label_id: int
    label_jagex: int


class HighscoreWorkerManager:
    def __init__(self, proxies, kafka_servers, kafka_topic, kafka_group):
        self.proxies = proxies
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.kafka_group = kafka_group
        self.player_queue = asyncio.Queue(maxsize=10)
        self.player_not_found_queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(len(proxies))
        self.kafka_producer: AIOKafkaProducer = None
        self.kafka_consumer: AIOKafkaConsumer = None

    async def setup_kafka_consumer(self):
        if self.kafka_consumer is not None:
            logger.warning("Consumer already started")
            return

        self.kafka_consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.kafka_group,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
        )
        await self.kafka_consumer.start()

    async def setup_kafka_producer(self):
        if self.kafka_producer is not None:
            logger.warning("Producer already started")
            return

        self.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_servers,
        )
        await self.kafka_producer.start()

    async def produce_player(self, topic: str, player: Player):
        """Produce player back to Kafka topic."""
        if self.kafka_producer is None:
            raise Exception("Kafka Producer is None")

        topics = ("player", "scraper-runemetrics")
        if topic not in topics:
            err = f"Unsupported Topic, received: {topic} expected value in {topics=}"
            raise Exception(err)

        if not isinstance(player, Player):
            err = f"Expected type Player, Received: {type(player)}, {player=}"
            raise Exception(err)

        message = player.model_dump()
        await self.kafka_producer.send_and_wait(
            topic=topic,
            value=message.encode("utf-8"),
        )
        logger.info(f"Produced [{player.name}] Topic [{topic}].")

    async def consume_player(self):
        if self.kafka_consumer is None:
            raise Exception("Kafka Consumer is None")

        async for msg in self.kafka_consumer:
            player = Player(**msg.value)
            await self.player_queue.put(player)

    async def fetch_player_stats(
        self, session: ClientSession, player: Player, hiscore_instance: Hiscore
    ):
        username = player.name
        proxy = session._proxy
        try:
            start_time = time.time()  # Start timing
            pstats = await hiscore_instance.get(
                mode=Mode.OLDSCHOOL,
                player=username,
                session=session,
            )
            latency = time.time() - start_time

            # Process stats logic
            l_activities = len([a for a in pstats.activities if a.score > 0])
            l_skills = len([s for s in pstats.skills if s.xp > 0])
            logger.info(f"{username=} {l_skills=}, {l_activities=}")

            # Increment success counter
            success_counter.labels(proxy=proxy).inc()

            # Record latency, labeled by proxy
            latency_histogram.labels(proxy=proxy).observe(latency)

        except PlayerDoesNotExist:
            logger.info(f"{username=} does not exist.")
            player.possible_ban = 1
            not_found_counter.labels(proxy=proxy).inc()

        except UnexpectedRedirection as e:
            error_counter.labels(proxy=proxy).inc()
            logger.error(f"Highscore page down for {username}: {e}")
            await self.produce_player(topic="player", player=player)
            await asyncio.sleep(120)

        except Undefined as e:
            error_counter.labels(proxy=proxy).inc()
            await self.produce_player(topic="player", player=player)
            raise e  # Fail loud

        except ClientResponseError as e:
            error_counter.labels(proxy=proxy).inc()
            await self.produce_player(topic="player", player=player)
            raise e  # Fail loud

    async def highscore_worker(self, proxy):
        limiter = RateLimiter(calls_per_interval=100, interval=60)
        hiscore_instance = Hiscore(proxy=proxy, rate_limiter=limiter)

        async with ClientSession() as session:
            # Store proxy for latency tracking
            session._proxy = proxy.split("@")[1]
            while True:
                player = await self.player_queue.get()

                # TODO: Stop signal
                if player is None:
                    break

                async with self.semaphore:
                    await self.fetch_player_stats(session, player, hiscore_instance)
                self.player_queue.task_done()

    async def start_highscore_workers(self):
        return [
            asyncio.create_task(self.highscore_worker(proxy)) for proxy in self.proxies
        ]

    async def stop_highscore_workers(self):
        for _ in range(len(self.proxies)):
            await self.player_queue.put(None)

    async def run(self):
        await self.setup_kafka_consumer()
        await self.setup_kafka_producer()

        try:
            tasks = await self.start_highscore_workers()
            await self.consume_player()
            await self.stop_highscore_workers()
            await self.player_queue.join()
            await asyncio.gather(*tasks)
        finally:
            if self.kafka_consumer:
                await self.kafka_consumer.stop()
            if self.kafka_producer:
                await self.kafka_producer.stop()


async def main():
    proxy_list = await Webshare(api_key=app_config.PROXY_API_KEY).get_proxies()
    logger.info(f"gathered {len(proxy_list)} proxies")

    # Start Prometheus metrics server
    start_http_server(8000)

    highscore_manager = HighscoreWorkerManager(
        proxies=proxy_list,
        kafka_servers=[app_config.KAFKA_HOST],
        kafka_topic=KAFKA_TOPIC_HIGHSCORE,
        kafka_group="scraper",
    )

    await highscore_manager.run()


# Run the asynchronous main function
if __name__ == "__main__":
    asyncio.run(main())
