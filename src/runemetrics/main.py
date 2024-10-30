import asyncio
import json
import logging
import time

from aiohttp import ClientResponseError, ClientSession
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from osrs.async_api.osrs.hiscores import RateLimiter
from osrs.exceptions import Undefined, UnexpectedRedirection
from prometheus_client import Counter, Histogram, start_http_server
from pydantic import BaseModel

from config.config import app_config
from modules.api.webshare_api import Webshare
from runemetrics.api import RuneMetrics

logger = logging.getLogger(__name__)

KAFKA_TOPIC_RUNEMETRICS = "scraper-runemetrics"

# Prometheus metrics
success_counter = Counter(
    name="rune_metrics_success",
    documentation="Successful RuneMetrics requests",
    labelnames=["proxy"],
)
error_counter = Counter(
    name="rune_metrics_errors",
    documentation="Errors in RuneMetrics requests",
    labelnames=["proxy"],
)
latency_histogram = Histogram(
    name="rune_metrics_latency",
    documentation="Latency of RuneMetrics requests",
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


class RuneMetricsWorkerManager:
    def __init__(self, proxies, kafka_servers, kafka_topic, kafka_group):
        self.proxies = proxies
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.kafka_group = kafka_group
        self.player_queue = asyncio.Queue(maxsize=10)
        self.semaphore = asyncio.Semaphore(len(proxies))
        self.kafka_producer: AIOKafkaProducer = None
        self.kafka_consumer: AIOKafkaConsumer = None

    async def setup_kafka_consumer(self):
        if self.kafka_consumer:
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
        if self.kafka_producer:
            logger.warning("Producer already started")
            return

        self.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_servers,
        )
        await self.kafka_producer.start()

    async def produce_message(self, topic: str, msg: dict):
        """Produce player data to Kafka."""
        if not self.kafka_producer:
            raise Exception("Kafka Producer is None")

        if not isinstance(msg, dict):
            raise Exception(f"Expected type dict, got {type(msg)}")

        await self.kafka_producer.send_and_wait(
            topic=topic,
            value=msg,
        )

    async def produce_player(self, topic: str, player: Player):
        """Produce player data to Kafka."""
        topics = ["scraper-runemetrics"]
        if topic not in topics:
            err = f"Unsupported Topic, received: {topic} expected value in {topics=}"
            raise Exception(err)

        if not isinstance(player, Player):
            raise Exception(f"Expected type Player, got {type(player)}")

        await self.produce_message(
            topic=topic,
            value=player.model_dump(),
        )

    async def consume_player(self):
        """Consumes player data from Kafka."""
        if not self.kafka_consumer:
            raise Exception("Kafka Consumer is None")

        async for msg in self.kafka_consumer:
            player = Player(**msg.value)
            await self.player_queue.put(player)

    async def fetch_player_profile(
        self, session: ClientSession, player: Player, metrics_instance: RuneMetrics
    ):
        username = player.name
        proxy = session._proxy
        try:
            start_time = time.time()
            data: dict = await metrics_instance.get_profile(player_name=username)
            latency = time.time() - start_time

            # process logic
            player.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            match data.get("error"):
                # username is not associated to an account
                case "NO_PROFILE":
                    player.label_jagex = 1
                # account is perm banned
                case "NOT_A_MEMBER":
                    player.label_jagex = 2
                # runemetrics is set to private. either they're too low level or they're banned.
                case "PROFILE_PRIVATE":
                    player.label_jagex = 3
                # account is active, probably just too low stats for hiscores
                case _:
                    player.label_jagex = 0

            msg = {"player": player.model_dump()}
            await self.produce_message(topic="scraper", msg=msg)

            # Increment success counter
            success_counter.labels(proxy=proxy).inc()

            # Record latency, labeled by proxy
            latency_histogram.labels(proxy=proxy).observe(latency)

        except UnexpectedRedirection as e:
            error_counter.labels(proxy=proxy).inc()
            logger.error(f"Highscore page down for {username}: {e}")
            await self.produce_player(topic=KAFKA_TOPIC_RUNEMETRICS, player=player)
            await asyncio.sleep(120)
        except Undefined as e:
            error_counter.labels(proxy=proxy).inc()
            await self.produce_player(topic=KAFKA_TOPIC_RUNEMETRICS, player=player)
            logger.error(e)
            raise Exception(e)  # Fail loud

        except ClientResponseError as e:
            error_counter.labels(proxy=proxy).inc()
            await self.produce_player(topic=KAFKA_TOPIC_RUNEMETRICS, player=player)
            logger.error(e)
            raise Exception(e)  # Fail loud
        finally:
            return

    async def worker(self, proxy):
        limiter = RateLimiter(calls_per_interval=100, interval=60)
        hiscore_instance = RuneMetrics(proxy=proxy, rate_limiter=limiter)

        async with ClientSession() as session:
            # Store proxy for latency tracking
            session._proxy = proxy.split("@")[1]
            while True:
                player = await self.player_queue.get()
                self.player_queue.task_done()
                # TODO: Stop signal
                if player is None:
                    break

                async with self.semaphore:
                    await self.fetch_player_profile(session, player, hiscore_instance)

    async def start_workers(self):
        self.tasks = [asyncio.create_task(self.worker(p)) for p in self.proxies]

    async def stop_workers(self):
        await self.kafka_consumer.stop()
        self.kafka_consumer = None

        while any([t for t in self.tasks if t.done()]):
            await self.player_queue.put(None)
            l_tasks = len([t for t in self.tasks if t.done()])
            logger.info(f"Shutdown: running tasks: {l_tasks}")

    async def run(self):
        await self.setup_kafka_consumer()
        await self.setup_kafka_producer()

        try:
            await self.start_workers()
            await self.consume_player()
            await self.stop_workers()
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

    runemetrics_manager = RuneMetricsWorkerManager(
        proxies=proxy_list,
        kafka_servers=[app_config.KAFKA_HOST],
        kafka_topic=KAFKA_TOPIC_RUNEMETRICS,
        kafka_group="scraper",
    )

    await runemetrics_manager.run()


# Run the asynchronous main function
if __name__ == "__main__":
    asyncio.run(main())
