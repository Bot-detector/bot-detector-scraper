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


hiscore_mapper = {
    "league_points": "league",
    "clue_scrolls_all": "cs_all",
    "clue_scrolls_beginner": "cs_beginner",
    "clue_scrolls_easy": "cs_easy",
    "clue_scrolls_medium": "cs_medium",
    "clue_scrolls_hard": "cs_hard",
    "clue_scrolls_elite": "cs_elite",
    "clue_scrolls_master": "cs_master",
    "theatre_of_blood_hard_mode": "theatre_of_blood_hard",
    "tombs_of_amascut_expert_mode": "tombs_of_amascut_expert",
}


class HighscoreWorkerManager:
    def __init__(self, proxies, kafka_servers, kafka_topic, kafka_group):
        self.proxies = proxies
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.kafka_group = kafka_group
        self.player_queue = asyncio.Queue(maxsize=100)
        self.player_not_found_queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(value=25)
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
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all",
        )
        await self.kafka_producer.start()

    async def produce_message(self, topic: str, msg: dict):
        """Produce player data to Kafka."""
        if not self.kafka_producer:
            logger.error("Kafka Producer is None")
            raise Exception("Kafka Producer is None")

        if not isinstance(msg, dict):
            logger.error(f"Expected type dict, got {type(msg)}")
            raise Exception(f"Expected type dict, got {type(msg)}")

        await self.kafka_producer.send(
            topic=topic,
            value=msg,
        )

    async def produce_player(self, topic: str, player: Player):
        """Produce player back to Kafka topic."""

        topics = ["player", "scraper-runemetrics"]
        if topic not in topics:
            err = f"Unsupported Topic, received: {topic} expected value in {topics=}"
            logger.error(err)
            raise Exception(err)

        if not isinstance(player, Player):
            err = f"Expected type Player, Received: {type(player)}, {player=}"
            logger.error(err)
            raise Exception(err)

        await self.produce_message(
            topic=topic,
            msg=player.model_dump(),
        )

    async def consume_player(self):
        if self.kafka_consumer is None:
            raise Exception("Kafka Consumer is None")

        async for msg in self.kafka_consumer:
            player = Player(**msg.value)
            await self.player_queue.put(player)

    def _parse_hiscore_name(self, name: str) -> str:
        name = name.lower()
        name = name.replace("'", "")
        name = name.replace(" - ", " ")
        name = name.replace("-", "_")
        name = name.replace(":", "")
        name = name.replace("(", "").replace(")", "")
        name = name.replace(" ", "_")
        # replaces "name" with its corresponding abbreviation from "hiscore_mapper" dictionary,
        # if one exists, or keeps the original name if it does not
        name = hiscore_mapper.get(name, name)
        return name

    async def fetch_player_stats(
        self, session: ClientSession, player: Player, hiscore_instance: Hiscore
    ):
        proxy = session._proxy
        try:
            total_counter.inc()
            start_time = time.time()
            pstats = await hiscore_instance.get(
                mode=Mode.OLDSCHOOL,
                player=player.name,
                session=session,
            )
            latency = time.time() - start_time

            # we know the player is not banned if he is on the highscores
            player.possible_ban = 0
            player.confirmed_ban = 0
            player.label_jagex = 0

            # parse data into dict
            activities = {
                self._parse_hiscore_name(a.name): a.score
                for a in pstats.activities
                if a.score >= 0
            }

            skills = {
                self._parse_hiscore_name(s.name): s.xp
                for s in pstats.skills
                if s.xp >= 0 and s.name != "Overall"
            }

            ## merge into one dict
            hiscore = activities | skills | {"total": sum(skills.values())}

            # send data to the scraper
            msg = {"player": player.model_dump(), "hiscore": hiscore}
            await self.produce_message(topic="scraper", msg=msg)

            # Increment success counter
            success_counter.labels(proxy=proxy).inc()

            # Record latency, labeled by proxy
            latency_histogram.labels(proxy=proxy).observe(latency)
        except PlayerDoesNotExist:
            player.possible_ban = 1
            await self.produce_player(topic="scraper-runemetrics", player=player)
            not_found_counter.labels(proxy=proxy).inc()
            return

        except UnexpectedRedirection as e:
            error_counter.labels(proxy=proxy).inc()
            logger.error(f"Highscore page down for {player.name}: {e}")
            await self.produce_player(topic=KAFKA_TOPIC_HIGHSCORE, player=player)
            await asyncio.sleep(120)
            return

        except Undefined as e:
            error_counter.labels(proxy=proxy).inc()
            await self.produce_player(topic=KAFKA_TOPIC_HIGHSCORE, player=player)
            logger.error(e)
            raise Exception(e)  # Fail loud

        except ClientResponseError as e:
            error_counter.labels(proxy=proxy).inc()
            await self.produce_player(topic=KAFKA_TOPIC_HIGHSCORE, player=player)
            logger.error(e)
            raise Exception(e)  # Fail loud
        # somehow i need this
        finally:
            return

    async def worker(self, proxy):
        limiter = RateLimiter(calls_per_interval=60, interval=60)
        hiscore_instance = Hiscore(proxy=proxy, rate_limiter=limiter)

        async with ClientSession() as session:
            # Store proxy for latency tracking
            session._proxy = proxy.split("@")[1]

            while True:
                player = await self.player_queue.get()
                self.player_queue.task_done()

                # TODO: Stop signal
                if player is None:
                    logger.info("break")
                    break

                async with self.semaphore:
                    asyncio.create_task(
                        self.fetch_player_stats(session, player, hiscore_instance)
                    )

    async def start_workers(self):
        self.tasks = [asyncio.create_task(self.worker(p)) for p in self.proxies]

    async def stop_workers(self):
        logger.info("Stopping Workers")
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
