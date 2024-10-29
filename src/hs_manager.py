import asyncio
import json
import logging

from aiohttp import ClientResponseError, ClientSession
from aiokafka import AIOKafkaConsumer
from osrs.async_api.osrs.hiscores import Hiscore, Mode, RateLimiter
from osrs.exceptions import PlayerDoesNotExist, Undefined, UnexpectedRedirection
from pydantic import BaseModel

from config.config import app_config
from modules.api.webshare_api import Webshare

logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_TOPIC_HIGHSCORE = "player"


class Player(BaseModel):
    id: int
    name: str
    created_at: str
    updated_at: str | None
    possible_ban: int
    confirmed_ban: int
    confirmed_player: int
    label_id: int
    label_jagex: int


class HighscoreWorkerManager:
    """Manager for highscore worker operations, including Kafka and worker setup."""

    def __init__(self, proxies, kafka_servers, kafka_topic, kafka_group):
        self.proxies = proxies
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.kafka_group = kafka_group
        self.player_queue = asyncio.Queue(maxsize=10)
        self.does_not_exist_queue = asyncio.Queue()  # Track players that don't exist
        self.semaphore = asyncio.Semaphore(10)  # Adjust based on requirements

    async def fetch_player_stats(
        self, session: ClientSession, player: Player, hiscore_instance: Hiscore
    ):
        """Fetch stats for a player using the provided hiscore instance."""
        username = player.name
        try:
            pstats = await hiscore_instance.get(
                mode=Mode.OLDSCHOOL,
                player=username,
                session=session,
            )
            l_activities = len([a for a in pstats.activities if a.score > 0])
            l_skills = len([s for s in pstats.skills if s.xp > 0])
            logger.info(f"Fetched stats for {username} {l_skills=}, {l_activities=}")

        except UnexpectedRedirection as e:
            # Highscore page is down, requeue the player for retry
            logger.error(f"Highscore page down for {username}: {e}. Retrying in 120s.")
            await asyncio.sleep(120)
            await self.player_queue.put(player)  # Requeue the player for retry

        except PlayerDoesNotExist:
            logger.info(f"Player {username} does not exist.")
            await self.does_not_exist_queue.put(player)  # Track as non-existent

        except Undefined as e:
            # Fail loudly for undefined errors
            logger.error(f"Undefined error for {username}: {e}")
            raise e

        except ClientResponseError as e:
            # Fail loudly for client response errors
            logger.error(f"Client response error for {username}: {e}")
            raise e

    async def highscore_worker(self, proxy):
        """Worker for processing highscore data using a specific proxy."""
        limiter = RateLimiter(calls_per_interval=100, interval=60)
        hiscore_instance = Hiscore(proxy=proxy, rate_limiter=limiter)

        async with ClientSession() as session:
            while True:
                player = await self.player_queue.get()
                if player is None:  # Stop signal
                    break
                async with self.semaphore:
                    await self.fetch_player_stats(session, player, hiscore_instance)
                self.player_queue.task_done()

    async def setup_kafka_consumer(self):
        """Initialize and return a Kafka consumer for highscore usernames."""
        consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.kafka_group,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
        )
        await consumer.start()
        return consumer

    async def start_highscore_workers(self):
        """Start highscore worker tasks for each proxy."""
        return [
            asyncio.create_task(self.highscore_worker(proxy)) for proxy in self.proxies
        ]

    async def consume_usernames(self, consumer):
        """Consume usernames from Kafka and add them to the queue."""
        async for msg in consumer:
            data = msg.value
            player = Player(**data)
            await self.player_queue.put(player)

    async def stop_highscore_workers(self):
        """Signal all highscore workers to stop by adding None to the queue."""
        for _ in range(len(self.proxies)):
            await self.player_queue.put(None)

    async def run(self):
        """Main method to manage highscore workers and username consumption."""
        consumer = await self.setup_kafka_consumer()

        try:
            # Start highscore workers
            tasks = await self.start_highscore_workers()

            # Consume usernames from Kafka and add them to the queue
            await self.consume_usernames(consumer)

            # Signal workers to stop
            await self.stop_highscore_workers()

            # Wait for all tasks to complete
            await self.player_queue.join()
            await asyncio.gather(*tasks)

        finally:
            await consumer.stop()


async def main():
    proxy_list = await Webshare(api_key=app_config.PROXY_API_KEY).get_proxies()
    logger.info(f"gathered {len(proxy_list)} proxies")

    # Initialize the HighscoreWorkerManager with necessary parameters
    highscore_manager = HighscoreWorkerManager(
        proxies=proxy_list,
        kafka_servers=[app_config.KAFKA_HOST],
        kafka_topic=KAFKA_TOPIC_HIGHSCORE,
        kafka_group="scraper",
    )

    # Run the highscore worker manager
    await highscore_manager.run()


# Run the asynchronous main function
if __name__ == "__main__":
    asyncio.run(main())
