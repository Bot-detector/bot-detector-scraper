import asyncio
import json
import logging
from asyncio import Queue

from aiokafka import AIOKafkaConsumer, ConsumerRecord

from config.config import app_config
from modules.proxy_manager import ProxyManager
from modules.worker import Worker, WorkerState
from modules.validation.player import Player
import random
from multiprocessing import Process
import time

logger = logging.getLogger(__name__)

class PlayerConsumer:
    def __init__(self, proxies):
        self.group_id = "scraper-v2"
        self.topics = ["player"]
        self.proxies = proxies
        self.workers:list[Worker]

    async def consume_message(self, msg:ConsumerRecord):
        player = msg.value.decode()
        player = json.loads(player)
        player = Player(**player)

        available_workers = [w for w in self.workers if w.state != WorkerState.BROKEN]
        if available_workers:
            worker = random.choice(available_workers)
            asyncio.ensure_future(worker.scrape_player(player=player))
        else:
            logger.warning("No available workers to process the message")

    async def consumer_loop(self, consumer:AIOKafkaConsumer):
        try:
            iterations = 0
            start_time = time.time()
            async for msg in consumer:
                await self.consume_message(msg)
                await consumer.commit()
                # Increment the iteration count
                iterations += 1
 

                # Calculate iterations per second and log it every 100 iterations
                if iterations % 100 == 0:
                    # Calculate elapsed time since start
                    elapsed_time = time.time() - start_time
                    iterations_per_second = iterations / elapsed_time
                    logger.info(f"{iterations_per_second:.2f} it/s")
        
        except Exception as e:
            logger.error(f"Exception occurred: {e}")

        finally:
            await consumer.stop()

    async def consumer(self):
        logger.info(f"{app_config.ENDPOINT=}")
        logger.info(f"{app_config.KAFKA_HOST=}")

        consumer = AIOKafkaConsumer(
            bootstrap_servers=app_config.KAFKA_HOST,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        consumer.subscribe(topics=self.topics)

        logger.info("initializing workers")
        self.workers = await asyncio.gather(*[Worker(proxy).initialize() for proxy in self.proxies])

        logger.info("consuming messages")
        await consumer.start()
        await self.consumer_loop(consumer)

async def get_proxies():
    proxy_manager = ProxyManager(app_config.PROXY_DOWNLOAD_URL)
    proxies = await proxy_manager.get_proxy_list()

    assert isinstance(proxies, list), "proxies must be a list"
    assert all(isinstance(p, str) for p in proxies), "proxies must contain only strings"
    logger.info(f"got {len(proxies)}")
    return proxies

def batchify_list(input_list: list, batch_size: int) -> list[list]:
    return [
        input_list[i : i + batch_size] for i in range(0, len(input_list), batch_size)
    ]

def run_player_consumer(proxies:list):
    asyncio.run(PlayerConsumer(proxies).consumer())

def create_and_run_processes(proxy_batches: list[list]):
    processes: list[Process] = []

    for batch in proxy_batches:
        process = Process(target=run_player_consumer, args=(batch,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

async def main():
    logger.info(f"{app_config.ENDPOINT=}")
    logger.info(f"{app_config.KAFKA_HOST=}")

    BATCH_SIZE = 50
    proxies = await get_proxies()
    proxy_batches = batchify_list(proxies, BATCH_SIZE)

    create_and_run_processes(proxy_batches)

    # logger.info("initializing consumer")
    # consumer = AIOKafkaConsumer(
    #     bootstrap_servers=app_config.KAFKA_HOST,
    #     group_id=group_id,
    #     auto_offset_reset="earliest",
    #     enable_auto_commit=False,
    # )
    # consumer.subscribe(topics=topics)

    # logger.info("consuming messages")
    # await consumer.start()
    # try:
    #     async for msg in consumer:
    #         player = msg.value.decode()
    #         player = json.loads(player)
    #         player = Player(**player)
    #         available_workers = [w for w in workers if w.state != WorkerState.BROKEN]
    #         worker: Worker = random.choice(available_workers)
    #         # logger.info(f"{worker.name}-{player.name}")
    #         asyncio.ensure_future(worker.scrape_player(player=player))
    #         await consumer.commit()

    # except Exception as e:
    #     logger.error(e)
    #     asyncio.ensure_future(main())
    # finally:
    #     await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
