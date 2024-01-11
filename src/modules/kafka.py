import asyncio
import json
import logging
import time
import traceback
from asyncio import Event, Queue

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.config import app_config

logger = logging.getLogger(__name__)


import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


async def kafka_consumer(topic: str, group: str):
    logger.info(f"starting consumer, topic: {topic}, group: {group}")

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=[app_config.KAFKA_HOST],
                group_id=group,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
            )
            logger.info(await consumer.topics())
            await consumer.start()
            logger.info("started")
            return consumer
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {e}")
            retry_count += 1
            logger.info(f"Retrying Kafka connection ({retry_count}/{max_retries})...")
            await asyncio.sleep(5)  # Add a delay before retrying

    raise RuntimeError("Failed to connect to Kafka after multiple retries")


async def kafka_producer():
    logger.info(f"starting producer")

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=[app_config.KAFKA_HOST],
                value_serializer=lambda v: json.dumps(v).encode(),
                acks="all",
            )
            await producer.start()
            return producer
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {e}")
            retry_count += 1
            logger.info(f"Retrying Kafka connection ({retry_count}/{max_retries})...")
            await asyncio.sleep(5)  # Add a delay before retrying

    raise RuntimeError("Failed to connect to Kafka after multiple retries")


def log_speed(
    counter: int, start_time: float, _queue: Queue, topic: str
) -> tuple[float, int]:
    end_time = time.time()
    delta_time = end_time - start_time
    speed = counter / delta_time
    logger.info(
        f"{topic=}, qsize={_queue.qsize()}, processed {counter} in {delta_time:.2f} seconds, {speed:.2f} msg/sec"
    )
    return time.time(), 0


async def receive_messages(
    consumer: AIOKafkaConsumer,
    receive_queue: Queue,
    shutdown_event: Event,
    batch_size: int = 200,
):
    logger.info(f"start receiving messages, topics={await consumer.topics()}")
    while not shutdown_event.is_set():
        try:
            batch = await consumer.getmany(timeout_ms=1000, max_records=batch_size)
        except Exception as error:
            error_type = type(error)
            logger.error(
                {
                    "error_type": error_type.__name__,
                    "error": error,
                }
            )
            tb_str = traceback.format_exc()
            logger.error(f"{error}, \n{tb_str}")
            await asyncio.sleep(5)  # Add a delay before retrying
            continue  # Restart the loop

        for tp, messages in batch.items():
            logger.info(f"Partition {tp}: {len(messages)} messages")
            await asyncio.gather(*[receive_queue.put(m.value) for m in messages])
            logger.info("done")
            await consumer.commit()

    logger.info("stop receiving messages")


async def send_messages(
    topic: str,
    producer: AIOKafkaProducer,
    send_queue: Queue,
    shutdown_event: Event,
):
    start_time = time.time()
    messages_sent = 0
    logger.info(f"start sending messages, topic={topic}")
    while not shutdown_event.is_set():
        if send_queue.empty():
            if messages_sent > 0:
                start_time, messages_sent = log_speed(
                    counter=messages_sent,
                    start_time=start_time,
                    _queue=send_queue,
                    topic=topic,
                )
            await asyncio.sleep(1)
            continue

        message = await send_queue.get()
        await producer.send(topic, value=message)
        send_queue.task_done()

        messages_sent += 1

        if messages_sent >= 100:
            start_time, messages_sent = log_speed(
                counter=messages_sent,
                start_time=start_time,
                _queue=send_queue,
                topic=topic,
            )
    logger.info("shutdown")
