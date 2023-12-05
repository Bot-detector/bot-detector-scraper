import asyncio
import json
import logging
import time
from asyncio import Event, Queue

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.config import app_config

logger = logging.getLogger(__name__)


async def kafka_consumer(topic: str, group: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=[app_config.KAFKA_HOST],
        group_id=group,
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
    while not shutdown_event.is_set():
        batch = await consumer.getmany(timeout_ms=1000, max_records=batch_size)
        for tp, messages in batch.items():
            logger.info(f"Partition {tp}: {len(messages)} messages")
            await asyncio.gather(*[receive_queue.put(m.value) for m in messages])
            logger.info("done")
            await consumer.commit()

    logger.info("shutdown")


async def send_messages(
    topic: str,
    producer: AIOKafkaProducer,
    send_queue: Queue,
    shutdown_event: Event,
):
    start_time = time.time()
    messages_sent = 0

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
