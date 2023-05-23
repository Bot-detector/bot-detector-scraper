from aiokafka import AIOKafkaProducer
import asyncio


async def send_one():
    # internal: kafka:9092
    # external: localhost:9094
    producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait("test", b"Super message")
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


asyncio.run(send_one())