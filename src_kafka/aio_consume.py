from aiokafka import AIOKafkaConsumer
import asyncio
import json
import time

async def consume(consumer:AIOKafkaConsumer):
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.value)
            await consumer.commit()
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

async def do_something(batch:list):
    print(len(batch), batch[0])
    return []

async def consume_v2(consumer:AIOKafkaConsumer):
    await consumer.start()
    batch_size = 250
    try:
        batch = []
        start_time = time.time()
        async for msg in consumer:
            player = msg.value.decode()
            player = json.loads(player)

            if (len(batch) > batch_size) or (start_time + 60 < time.time() and batch):
                batch = await do_something(batch)
                start_time = time.time()

            batch.append(player)
    except Exception:
        await consumer.stop()
    finally:
        print("stooop")
        await consumer.stop()

async def consume_v3(consumer: AIOKafkaConsumer):
    # Get cluster layout and join group `my-group`
    await consumer.start()
    batch = []
    try:
        while True:
            msgs = await consumer.getmany(max_records=100)
            for tp, messages in msgs.items():
                for message in messages:
                    print("Consumed:", message.value.decode())

            # Commit the consumed messages
            await consumer.commit()
            break

    finally:
        # Stop the consumer and leave the consumer group
        await consumer.stop()

async def main():
    consumer = AIOKafkaConsumer(
        bootstrap_servers='localhost:9094',
        group_id="my-group",
        auto_offset_reset='earliest',
    )
    # Subscribe to the topic
    consumer.subscribe(['player']) 

    # print("v1")
    # await consume(consumer)
    # print('v2')

    # await consume_v2()
    
    print('v3')
    await consume_v3(consumer)


asyncio.run(main())