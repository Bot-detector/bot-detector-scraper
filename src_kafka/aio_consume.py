from aiokafka import AIOKafkaConsumer
import asyncio
import json
import time


async def consume(consumer: AIOKafkaConsumer):
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


async def do_something(batch: list, batch_num):
    file_path = f"kafka_data/{batch_num}_player.json"
    print(file_path)
    with open(file_path, "w") as file:
        json.dump(batch, file)
    return []


async def consume_v2(consumer: AIOKafkaConsumer):
    await consumer.start()
    batch_size = 1000
    batch_num = 1
    try:
        batch = []
        async for msg in consumer:
            player = msg.value.decode()
            player = json.loads(player)
            if len(batch) % 100 == 0:
                print(len(batch))
            if len(batch) > batch_size:
                batch = await do_something(batch, batch_num)
                batch_num += 1
            batch.append(player)
    except Exception as e:
        print(e)
        await consumer.stop()
    finally:
        print("stooop")
        await consumer.stop()


async def consume_v3(consumer: AIOKafkaConsumer):
    # Get cluster layout and join group `my-group`
    await consumer.start()
    batch = []
    batch_num = 1
    try:
        while True:
            msgs = await consumer.getmany(max_records=100)
            for tp, messages in msgs.items():
                for message in messages:
                    batch.append(message.value.decode())
                    # Commit the consumed messages
                    print(len(batch))

                    if len(batch) > 1000:
                        file_path = f"kafka_data/{batch_num}_player.json"
                        print(file_path)
                        batch_num += 1
                        with open(file_path, "w") as file:
                            json.dump(batch, file)
                    await consumer.commit()

    finally:
        # Stop the consumer and leave the consumer group
        await consumer.stop()


async def main():
    consumer = AIOKafkaConsumer(
        bootstrap_servers="localhost:9094",
        group_id="my-group",
        auto_offset_reset="earliest",
    )
    # Subscribe to the topic
    consumer.subscribe(["player"])

    # print("v1")
    # await consume(consumer)

    print("v2")
    await consume_v2(consumer)

    # print('v3')
    # await consume_v3(consumer)


asyncio.run(main())
