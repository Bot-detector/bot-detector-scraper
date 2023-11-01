import json
from AioKafkaEngine.AioKafkaEngine import AioKafkaEngine
import asyncio

async def main():
    engine = AioKafkaEngine(
        bootstrap_servers=["localhost:9094"], # port forward prd kubectl port-forward -n kafka svc/bd-prd-kafka-service 9094:9094
        topic="player"
    )
    await engine.start_consumer(group_id="test")
    await engine.consume_messages()
    data = []
    while len(data) < 1000:
        msg = await engine.receive_queue.get()
        print(msg)
        data.append(msg)
        engine.receive_queue.task_done()

    await engine.stop_consumer()

    await write_to_json(data)

async def write_to_json(data):
    with open('./kafka_data.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)

if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
        loop.run_until_complete(main())
    except RuntimeError:
        asyncio.run(main())
