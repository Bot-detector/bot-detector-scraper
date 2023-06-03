from aiokafka import AIOKafkaProducer
import json

class KafkaProducer:
    def __init__(self, topic: str, bootstrap_servers:str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def initialize(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )
        await self.producer.start()

    async def send_message(self, message):
        await self.producer.send(topic=self.topic, value=message)

    async def stop(self):
        await self.producer.stop()

