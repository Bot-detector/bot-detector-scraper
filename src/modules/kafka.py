from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


class Kafka:
    def __init__(self, bootstrap_server: str, topic: str, group_id: str) -> None:
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id
        self.topic = topic
        self.consumer = AIOKafkaConsumer(
            topic, bootstrap_server=bootstrap_server, group_id=group_id
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)

    async def send_message(self, message):
        await self.producer.start()
        try:
            await self.producer.send_and_wait(topic=self.topic, value=message)
        finally:
            await self.producer.stop()

    async def consume_message(self):
        await self.consumer.start()
        message = None
        try:
            message = await self.consumer.getone()
        finally:
            await self.consumer.stop()
        return message