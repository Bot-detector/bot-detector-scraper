from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


class kafka:
    def __init__(self, bootstrap_server: str, topic: str, group_id: str) -> None:
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id
        self.topic = topic
        self.consumer = AIOKafkaConsumer(
            topic, bootstrap_server=bootstrap_server, group_id=group_id
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)

    async def send_message(self, message):
        pass

    async def consume_message(self):
        pass
