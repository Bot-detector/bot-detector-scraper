from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from pydantic import BaseModel


class AdminConfig(BaseModel):
    bootstrap_servers: str


class ProducerConfig(BaseModel):
    bootstrap_servers: str


class ConsumerConfig(BaseModel):
    bootstrap_servers: str
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool


# internal: kafka:9092
# external: localhost:9094

# Define producer config
producer_config = ProducerConfig(bootstrap_servers="localhost:9094")
admin_config = AdminConfig(bootstrap_servers="localhost:9094")

# Define consumer config
consumer_config = ConsumerConfig(
    bootstrap_servers="localhost:9094",
    group_id="my_consumer_group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=1,
)


def calculate_offset_difference(consumer: KafkaConsumer, topic: str):
    partitions = consumer.assignment()
    partitions: set[TopicPartition]
    print(f"{partitions=}")
    return sum(
        [consumer.end_offsets([p])[p] - consumer.position(p) for p in partitions]
    )


def create_topic(client: KafkaAdminClient, topics: list):
    print(f"Creating: {topics}")
    for topic in topics:
        try:
            client.create_topics(
                [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
            )
            print(f"Topic '{topic}' created successfully")
        except TopicAlreadyExistsError:
            continue


def send_messages(client: KafkaProducer, topic: str, values: list):
    print("sending messages")
    for value in values:
        if not isinstance(value, bytes):
            value = bytes(str(value), "utf-8")
        client.send(topic=topic, value=value)
    client.flush()


def consume_messages(client: KafkaConsumer):
    try:
        print("Consuming messages...")
        for message in client:

            print(message)
            client.commit()

    except KeyboardInterrupt:
        # Gracefully close the consumer on keyboard interrupt
        client.close()
        print("Consumer stopped.")


def main():
    producer = KafkaProducer(**producer_config.dict())
    consumer = KafkaConsumer(**consumer_config.dict())
    admin_client = KafkaAdminClient(**admin_config.dict())

    # Subscribe to a topic
    topic = "scraper"
    consumer.subscribe(topic)
    # Poll a message to trigger assignment update
    consumer.poll(timeout_ms=100)

    existing_topics = admin_client.list_topics()
    desired_topics = ["scraper","player"]
    missing_topics = [t for t in desired_topics if t not in existing_topics]
    print(f"{existing_topics=}, {desired_topics=}")
    if missing_topics:
        create_topic(admin_client, missing_topics)

    values = [{"key": "value"}, {"value": "key"}]
    send_messages(client=producer, topic=topic, values=values)
    print(calculate_offset_difference(consumer, topic=topic))
    consume_messages(consumer)


if __name__ == "__main__":
    main()
