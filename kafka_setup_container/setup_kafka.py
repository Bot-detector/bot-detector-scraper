# setup_kafka.py
import json
from kafka.admin import NewTopic, KafkaAdminClient
from kafka import KafkaProducer
import os


def setup_kafka():
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create Kafka topics
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

    topics = admin_client.list_topics()
    print("existing topics", topics)

    if not topics == []:
        admin_client.delete_topics(topics)

    res = admin_client.create_topics(
        [
            NewTopic(
                name="player",
                num_partitions=3,
                replication_factor=1,
            ),
            NewTopic(
                name="scraper",
                num_partitions=4,
                replication_factor=1,
            ),
        ]
    )

    print("created_topic", res)
    topics = admin_client.list_topics()
    print("all topics", topics)

    # Populate Kafka topics with initial data from the JSON file
    with open("initial_data.json") as file:
        data = json.load(file)

    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode(),
    )
    for topic, records in data.items():
        for record in records:
            message = json.dumps(record).encode("utf-8")
            producer.send(topic, value=message)

    producer.close()


if __name__ == "__main__":
    setup_kafka()
