# setup_kafka.py
import json
import os
import time
import zipfile

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def create_topics():
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
                num_partitions=4,
                replication_factor=1,
            ),
            NewTopic(
                name="scraper",
                num_partitions=4,
                replication_factor=1,
            ),
            NewTopic(
                name="scraper-runemetrics",
                num_partitions=4,
                replication_factor=1,
            ),
            NewTopic(
                name="reports",
                num_partitions=4,
                replication_factor=1,
            ),
        ]
    )

    print("created_topic", res)

    topics = admin_client.list_topics()
    print("all topics", topics)
    return


def get_data() -> list[dict]:
    zip_file_path = "kafka_data/kafka_data.zip"
    extracted_folder = "kafka_data"

    print("Extracting data from the zip archive...")
    # Extract the files from the zip archive
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extracted_folder)

    result = []
    for file_name in os.listdir(extracted_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(extracted_folder, file_name)
            print(f"Processing file: {file_path}")

            with open(file_path) as file:
                data = json.load(file)
                result.extend(data)
    print(f"Received {len(result)}")
    return result


def insert_data(data: list, topic: str):
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode(),
        acks="all",
        # retries=100,
        batch_size=1,
    )

    for i, record in enumerate(data):
        print("inserting", i, record)
        producer.send(topic, value=record)


def setup_kafka():
    create_topics()
    data = get_data()
    insert_data(data=data, topic="player")


if __name__ == "__main__":
    setup_kafka()
