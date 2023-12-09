# setup_kafka.py
import json
import os
import time
import zipfile

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic


def get_num_partitions(topic):
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=kafka_broker)

    # Get the metadata for the specified topic
    metadata = consumer.partitions_for_topic(topic)

    # Close the consumer
    consumer.close()

    # Return the number of partitions
    return len(metadata) if metadata is not None else 0


def validate_partition_size(topic):
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")
    # Get the number of partitions for the topic
    num_partitions = get_num_partitions(topic)

    if num_partitions == 0:
        print(f"Topic '{topic}' not found or has no partitions.")
        return

    # Create KafkaConsumer
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_broker, enable_auto_commit=False, group_id=None
    )

    # Initialize a dictionary to store the size of each partition
    partition_sizes = {partition: 0 for partition in range(num_partitions)}

    for partition in range(num_partitions):
        # Assign the consumer to a specific partition
        topic_partition = TopicPartition(topic, partition)
        consumer.assign([topic_partition])

        # Seek to the end of the partition to get the current offset
        consumer.seek_to_end(topic_partition)
        end_offset = consumer.position(topic_partition)

        # Store the size of the partition
        partition_sizes[partition] = end_offset

    total_size = 0
    # Print the sizes of each partition
    for partition, size in partition_sizes.items():
        total_size += size
        print(f"Partition {partition}: {size} messages")

    # Close the consumer
    consumer.close()
    return total_size


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
    total_size = validate_partition_size(topic="player")
    print(f"{len(data)=} and {total_size=} must be the same")
    assert len(data) == total_size, f"{len(data)=} and {total_size=} must be the same"


if __name__ == "__main__":
    setup_kafka()
