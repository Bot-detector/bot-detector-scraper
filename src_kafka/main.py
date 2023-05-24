from kafka import KafkaProducer, KafkaConsumer
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
    bootstrap_servers='localhost:9094',
    group_id='my_consumer_group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    max_poll_records=1,
)

def create_topic(client:KafkaAdminClient, topics:list):
    print(f"Creating: {topics}")
    for topic in topics:
        try:
            client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            print(f"Topic '{topic}' created successfully")
        except TopicAlreadyExistsError:
            continue

def send_messages(client:KafkaProducer, topic:str, values:list):
    print("sending messages")
    for value in values:
        if not isinstance(value, bytes):
            value = bytes(str(value), 'utf-8')
        client.send(topic=topic, value=value)
    client.flush()

def consume_messages(client:KafkaConsumer):
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
    consumer.subscribe("scraper")
    admin_client = KafkaAdminClient(**admin_config.dict())
    
    existing_topics = admin_client.list_topics()
    desired_topics = ["scraper"]
    missing_topics = [t for t in desired_topics if t not in existing_topics]
    print(f"{existing_topics=}, {desired_topics=}")
    if missing_topics:
        create_topic(admin_client, missing_topics)

    values = [
        {"key": "value"},
        {"value": "key"}
    ]
    send_messages(client=producer, topic="scraper", values=values)
    consume_messages(consumer)

if __name__ == "__main__":
    main()
