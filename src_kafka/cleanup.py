from kafka.admin import NewTopic, KafkaAdminClient


def main():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9094")
    topics = admin_client.list_topics()
    print(topics)
    if topics == []:
        admin_client.create_topics([
            NewTopic(name="player", num_partitions=1, replication_factor=1),
            NewTopic(name="scraper", num_partitions=1, replication_factor=1),
        ])
    else:
        admin_client.delete_topics(topics)

        admin_client.create_topics([
            NewTopic(name="player", num_partitions=1, replication_factor=1),
            NewTopic(name="scraper", num_partitions=1, replication_factor=1),
        ])
    topics = admin_client.list_topics()
    print(topics)

if __name__ == "__main__":
    main()