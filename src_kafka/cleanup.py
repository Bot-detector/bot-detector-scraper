from kafka.admin import NewTopic, KafkaAdminClient


def main():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9094")
    topics = admin_client.list_topics()
    print(topics)
    if not topics == []:
        admin_client.delete_topics(topics)
    res = admin_client.create_topics(
        [
            NewTopic(
                name="player",
                num_partitions=12,
                replication_factor=1,
                topic_configs={"retention.ms": 86_400_000, "cleanup.policy": "delete"},
            ),
            NewTopic(
                name="scraper",
                num_partitions=2,
                replication_factor=1,
                topic_configs={"retention.ms": 86_400_000, "cleanup.policy": "delete"},
            ),
        ]
    )

    print(res)
    topics = admin_client.list_topics()
    print(topics)


if __name__ == "__main__":
    main()
