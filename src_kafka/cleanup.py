from kafka.admin import NewTopic, KafkaAdminClient


def main():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9094")
    topics = admin_client.list_topics()
    print(topics)

    if not topics == []:
        admin_client.delete_topics(topics)
        # admin_client.delete_topics(['scraper'])
        # admin_client.delete_topics(["player"])
        
    res = admin_client.create_topics(
        [
            NewTopic(
                name="player",
                num_partitions=3,
                replication_factor=1,
                topic_configs={
                    # "session.timeout.ms":60000
                    "retention.bytes": 1_000_000_000, # 1GB
                    "segment.bytes": 100_000_000, # 100 mb
                    "retention.ms": 3_600_000 * 4, # 4 hours
                    "segment.ms": 3_600_000 , # 1 hour minutes
                    # "cleanup.policy": "delete" ,   
                },
            ),
            NewTopic(
                name="scraper",
                num_partitions=4,
                replication_factor=1,
                topic_configs={
                    # "session.timeout.ms": 60000
                    "retention.bytes": 1_000_000_000, # 1GB
                    "segment.bytes": 100_000_000, # 100 mb
                    "retention.ms": 3_600_000 * 4, # 4 hours
                    "segment.ms": 3_600_000 , # 1 hour minutes
                    # "cleanup.policy": "delete",
                },
            ),
        ]
    )

    print(res)
    topics = admin_client.list_topics()
    print(topics)


if __name__ == "__main__":
    main()
