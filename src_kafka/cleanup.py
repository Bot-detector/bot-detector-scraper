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
                    "retention.ms": 3_600_000, # 1 hour
                    "segment.ms": 900_000, # 15 minutes
                    # "segment.bytes": 10_000_000, # 10 mb
                    "cleanup.policy": "delete" ,   
                },
            ),
            NewTopic(
                name="scraper",
                num_partitions=2,
                replication_factor=1,
                topic_configs={
                    "retention.ms": 3_600_000, # 1 hour
                    "segment.ms": 900_000, # 15 minutes
                    # "segment.bytes": 10_000_000, # 10 mb
                    "cleanup.policy": "delete",
                },
            ),
        ]
    )

    print(res)
    topics = admin_client.list_topics()
    print(topics)


if __name__ == "__main__":
    main()
