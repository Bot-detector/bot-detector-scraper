# cleanup.py
from kafka.admin import NewTopic, KafkaAdminClient


def main():
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9094")
    topics = admin_client.list_topics()
    print("existng topics", topics)

    if not topics == []:
        admin_client.delete_topics(topics)
        # admin_client.delete_topics(['scraper'])
        # admin_client.delete_topics(["player"])

    res = admin_client.create_topics(
        [
            NewTopic(
                name="player",
                num_partitions=25,
                replication_factor=1,
                topic_configs={
                    # "session.timeout.ms":60000
                    # "retention.ms": 3_600_000, # 1 hour
                    # "segment.ms": 900_000, # 15 minutes
                    # # "segment.bytes": 10_000_000, # 10 mb
                    # "cleanup.policy": "delete" ,
                },
            ),
            NewTopic(
                name="scraper",
                num_partitions=4,
                replication_factor=1,
                topic_configs={
                    # "session.timeout.ms": 60000
                    # "retention.ms": 3_600_000, # 1 hour
                    # "segment.ms": 900_000, # 15 minutes
                    # # "segment.bytes": 10_000_000, # 10 mb
                    # "cleanup.policy": "delete",
                },
            ),
        ]
    )

    print("created_topic", res)
    topics = admin_client.list_topics()
    print("all topics", topics)


if __name__ == "__main__":
    main()
