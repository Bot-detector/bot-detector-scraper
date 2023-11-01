import asyncio
import json

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel


class Player(BaseModel):
    id: int
    name: str
    created_at: str
    updated_at: str | None
    possible_ban: int
    confirmed_ban: int
    confirmed_player: int
    label_id: int
    label_jagex: int


def serializer(value):
    return json.dumps(value).encode()


async def send_one():
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9094",
        value_serializer=lambda x: json.dumps(x).encode(),
    )
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        player = Player(
            id=1,
            name="extreme4all",
            created_at="",
            updated_at="",
            possible_ban=0,
            confirmed_ban=0,
            confirmed_player=0,
            label_id=0,
            label_jagex=0,
        )
        await producer.send_and_wait("player", player.dict())
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


asyncio.run(send_one())
