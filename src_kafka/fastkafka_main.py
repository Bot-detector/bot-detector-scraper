# fastkafka run --num-workers=1 --kafka-broker=localhost fastkafka_main:app
import logging
from contextlib import asynccontextmanager

from aiokafka.admin import AIOKafkaAdminClient
from fastkafka import FastKafka
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

url = "localhost"
port = 9094

kafka_brokers = {
    "localhost": {
        "url": url,
        "description": "local development kafka broker",
        "port": port,
    }
}


async def startup():
    logger.debug("startup")


async def shutdown():
    logger.debug("shucdtdown")


@asynccontextmanager
async def lifespan(app: FastKafka):
    await startup()
    yield
    await shutdown()


app = FastKafka(
    title="Demo Kafka app",
    kafka_brokers=kafka_brokers,
    lifespan=lifespan,
)


class HelloWorld(BaseModel):
    msg: str = Field(
        ...,
        example="Hello",
        description="Demo hello world message",
    )


@app.consumes(topic="scraper", auto_offset_reset="latest")
async def on_hello_world(msg: HelloWorld):
    logger.info(f"{msg} ")


@app.produces(topic="scraper")
async def to_hello_world(msg: str) -> HelloWorld:
    return HelloWorld(msg=msg)


# @app.run_in_background()
# async def hello_every_second():
#     while(True):
#         await to_hello_world(msg="Hello world!")
#         await asyncio.sleep(1)
