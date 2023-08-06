import os
import dotenv
from aiohttp import ClientTimeout
from config.app_config import AppConfig
from config import logging

dotenv.load_dotenv()

app_config = AppConfig(
    PROXY_DOWNLOAD_URL=os.getenv("PROXY_DOWNLOAD_URL"),
    ENDPOINT=os.getenv("ENDPOINT"),
    QUERY_SIZE=int(os.getenv("QUERY_SIZE")),
    TOKEN=os.getenv("TOKEN"),
    MAX_BYTES=int(os.getenv("MAX_BYTES", 1_000_000)),
    POST_INTERVAL=os.getenv("POST_INTERVAL", 60),
    TIMEOUT_SECONDS=10,
    SESSION_TIMEOUT=ClientTimeout(total=10),
    KAFKA_HOST=os.getenv("KAFKA_HOST")
)

