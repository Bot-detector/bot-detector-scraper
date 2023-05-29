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
    POST_INTERVAL=round(int(os.getenv("QUERY_SIZE")) * 0.1)
        if int(os.getenv("QUERY_SIZE")) > 100
        else int(os.getenv("QUERY_SIZE")),
    TIMEOUT_SECONDS=10,
    SESSION_TIMEOUT=ClientTimeout(total=None, sock_connect=10, sock_read=10),
)

