import logging
import os
import sys

import dotenv
import aiohttp

dotenv.load_dotenv()

PROXY_DOWNLOAD_URL = os.getenv("PROXY_DOWNLOAD_URL")
ENDPOINT = os.getenv("endpoint")
QUERY_SIZE = int(os.getenv("QUERY_SIZE"))
TOKEN = os.getenv("TOKEN")

POST_INTERVAL = round(QUERY_SIZE * 0.5)
POST_INTERVAL = POST_INTERVAL if POST_INTERVAL > 100 else QUERY_SIZE

TIMEOUT_SECONDS = 10
SESSION_TIMEOUT = aiohttp.ClientTimeout(
    total=None, sock_connect=TIMEOUT_SECONDS, sock_read=TIMEOUT_SECONDS
)

# setup logging
file_handler = logging.FileHandler(filename="error.log", mode="a")
stream_handler = logging.StreamHandler(sys.stdout)

# log formatting
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

handlers = [file_handler, stream_handler]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)

logging.getLogger("urllib3").setLevel(logging.INFO)
