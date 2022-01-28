import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

PROXY_DOWNLOAD_URL = os.getenv('PROXY_DOWNLOAD_URL')
ENDPOINT = os.getenv('endpoint')
QUERY_SIZE = os.getenv('QUERY_SIZE')
TOKEN = os.getenv('TOKEN')

# setup logging
file_handler = logging.FileHandler(filename="error.log", mode='a')
stream_handler = logging.StreamHandler(sys.stdout)
# # log formatting
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

handlers = [
    file_handler,
    stream_handler
]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)

logging.getLogger('urllib3').setLevel(logging.INFO)