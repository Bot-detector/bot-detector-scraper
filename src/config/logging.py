import logging
import sys

# setup logging
stream_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler(filename="error.log")

# log formatting
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s"
)

stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

handlers = [
    stream_handler,
    # file_handler # this is good for debugging
]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)

logging.getLogger("urllib3").setLevel(logging.INFO)
# logging.getLogger("modules.scraper").setLevel(logging.WARNING)
logging.getLogger("aiokafka").setLevel(logging.WARNING)