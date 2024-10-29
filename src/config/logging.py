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


# | Level                | Numeric Value | What it Means / When to Use It                                                                 |
# |----------------------|---------------|--------------------------------------------------------------------------------------------------|
# | `logging.NOTSET`     | 0             | When set on a logger, indicates that ancestor loggers are to be consulted to determine the effective level. If that still resolves to NOTSET, then all events are logged. When set on a handler, all events are handled. |
# | `logging.DEBUG`      | 10            | Detailed information, typically only of interest to a developer trying to diagnose a problem.  |
# | `logging.INFO`       | 20            | Confirmation that things are working as expected.                                               |
# | `logging.WARNING`    | 30            | An indication that something unexpected happened, or that a problem might occur in the near future (e.g., ‘disk space low’). The software is still working as expected. |
# | `logging.ERROR`      | 40            | Due to a more serious problem, the software has not been able to perform some function.         |
# | `logging.CRITICAL`   | 50            | A serious error, indicating that the program itself may be unable to continue running.          |

logging.basicConfig(level=logging.DEBUG, handlers=handlers)

logging.getLogger("urllib3").setLevel(logging.INFO)
# logging.getLogger("modules.scraper").setLevel(logging.WARNING)
logging.getLogger("modules.api").setLevel(logging.WARNING)
logging.getLogger("aiokafka").setLevel(logging.INFO)
logging.getLogger("aiokafka").setLevel(logging.INFO)
logging.getLogger("osrs.async_api.osrs.hiscores").setLevel(logging.INFO)
