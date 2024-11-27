import json
import logging


# Configure JSON logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "ts": self.formatTime(record, self.datefmt),
            "lvl": record.levelname,
            "name": record.name,
            # "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


# Set up the logger
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logging.basicConfig(level=logging.INFO, handlers=[handler])


# set imported loggers to warning
logging.getLogger("urllib3").setLevel(logging.INFO)
# logging.getLogger("modules.scraper").setLevel(logging.WARNING)
logging.getLogger("modules.api").setLevel(logging.WARNING)
logging.getLogger("aiokafka").setLevel(logging.INFO)
