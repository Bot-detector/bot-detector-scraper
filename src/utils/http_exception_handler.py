import traceback
from aiohttp.client_exceptions import (
    ServerTimeoutError,
    ServerDisconnectedError,
    ClientConnectorError,
    ContentTypeError,
    ClientOSError,
)
import asyncio
import logging
from modules.validation.player import PlayerDoesNotExistException


class InvalidResponse(Exception):
    pass


def http_exception_handler(func):
    async def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        try:
            result = await func(*args, **kwargs)
            return result
        except (
            ServerTimeoutError,
            ServerDisconnectedError,
            ClientConnectorError,
            ContentTypeError,
            ClientOSError,
        ) as e:
            logger.error(f"{e}")
            raise InvalidResponse(f"{e}")
        except PlayerDoesNotExistException:
            raise PlayerDoesNotExistException()
        except Exception as e:
            # get the stack trace as a string
            tb_str = traceback.format_exc()
            logger.error(f"{e}, \n{tb_str}")
            await asyncio.sleep(10)
        return None

    return wrapper
