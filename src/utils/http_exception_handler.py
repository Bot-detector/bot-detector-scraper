
import traceback
from aiohttp.client_exceptions import (
    ServerTimeoutError,
    ServerDisconnectedError,
    ClientConnectorError,
    ContentTypeError,
    ClientOSError,
    ClientHttpProxyError,
)
import asyncio
import logging

def http_exception_handler(func):
    async def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__) 
        try:
            result = await func(*args, **kwargs)
            return result
        except ClientHttpProxyError:
            return "ClientHttpProxyError"
        except (
            ServerTimeoutError,
            ServerDisconnectedError,
            ClientConnectorError,
            ContentTypeError,
            ClientOSError,
        ) as e:
            logger.error(f"{e}")
        except Exception as e:
            # get the stack trace as a string
            tb_str = traceback.format_exc()  
            logger.error(f"{e}, \n{tb_str}")
            await asyncio.sleep(10)
        return None
    return wrapper