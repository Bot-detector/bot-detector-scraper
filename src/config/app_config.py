
from pydantic import BaseModel, HttpUrl, BaseSettings
from typing import Any

class AppConfig(BaseSettings):
    PROXY_DOWNLOAD_URL: HttpUrl
    ENDPOINT: str
    QUERY_SIZE: int
    TOKEN: str
    MAX_BYTES: int

    POST_INTERVAL: int
    TIMEOUT_SECONDS: int
    SESSION_TIMEOUT: Any

    KAFKA_HOST: str
    
    class Config:
        env_prefix = ""