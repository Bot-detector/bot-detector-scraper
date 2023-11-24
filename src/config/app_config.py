from typing import Any

from pydantic import BaseModel, HttpUrl


class AppConfig(BaseModel):
    PROXY_DOWNLOAD_URL: HttpUrl
    PROXY_API_KEY: str
    ENDPOINT: str
    QUERY_SIZE: int
    TOKEN: str
    MAX_BYTES: int

    POST_INTERVAL: int
    TIMEOUT_SECONDS: int
    SESSION_TIMEOUT: int

    KAFKA_HOST: str

    class Config:
        env_prefix = ""
