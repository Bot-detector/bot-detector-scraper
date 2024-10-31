import os

from pydantic import BaseSettings


class AppConfig(BaseSettings):
    PROXY_API_KEY: str = os.getenv("PROXY_API_KEY")
    SESSION_TIMEOUT: int = os.getenv("SESSION_TIMEOUT", 60)
    KAFKA_HOST: str = os.getenv("KAFKA_HOST")

    class Config:
        env_prefix = ""


app_config = AppConfig()
