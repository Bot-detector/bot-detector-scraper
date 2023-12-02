import os

import dotenv
from pydantic import BaseSettings

from config import logging


class AppConfig(BaseSettings):
    PROXY_API_KEY: str
    SESSION_TIMEOUT: int = 60
    KAFKA_HOST: str = "kafka:9092"

    class Config:
        env_prefix = ""


app_config = AppConfig()
