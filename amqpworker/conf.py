import logging
from typing import List, Optional

from loguru import logger as _logger
from pydantic import BaseSettings

from amqpworker.options import DefaultValues

INFINITY = float("inf")


class Settings(BaseSettings):
    LOGLEVEL: str = "ERROR"

    AMQP_DEFAULT_VHOST: str = "/"
    AMQP_DEFAULT_PREFETCH_COUNT: int = 128
    AMQP_DEFAULT_HEARTBEAT: int = 60

    HTTP_HOST: str = "127.0.0.1"
    HTTP_PORT: int = 8080

    FLUSH_TIMEOUT: int = DefaultValues.BULK_FLUSH_INTERVAL

    # metrics
    METRICS_NAMESPACE: str = "amqpworker"
    METRICS_APPPREFIX: Optional[str]
    METRICS_ROUTE_PATH: str = "/metrics"
    METRICS_ROUTE_ENABLED: bool = True
    METRICS_DEFAULT_HISTOGRAM_BUCKETS_IN_MS: List[float] = [
        10,
        50,
        100,
        200,
        500,
        1000,
        5000,
        INFINITY,
    ]

    class Config:
        allow_mutation = False
        env_prefix = "AMQPWORKER_"


settings = Settings()

loglevel = getattr(logging, settings.LOGLEVEL, logging.INFO)
_logger.level( settings.LOGLEVEL)
logger = _logger
