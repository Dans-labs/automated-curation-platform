from functools import lru_cache
import logging
import os

from redis import Redis
from rq import Queue

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0",
)

ACP_DEPOSIT_QUEUE = os.getenv(
    "ACP_DEPOSIT_QUEUE",
    "acp-deposit",
)


@lru_cache
def get_redis_connection() -> Redis:
    logger.info("Creating ACP deposit Redis connection: %s", REDIS_URL)
    return Redis.from_url(
        REDIS_URL,
        decode_responses=False,
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=30,
    )


def get_deposit_queue() -> Queue:
    logger.info("Opening ACP deposit queue: %s", ACP_DEPOSIT_QUEUE)
    return Queue(
        name=ACP_DEPOSIT_QUEUE,
        connection=get_redis_connection(),
        default_timeout=3600,
    )