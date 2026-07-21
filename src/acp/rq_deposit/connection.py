from functools import lru_cache
import os

from redis import Redis
from rq import Queue


REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://redis:6379/0",
)

ACP_DEPOSIT_QUEUE = os.getenv(
    "ACP_DEPOSIT_QUEUE",
    "acp-deposit",
)


@lru_cache
def get_redis_connection() -> Redis:
    return Redis.from_url(
        REDIS_URL,
        decode_responses=False,
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=30,
    )


def get_deposit_queue() -> Queue:
    return Queue(
        name=ACP_DEPOSIT_QUEUE,
        connection=get_redis_connection(),
        default_timeout=3600,
    )