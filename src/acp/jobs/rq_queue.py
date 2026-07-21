"""
RQ queue management for ACP deposit jobs.
"""
import logging
import os
from redis import Redis
from rq import Queue

logger = logging.getLogger(__name__)

_redis_conn = None
_deposit_queue = None


def get_redis_connection() -> Redis:
    """Get or create a Redis connection."""
    global _redis_conn
    if _redis_conn is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _redis_conn = Redis.from_url(redis_url, decode_responses=True)
        logger.info(f"Connected to Redis at {redis_url}")
    return _redis_conn


def get_deposit_queue() -> Queue:
    """Get or create the deposit job queue."""
    global _deposit_queue
    if _deposit_queue is None:
        _deposit_queue = Queue("acp-deposit", connection=get_redis_connection())
        logger.info("Initialized ACP deposit queue")
    return _deposit_queue


def initialize_queues():
    """Initialize all queues (called at startup)."""
    get_redis_connection()
    get_deposit_queue()
    logger.info("All RQ queues initialized")


