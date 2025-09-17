# --- Redis cache helpers ---
import json
import os
import structlog
from redis import Redis

# Configure logging using structlog
logger = structlog.get_logger("redis_utils")


def get_redis_client():
    redis_url = os.environ.get("REDISCLOUD_URL")
    return Redis.from_url(redis_url) if redis_url else None


def get_from_cache(key):
    client = get_redis_client()
    if client:
        cached = client.get(key)
        if cached:
            try:
                return json.loads(cached)
            except Exception as e:
                logger.warning(f"Failed to decode cache for {key}: {e}")
    return None


def set_to_cache(key, value, expiration_seconds=600):
    client = get_redis_client()
    if client:
        try:
            client.setex(key, expiration_seconds, json.dumps(value))
        except Exception as e:
            logger.warning(f"Failed to set cache for {key}: {e}")