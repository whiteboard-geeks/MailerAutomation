"""
Pure leaky bucket rate limiter with safety factor - no artificial burst limits.

This module provides rate limiting functionality for APIs like Instantly
that have strict request rate limits (e.g., 600 requests/minute = 10 requests/second).

Pure Leaky Bucket Algorithm:
- Tokens accumulate in bucket at effective_rate (API limit * safety_factor)
- No artificial cap on bucket size - let the algorithm naturally enforce rate
- Over time, sustained rate will converge to effective_rate regardless of initial tokens
"""

import time
import redis
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)


class RedisRateLimiter:
    """
    Pure leaky bucket rate limiter with safety factor.

    The leaky bucket accumulates tokens at effective_rate over time.
    No artificial burst_allowance cap - let the algorithm naturally enforce sustained rate.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        requests_per_second: float,
        safety_factor: float = 0.5,
        window_size_seconds: int = 60,
        fallback_on_redis_error: bool = True,
    ):
        """
        Initialize the pure leaky bucket rate limiter.

        Args:
            redis_client: Redis client instance
            requests_per_second: Maximum API rate limit (e.g., 10 for Instantly)
            safety_factor: Safety margin (0.5 = 50% of API limit, default: 0.5)
            window_size_seconds: Time window for rate limiting (default: 60s)
            fallback_on_redis_error: Allow requests if Redis fails (default: True)
        """
        self.redis_client = redis_client
        self.api_rate_limit = requests_per_second
        self.safety_factor = safety_factor
        self.effective_rate = requests_per_second * safety_factor
        self.window_size_seconds = window_size_seconds
        self.fallback_on_redis_error = fallback_on_redis_error

        # Calculate token replenishment rate using effective rate
        self.tokens_per_window = self.effective_rate * window_size_seconds
        self.token_replenish_interval = 1.0 / self.effective_rate  # seconds per token

    def acquire_token(self, key: str) -> bool:
        """
        Attempt to acquire a token for the given key.

        Pure leaky bucket algorithm:
        1. Check current token count and last refill time
        2. Calculate tokens to add based on elapsed time and effective rate
        3. Add tokens (no artificial cap)
        4. If tokens >= 1, consume one and allow request
        5. If tokens < 1, deny request

        Args:
            key: Unique identifier for the rate limit bucket (e.g., "instantly_api")

        Returns:
            bool: True if token acquired (request allowed), False if denied
        """
        try:
            return self._acquire_token_redis(key)
        except redis.RedisError as e:
            logger.warning(f"Redis error in rate limiter: {e}")
            return self.fallback_on_redis_error
        except Exception as e:
            logger.error(f"Unexpected error in rate limiter: {e}")
            return self.fallback_on_redis_error

    def _acquire_token_redis(self, key: str) -> bool:
        """
        Core Redis-based token acquisition logic using pure leaky bucket.

        Uses Redis pipeline for atomic operations to ensure thread safety
        and consistency across distributed instances.
        """
        bucket_key = f"rate_limit:{key}"
        timestamp_key = f"rate_limit:{key}:timestamp"

        current_time = time.time()

        # Use Redis pipeline for atomic operations
        pipe = self.redis_client.pipeline()

        try:
            # Watch keys for atomic transaction
            pipe.watch(bucket_key, timestamp_key)

            # Get current state
            current_tokens = pipe.get(bucket_key)
            last_refill = pipe.get(timestamp_key)

            # Parse current state
            if current_tokens is None:
                # First request - start with 0 tokens (pure leaky bucket)
                current_tokens = 0.0
                last_refill = current_time
            else:
                current_tokens = float(current_tokens)
                if last_refill is None:
                    last_refill = current_time
                else:
                    last_refill = float(last_refill)

            # Calculate token replenishment using effective rate (with safety factor)
            time_elapsed = current_time - last_refill
            tokens_to_add = time_elapsed * self.effective_rate

            # New token count = current + replenished (no artificial cap)
            new_token_count = current_tokens + tokens_to_add

            # Check if we can consume a token
            if new_token_count >= 1.0:
                # Consume one token
                final_token_count = new_token_count - 1.0

                # Start transaction
                pipe.multi()

                # Update Redis state atomically
                pipe.setex(bucket_key, self.window_size_seconds, final_token_count)
                pipe.setex(timestamp_key, self.window_size_seconds, current_time)

                # Execute transaction
                pipe.execute()

                logger.debug(
                    f"Token acquired for key '{key}': "
                    f"tokens={final_token_count:.2f}, "
                    f"elapsed={time_elapsed:.2f}s, "
                    f"added={tokens_to_add:.2f}, "
                    f"effective_rate={self.effective_rate:.2f}"
                )
                return True
            else:
                # No tokens available - bucket is empty
                # Update timestamp but keep token count (may be negative)
                pipe.multi()
                pipe.setex(bucket_key, self.window_size_seconds, new_token_count)
                pipe.setex(timestamp_key, self.window_size_seconds, current_time)
                pipe.execute()

                logger.debug(
                    f"Token denied for key '{key}': "
                    f"tokens={new_token_count:.2f}, "
                    f"elapsed={time_elapsed:.2f}s, "
                    f"need=1.0, "
                    f"effective_rate={self.effective_rate:.2f}"
                )
                return False

        except redis.WatchError:
            # Another operation modified the keys during our transaction
            # This is expected in high-concurrency scenarios
            logger.debug(f"Redis watch error for key '{key}' - retrying")
            return False
        finally:
            pipe.reset()

    def get_bucket_status(self, key: str) -> dict:
        """
        Get current status of the rate limit bucket.

        Args:
            key: Rate limit bucket key

        Returns:
            dict: Bucket status including token count, last refill time, etc.
        """
        try:
            bucket_key = f"rate_limit:{key}"
            timestamp_key = f"rate_limit:{key}:timestamp"

            current_tokens = self.redis_client.get(bucket_key)
            last_refill = self.redis_client.get(timestamp_key)
            current_time = time.time()

            if current_tokens is None:
                current_tokens = 0.0
            else:
                current_tokens = float(current_tokens)

            if last_refill is None:
                last_refill = current_time
            else:
                last_refill = float(last_refill)

            time_elapsed = current_time - last_refill
            tokens_to_add = time_elapsed * self.effective_rate
            effective_tokens = current_tokens + tokens_to_add

            return {
                "key": key,
                "current_tokens": current_tokens,
                "effective_tokens": effective_tokens,
                "tokens_to_add": tokens_to_add,
                "time_elapsed": time_elapsed,
                "last_refill": last_refill,
                "current_time": current_time,
                "api_rate_limit": self.api_rate_limit,
                "safety_factor": self.safety_factor,
                "effective_rate": self.effective_rate,
                "window_size_seconds": self.window_size_seconds,
            }
        except Exception as e:
            logger.error(f"Error getting bucket status: {e}")
            return {"error": str(e)}

    def reset_bucket(self, key: str) -> bool:
        """
        Reset the rate limit bucket to 0 tokens.

        Args:
            key: Rate limit bucket key

        Returns:
            bool: True if reset successful, False otherwise
        """
        try:
            bucket_key = f"rate_limit:{key}"
            timestamp_key = f"rate_limit:{key}:timestamp"

            # Delete keys to reset bucket
            self.redis_client.delete(bucket_key, timestamp_key)

            logger.info(f"Rate limit bucket reset for key '{key}'")
            return True
        except Exception as e:
            logger.error(f"Error resetting bucket: {e}")
            return False

    def __str__(self):
        """String representation of the rate limiter configuration."""
        return (
            f"RedisRateLimiter("
            f"api_limit={self.api_rate_limit}/s, "
            f"safety_factor={self.safety_factor}, "
            f"effective_rate={self.effective_rate}/s, "
            f"pure_leaky_bucket=True)"
        )
