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
from typing import Optional
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class APIRateConfig:
    """Configuration preset for different API rate limits."""

    name: str
    requests_per_minute: int
    requests_per_second: float
    recommended_safety_factor: float
    description: str

    @classmethod
    def instantly(cls) -> "APIRateConfig":
        """Instantly API rate limit configuration."""
        return cls(
            name="instantly",
            requests_per_minute=600,
            requests_per_second=10.0,
            recommended_safety_factor=0.8,  # 80% of limit for safety
            description="Instantly API: 600 requests/minute = 10 requests/second",
        )

    @classmethod
    def close_crm(cls) -> "APIRateConfig":
        """Close CRM API rate limit configuration."""
        return cls(
            name="close_crm",
            requests_per_minute=300,
            requests_per_second=5.0,
            recommended_safety_factor=0.8,
            description="Close CRM API: 300 requests/minute = 5 requests/second",
        )

    @classmethod
    def custom(
        cls, requests_per_minute: int, safety_factor: float = 0.8
    ) -> "APIRateConfig":
        """Custom API rate limit configuration."""
        requests_per_second = requests_per_minute / 60.0
        return cls(
            name="custom",
            requests_per_minute=requests_per_minute,
            requests_per_second=requests_per_second,
            recommended_safety_factor=safety_factor,
            description=f"Custom API: {requests_per_minute} requests/minute = {requests_per_second:.1f} requests/second",
        )


class RedisRateLimiter:
    """
    Pure leaky bucket rate limiter with safety factor and improved fallback handling.

    The leaky bucket accumulates tokens at effective_rate over time.
    No artificial burst_allowance cap - let the algorithm naturally enforce sustained rate.
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        requests_per_second: Optional[float] = None,
        safety_factor: float = 0.8,
        window_size_seconds: int = 60,
        fallback_on_redis_error: bool = True,
        api_config: Optional[APIRateConfig] = None,
        redis_url: Optional[str] = None,
        max_redis_retries: int = 3,
        redis_retry_delay: float = 0.1,
    ):
        """
        Initialize the pure leaky bucket rate limiter.

        Args:
            redis_client: Redis client instance (optional if redis_url provided)
            requests_per_second: Maximum API rate limit (optional if api_config provided)
            safety_factor: Safety margin (0.8 = 80% of API limit, default: 0.8)
            window_size_seconds: Time window for rate limiting (default: 60s)
            fallback_on_redis_error: Allow requests if Redis fails (default: True)
            api_config: Pre-configured API rate limits (e.g., APIRateConfig.instantly())
            redis_url: Redis connection URL (if redis_client not provided)
            max_redis_retries: Maximum retry attempts for Redis operations (default: 3)
            redis_retry_delay: Delay between Redis retry attempts in seconds (default: 0.1)
        """
        # Handle API configuration
        if api_config is not None:
            self.api_rate_limit = api_config.requests_per_second
            if safety_factor == 0.8:  # Use recommended safety factor if default
                self.safety_factor = api_config.recommended_safety_factor
            else:
                self.safety_factor = safety_factor
            self.api_config = api_config
        elif requests_per_second is not None:
            self.api_rate_limit = requests_per_second
            self.safety_factor = safety_factor
            self.api_config = APIRateConfig.custom(
                int(requests_per_second * 60), safety_factor
            )
        else:
            raise ValueError(
                "Either api_config or requests_per_second must be provided"
            )

        # Handle Redis connection
        if redis_client is not None:
            self.redis_client = redis_client
        elif redis_url is not None:
            try:
                self.redis_client = redis.from_url(redis_url)
                # Test connection
                self.redis_client.ping()
                logger.info(f"Successfully connected to Redis at: {redis_url}")
            except Exception as e:
                logger.warning(f"Failed to connect to Redis at {redis_url}: {e}")
                if not fallback_on_redis_error:
                    raise
                self.redis_client = None
        else:
            # Try default Redis connection
            try:
                self.redis_client = redis.Redis(host="localhost", port=6379, db=0)
                self.redis_client.ping()
                logger.info("Successfully connected to default Redis (localhost:6379)")
            except Exception as e:
                logger.warning(f"Failed to connect to default Redis: {e}")
                if not fallback_on_redis_error:
                    raise
                self.redis_client = None

        self.effective_rate = self.api_rate_limit * self.safety_factor
        self.window_size_seconds = window_size_seconds
        self.fallback_on_redis_error = fallback_on_redis_error
        self.max_redis_retries = max_redis_retries
        self.redis_retry_delay = redis_retry_delay

        # Calculate token replenishment rate using effective rate
        self.tokens_per_window = self.effective_rate * window_size_seconds
        self.token_replenish_interval = 1.0 / self.effective_rate  # seconds per token

        # Fallback rate limiter (in-memory) for when Redis is unavailable
        self._fallback_bucket = {"tokens": 0.0, "last_refill": time.time()}

        logger.info(f"Rate limiter initialized: {self}")

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
        if self.redis_client is None:
            logger.debug(
                f"Redis unavailable, using fallback rate limiter for key '{key}'"
            )
            return self._acquire_token_fallback(key)

        for attempt in range(self.max_redis_retries):
            try:
                return self._acquire_token_redis(key)
            except redis.ConnectionError as e:
                logger.warning(
                    f"Redis connection error (attempt {attempt + 1}/{self.max_redis_retries}): {e}"
                )
                if attempt < self.max_redis_retries - 1:
                    time.sleep(
                        self.redis_retry_delay * (2**attempt)
                    )  # Exponential backoff
                    continue
                else:
                    logger.error(
                        f"Redis connection failed after {self.max_redis_retries} attempts"
                    )
                    if self.fallback_on_redis_error:
                        logger.info(
                            f"Falling back to in-memory rate limiter for key '{key}'"
                        )
                        return self._acquire_token_fallback(key)
                    return False
            except redis.RedisError as e:
                logger.warning(
                    f"Redis error in rate limiter (attempt {attempt + 1}/{self.max_redis_retries}): {e}"
                )
                if attempt < self.max_redis_retries - 1:
                    time.sleep(self.redis_retry_delay)
                    continue
                else:
                    if self.fallback_on_redis_error:
                        logger.info(
                            f"Falling back to in-memory rate limiter for key '{key}'"
                        )
                        return self._acquire_token_fallback(key)
                    return False
            except Exception as e:
                logger.error(f"Unexpected error in rate limiter: {e}")
                if self.fallback_on_redis_error:
                    return self._acquire_token_fallback(key)
                return False

        # Should not reach here, but fallback just in case
        if self.fallback_on_redis_error:
            return self._acquire_token_fallback(key)
        return False

    def _acquire_token_fallback(self, key: str) -> bool:
        """
        Fallback in-memory rate limiter when Redis is unavailable.

        Note: This only works for single-process rate limiting and will not
        coordinate across multiple application instances.
        """
        current_time = time.time()

        # Calculate tokens to add based on elapsed time
        time_elapsed = current_time - self._fallback_bucket["last_refill"]
        tokens_to_add = time_elapsed * self.effective_rate

        # Add tokens (no artificial cap)
        new_token_count = self._fallback_bucket["tokens"] + tokens_to_add

        # Check if we can consume a token
        if new_token_count >= 1.0:
            # Consume one token
            self._fallback_bucket["tokens"] = new_token_count - 1.0
            self._fallback_bucket["last_refill"] = current_time

            logger.debug(
                f"Fallback token acquired for key '{key}': "
                f"tokens={self._fallback_bucket['tokens']:.2f}, "
                f"elapsed={time_elapsed:.2f}s"
            )
            return True
        else:
            # Update timestamp but keep token count
            self._fallback_bucket["tokens"] = new_token_count
            self._fallback_bucket["last_refill"] = current_time

            logger.debug(
                f"Fallback token denied for key '{key}': "
                f"tokens={new_token_count:.2f}, "
                f"need=1.0"
            )
            return False

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
        redis_status = "connected" if self.redis_client is not None else "fallback"
        return (
            f"RedisRateLimiter("
            f"config={self.api_config.name}, "
            f"api_limit={self.api_rate_limit}/s, "
            f"safety_factor={self.safety_factor}, "
            f"effective_rate={self.effective_rate}/s, "
            f"redis={redis_status}, "
            f"pure_leaky_bucket=True)"
        )
