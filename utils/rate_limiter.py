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

    def _acquire_token_redis_lua(self, key: str) -> bool:
        """
        Atomic leaky bucket token acquisition via Redis Lua script.
        """
        bucket_key = f"rate_limit:{key}"
        timestamp_key = f"rate_limit:{key}:timestamp"
        try:
            result = self.redis_client.evalsha(
                self._lua_sha,
                2,
                bucket_key,
                timestamp_key,
                self.window_size_seconds,
                self.effective_rate,
            )
            return bool(result)
        except Exception as e:
            logger.warning(f"Lua script evaluation failed: {e}")
            return self._acquire_token_redis(key)

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


def extract_endpoint_key(url: str) -> str:
    """
    Extract consistent endpoint key from Close API URL for rate limiting.

    Converts Close API URLs into normalized endpoint keys by extracting the
    root resource path. All operations on the same resource type share the
    same rate limit bucket.

    Examples:
        https://api.close.com/api/v1/lead/lead_123/ -> /api/v1/lead/
        https://api.close.com/api/v1/lead/lead_456/activity/ -> /api/v1/lead/
        https://api.close.com/api/v1/data/search/ -> /api/v1/data/search/

    Args:
        url: Full Close API URL

    Returns:
        str: Normalized endpoint key (e.g., "/api/v1/lead/")

    Raises:
        ValueError: If URL is invalid or not a Close API URL
    """
    # Input validation
    if url is None:
        raise ValueError("Invalid URL: URL cannot be None")

    if not isinstance(url, str):
        raise ValueError("URL must be a string")

    url = url.strip()
    if not url:
        raise ValueError("Invalid URL: URL cannot be empty")

    # Parse URL
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL format: {str(e)}")

    # Validate scheme
    if parsed.scheme not in ["http", "https"]:
        raise ValueError("Invalid URL format: URL must use http or https")

    # Validate domain (case-insensitive)
    if parsed.netloc.lower() != "api.close.com":
        raise ValueError("Not a Close API URL: URL must be for api.close.com")

    # Extract and validate path
    path = parsed.path
    if not path or path == "/":
        raise ValueError("Not a Close API endpoint: Missing API path")

    # Ensure path starts with /api/ (case-insensitive)
    if not path.lower().startswith("/api/"):
        raise ValueError("Not a Close API endpoint: Path must start with /api/")

    # Split path into segments
    path_segments = [seg for seg in path.split("/") if seg]  # Remove empty segments

    # Validate minimum path structure: ['api', 'v1', 'resource']
    if len(path_segments) < 3:
        raise ValueError("Not a Close API endpoint: Invalid path structure")

    # Validate API version (case-insensitive)
    if path_segments[0].lower() != "api":
        raise ValueError("Not a Close API endpoint: Path must start with /api/")

    if path_segments[1].lower() != "v1":
        raise ValueError("Unsupported API version: Only v1 is supported")

    # Extract root resource (3rd segment) - preserve original case
    root_resource = path_segments[2]

    # Build normalized endpoint key - preserve original case from path
    # For resource endpoints (lead, task, contact, activity), use root
    # For static endpoints (data/search, me, status), preserve full path

    # Check if this is a resource endpoint (has potential ID in 4th segment)
    if len(path_segments) >= 4:
        # Check if 4th segment looks like a resource ID
        potential_id = path_segments[3]

        # Close resource IDs typically follow patterns like: lead_123, task_456, cont_789, acti_123
        resource_id_patterns = ["lead_", "task_", "cont_", "acti_", "user_", "org_"]

        # If 4th segment starts with known resource ID pattern, this is a resource endpoint
        if any(potential_id.startswith(pattern) for pattern in resource_id_patterns):
            # Return root resource endpoint - preserve original case
            return f"/{path_segments[0]}/{path_segments[1]}/{root_resource}/"

    # For static endpoints or unrecognized patterns, preserve the full path structure
    # but normalize to ensure trailing slash
    if root_resource.lower() in ["data"]:
        # Special handling for data endpoints like /api/v1/data/search/
        if len(path_segments) >= 4:
            return f"/{path_segments[0]}/{path_segments[1]}/{root_resource}/{path_segments[3]}/"
        else:
            return f"/{path_segments[0]}/{path_segments[1]}/{root_resource}/"
    else:
        # For other static endpoints (me, status, etc.) - preserve original case
        return f"/{path_segments[0]}/{path_segments[1]}/{root_resource}/"


def parse_close_ratelimit_header(header_value: Optional[str]) -> dict:
    """
    Parse Close's ratelimit header format.

    Args:
        header_value: The ratelimit header value from Close API response
                     Format: "limit=160; remaining=159; reset=8"

    Returns:
        dict: Parsed rate limit information with keys: limit, remaining, reset

    Raises:
        ValueError: If header format is invalid or missing required fields
    """
    if not header_value:
        raise ValueError("Invalid ratelimit header format: header is None or empty")

    if not isinstance(header_value, str):
        raise ValueError("Invalid ratelimit header format: header must be a string")

    header_value = header_value.strip()
    if not header_value:
        raise ValueError("Invalid ratelimit header format: header is empty")

    # Parse the header format: "limit=160; remaining=159; reset=8"
    # Split by semicolon and parse each key=value pair
    parsed_data = {}
    required_fields = ["limit", "remaining", "reset"]
    valid_pairs_found = False

    try:
        parts = header_value.split(";")
        for part in parts:
            part = part.strip()
            if "=" not in part:
                continue

            key, value = part.split("=", 1)
            key = key.strip().lower()
            value = value.strip()

            if not value:
                raise ValueError(
                    f"Invalid ratelimit header format: empty value for {key}"
                )

            valid_pairs_found = True

            # Only process required fields, ignore additional fields with non-numeric values
            if key in required_fields:
                # Convert to integer (handle float values by converting to int)
                try:
                    parsed_data[key] = int(float(value))
                except (ValueError, TypeError):
                    raise ValueError(
                        f"Invalid ratelimit header format: non-numeric value '{value}' for {key}"
                    )
            else:
                # For additional fields, try to parse as numeric but ignore if not
                try:
                    parsed_data[key] = int(float(value))
                except (ValueError, TypeError):
                    # Ignore non-numeric additional fields
                    pass

        # If no valid key=value pairs were found, it's an invalid format
        if not valid_pairs_found:
            raise ValueError(
                "Invalid ratelimit header format: no valid key=value pairs found"
            )

    except Exception as e:
        if isinstance(e, ValueError) and "Invalid ratelimit header format" in str(e):
            raise
        raise ValueError(f"Invalid ratelimit header format: {str(e)}")

    # Check for required fields
    missing_fields = [field for field in required_fields if field not in parsed_data]

    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

    # Return only the required fields (ignore any additional fields)
    return {
        "limit": parsed_data["limit"],
        "remaining": parsed_data["remaining"],
        "reset": parsed_data["reset"],
    }


class CloseRateLimiter(RedisRateLimiter):
    """
    Dynamic rate limiter for Close.com API with endpoint-specific rate limiting.

    Extends RedisRateLimiter to provide:
    - Endpoint-specific rate limiting (different limits for different endpoints)
    - Dynamic limit discovery from Close API response headers
    - Conservative defaults for unknown endpoints
    - Safety factor application to discovered limits
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        conservative_default_rps: float = 1.0,
        safety_factor: float = 0.8,
        cache_expiration_seconds: int = 3600,  # 1 hour cache for discovered limits
        **kwargs,
    ):
        """
        Initialize Close.com dynamic rate limiter.

        Args:
            redis_client: Redis client instance
            conservative_default_rps: Default rate for unknown endpoints (req/sec)
            safety_factor: Safety margin for discovered limits (0.8 = 80% of API limit)
            cache_expiration_seconds: How long to cache discovered limits
            **kwargs: Additional arguments passed to RedisRateLimiter
        """
        # Set instance attributes first
        self.conservative_default_rps = conservative_default_rps
        self.cache_expiration_seconds = cache_expiration_seconds

        # Initialize parent with conservative default
        super().__init__(
            redis_client=redis_client,
            requests_per_second=conservative_default_rps,
            safety_factor=safety_factor,
            **kwargs,
        )

        logger.info(
            f"CloseRateLimiter initialized: conservative_default={conservative_default_rps} req/s, safety_factor={safety_factor}"
        )

    def acquire_token_for_endpoint(self, endpoint_url: str) -> bool:
        """
        Acquire a rate limit token for a specific Close API endpoint.

        Args:
            endpoint_url: Full Close API URL (e.g., "https://api.close.com/api/v1/lead/lead_123/")

        Returns:
            bool: True if token acquired (request allowed), False if rate limited
        """
        try:
            # Extract consistent endpoint key from URL
            endpoint_key = extract_endpoint_key(endpoint_url)

            # Check if we have cached limits for this endpoint
            cached_limits = self._get_cached_limits(endpoint_key)

            if cached_limits:
                # Use discovered limits with safety factor
                effective_rate = (
                    cached_limits["limit"] * self.safety_factor / 60.0
                )  # Convert to req/sec

                # Create temporary rate limiter with discovered limits
                temp_limiter = RedisRateLimiter(
                    redis_client=self.redis_client,
                    requests_per_second=effective_rate,
                    safety_factor=1.0,  # Already applied above
                    fallback_on_redis_error=self.fallback_on_redis_error,
                )

                # Use endpoint-specific bucket key
                bucket_key = f"close_endpoint:{endpoint_key}"
                return temp_limiter.acquire_token(bucket_key)
            else:
                # Use conservative default for unknown endpoints
                bucket_key = f"close_endpoint:{endpoint_key}"
                return self.acquire_token(bucket_key)

        except Exception as e:
            logger.error(f"Error in acquire_token_for_endpoint: {e}")
            # Fallback to conservative default
            return self.acquire_token(f"close_fallback:{endpoint_url}")

    def update_from_response_headers(self, endpoint_url: str, response) -> None:
        """
        Update rate limits based on Close API response headers.

        Args:
            endpoint_url: Full Close API URL
            response: HTTP response object with headers
        """
        try:
            # Check if response has rate limit headers
            if not hasattr(response, "headers") or not response.headers:
                return

            ratelimit_header = response.headers.get("ratelimit")
            if not ratelimit_header:
                return

            # Parse the rate limit header
            try:
                parsed_limits = parse_close_ratelimit_header(ratelimit_header)

                # Extract endpoint key
                endpoint_key = extract_endpoint_key(endpoint_url)

                # Cache the discovered limits
                self._cache_limits(endpoint_key, parsed_limits)

                logger.info(f"Updated rate limits for {endpoint_key}: {parsed_limits}")

            except ValueError as e:
                logger.warning(
                    f"Failed to parse rate limit header '{ratelimit_header}': {e}"
                )

        except Exception as e:
            logger.error(f"Error updating limits from response headers: {e}")

    def get_endpoint_limits(self, endpoint_key: str) -> dict:
        """
        Get cached rate limits for a specific endpoint.

        Args:
            endpoint_key: Normalized endpoint key (e.g., "/api/v1/lead/")

        Returns:
            dict: Cached limits or empty dict if not found
        """
        try:
            return self._get_cached_limits(endpoint_key) or {}
        except Exception as e:
            logger.error(f"Error getting endpoint limits: {e}")
            return {}

    def _extract_endpoint_key(self, endpoint_url: str) -> str:
        """
        Extract endpoint key from URL (wrapper for extract_endpoint_key function).

        Args:
            endpoint_url: Full Close API URL

        Returns:
            str: Normalized endpoint key
        """
        return extract_endpoint_key(endpoint_url)

    def _get_cached_limits(self, endpoint_key: str) -> Optional[dict]:
        """
        Retrieve cached rate limits for an endpoint from Redis.

        Args:
            endpoint_key: Normalized endpoint key

        Returns:
            dict: Cached limits or None if not found
        """
        try:
            if not self.redis_client:
                return None

            cache_key = f"close_rate_limit:limits:{endpoint_key}"
            cached_data = self.redis_client.get(cache_key)

            if cached_data:
                import json

                return json.loads(cached_data.decode("utf-8"))

        except Exception as e:
            logger.warning(f"Error retrieving cached limits for {endpoint_key}: {e}")

        return None

    def _cache_limits(self, endpoint_key: str, limits: dict) -> None:
        """
        Cache discovered rate limits for an endpoint in Redis.

        Args:
            endpoint_key: Normalized endpoint key
            limits: Parsed rate limit data
        """
        try:
            if not self.redis_client:
                return

            cache_key = f"close_rate_limit:limits:{endpoint_key}"

            import json

            cached_data = json.dumps(limits)

            # Cache with expiration
            self.redis_client.setex(
                cache_key, self.cache_expiration_seconds, cached_data
            )

            logger.debug(f"Cached limits for {endpoint_key}: {limits}")

        except Exception as e:
            logger.error(f"Error caching limits for {endpoint_key}: {e}")

    def __str__(self):
        """String representation of the Close rate limiter."""
        return (
            f"CloseRateLimiter("
            f"conservative_default={self.conservative_default_rps}/s, "
            f"safety_factor={self.safety_factor}, "
            f"cache_expiration={self.cache_expiration_seconds}s, "
            f"redis={'connected' if self.redis_client else 'fallback'})"
        )
