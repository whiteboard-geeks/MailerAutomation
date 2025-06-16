"""
Integration tests for Redis rate limiter configuration and fallback features.
"""

import os
import time
import redis
import pytest
from datetime import datetime

from utils.rate_limiter import RedisRateLimiter, APIRateConfig


class TestRateLimiterConfiguration:
    """Test rate limiter configuration and fallback features."""

    def setup_method(self):
        """Setup before each test."""
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        self.test_keys = []

    def teardown_method(self):
        """Cleanup after each test."""
        # Clean up test keys from Redis if available
        try:
            redis_client = redis.from_url(self.redis_url)
            for key in self.test_keys:
                try:
                    redis_client.delete(key)
                except Exception:
                    pass
        except Exception:
            pass

    def test_instantly_api_configuration(self):
        """Test Instantly API configuration preset."""
        print("\n=== TESTING INSTANTLY API CONFIGURATION ===")

        # Test Instantly configuration
        instantly_config = APIRateConfig.instantly()

        assert instantly_config.name == "instantly"
        assert instantly_config.requests_per_minute == 600
        assert instantly_config.requests_per_second == 10.0
        assert instantly_config.recommended_safety_factor == 0.8
        assert "600 requests/minute" in instantly_config.description

        # Create rate limiter with Instantly config
        rate_limiter = RedisRateLimiter(
            api_config=instantly_config,
            redis_url=self.redis_url,
            fallback_on_redis_error=True,
        )

        assert rate_limiter.api_rate_limit == 10.0
        assert rate_limiter.safety_factor == 0.8
        assert rate_limiter.effective_rate == 8.0  # 10.0 * 0.8
        assert rate_limiter.api_config.name == "instantly"

        print(f"✅ Instantly config test passed: {rate_limiter}")

    def test_custom_api_configuration(self):
        """Test custom API configuration."""
        print("\n=== TESTING CUSTOM API CONFIGURATION ===")

        # Test custom configuration
        custom_config = APIRateConfig.custom(300, safety_factor=0.7)

        assert custom_config.name == "custom"
        assert custom_config.requests_per_minute == 300
        assert custom_config.requests_per_second == 5.0
        assert custom_config.recommended_safety_factor == 0.7

        # Create rate limiter with custom config
        rate_limiter = RedisRateLimiter(
            api_config=custom_config,
            redis_url=self.redis_url,
            fallback_on_redis_error=True,
        )

        assert rate_limiter.api_rate_limit == 5.0
        assert rate_limiter.safety_factor == 0.7
        assert rate_limiter.effective_rate == 3.5  # 5.0 * 0.7

        print(f"✅ Custom config test passed: {rate_limiter}")

    def test_redis_fallback_behavior(self):
        """Test fallback behavior when Redis is unavailable."""
        print("\n=== TESTING REDIS FALLBACK BEHAVIOR ===")

        # Create rate limiter with invalid Redis URL to force fallback
        rate_limiter = RedisRateLimiter(
            requests_per_second=5.0,
            safety_factor=0.8,
            fallback_on_redis_error=True,
            redis_url="redis://invalid:6379",
            max_redis_retries=1,
            redis_retry_delay=0.01,
        )

        # Verify fallback mode is active
        assert rate_limiter.redis_client is None
        assert rate_limiter.fallback_on_redis_error is True

        # Test that fallback rate limiting works
        test_key = f"test_fallback:{datetime.now().isoformat()}"

        # Test rapid requests - should be rate limited
        allowed_count = 0
        start_time = time.time()

        for i in range(8):
            if rate_limiter.acquire_token(test_key):
                allowed_count += 1
            time.sleep(0.05)  # Rapid requests

        elapsed = time.time() - start_time

        # Should allow some requests but not all due to rate limiting
        assert (
            allowed_count < 8
        ), f"Fallback should rate limit, but allowed {allowed_count}/8 requests"
        assert (
            allowed_count > 0
        ), f"Fallback should allow some requests, but allowed {allowed_count}/8"

        print(
            f"✅ Fallback test passed: {allowed_count}/8 requests allowed in {elapsed:.2f}s"
        )

    def test_redis_connection_retry_logic(self):
        """Test Redis connection retry logic during token acquisition."""
        print("\n=== TESTING REDIS RETRY LOGIC ===")

        # Create a rate limiter with a working Redis client first
        try:
            redis_client = redis.from_url(self.redis_url)
            redis_client.ping()
        except Exception:
            pytest.skip("Redis not available for retry logic testing")

        # Create rate limiter with working Redis connection
        rate_limiter = RedisRateLimiter(
            requests_per_second=5.0,
            safety_factor=0.8,
            fallback_on_redis_error=True,
            redis_client=redis_client,
            max_redis_retries=3,
            redis_retry_delay=0.01,
        )

        # Verify Redis is connected initially
        assert rate_limiter.redis_client is not None

        # Now break the Redis connection to test retry logic
        # Close the existing connection
        rate_limiter.redis_client.connection_pool.disconnect()

        # Replace with invalid connection to trigger retries during acquire_token
        invalid_client = redis.Redis(
            host="invalid.invalid", port=6379, db=0, socket_connect_timeout=0.1
        )
        rate_limiter.redis_client = invalid_client

        test_key = f"test_retry:{datetime.now().isoformat()}"

        start_time = time.time()
        result = rate_limiter.acquire_token(test_key)
        retry_time = time.time() - start_time

        # Should have tried retries and then fallen back
        assert (
            retry_time > 0.01
        ), f"Should have taken time for retries, took {retry_time:.3f}s"

        # Should still work in fallback mode
        assert isinstance(
            result, bool
        ), "Should return boolean result even after retry failures"

        print(
            f"✅ Retry logic test passed: took {retry_time:.3f}s with {rate_limiter.max_redis_retries} retries"
        )

    def test_close_crm_configuration(self):
        """Test Close CRM API configuration preset."""
        print("\n=== TESTING CLOSE CRM API CONFIGURATION ===")

        # Test Close CRM configuration
        close_config = APIRateConfig.close_crm()

        assert close_config.name == "close_crm"
        assert close_config.requests_per_minute == 300
        assert close_config.requests_per_second == 5.0
        assert close_config.recommended_safety_factor == 0.8

        # Create rate limiter with Close CRM config
        rate_limiter = RedisRateLimiter(
            api_config=close_config,
            redis_url=self.redis_url,
            fallback_on_redis_error=True,
        )

        assert rate_limiter.api_rate_limit == 5.0
        assert rate_limiter.safety_factor == 0.8
        assert rate_limiter.effective_rate == 4.0  # 5.0 * 0.8

        print(f"✅ Close CRM config test passed: {rate_limiter}")

    def test_configuration_validation(self):
        """Test configuration validation and error handling."""
        print("\n=== TESTING CONFIGURATION VALIDATION ===")

        # Test missing configuration should raise error
        with pytest.raises(
            ValueError,
            match="Either api_config or requests_per_second must be provided",
        ):
            RedisRateLimiter()

        # Test that providing both config and requests_per_second works (api_config takes precedence)
        instantly_config = APIRateConfig.instantly()
        rate_limiter = RedisRateLimiter(
            api_config=instantly_config,
            requests_per_second=999.0,  # Should be ignored
            redis_url=self.redis_url,
            fallback_on_redis_error=True,
        )

        # Should use api_config, not requests_per_second
        assert rate_limiter.api_rate_limit == 10.0  # From instantly config
        assert rate_limiter.api_config.name == "instantly"

        print("✅ Configuration validation test passed")

    def test_fallback_disabled_behavior(self):
        """Test behavior when fallback is disabled and Redis fails."""
        print("\n=== TESTING FALLBACK DISABLED BEHAVIOR ===")

        # Test with fallback disabled - should not create rate limiter if Redis fails
        with pytest.raises(Exception):  # Should raise connection error
            RedisRateLimiter(
                requests_per_second=5.0,
                fallback_on_redis_error=False,
                redis_url="redis://invalid:6379",
                max_redis_retries=1,
                redis_retry_delay=0.01,
            )

        print(
            "✅ Fallback disabled test passed: properly raised exception when Redis unavailable"
        )
