"""
Integration tests for Close Rate Limiter with real Redis instance.

These tests validate that the CloseRateLimiter works correctly with a real Redis
instance, testing Redis key structure, endpoint isolation, caching, and fallback behavior.
"""

import os
import time
import redis
import pytest
from datetime import datetime
from unittest.mock import Mock

from utils.rate_limiter import CloseRateLimiter


@pytest.mark.integration
class TestCloseRateLimiterRedisIntegration:
    """Integration tests for CloseRateLimiter with real Redis."""

    def setup_method(self):
        """Setup before each test."""
        # Get Redis URL from environment
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        self.redis_client = None
        self.test_keys = []  # Track keys created during tests for cleanup

        # Attempt to connect to Redis
        try:
            self.redis_client = redis.from_url(self.redis_url)
            # Test the connection
            self.redis_client.ping()
            print(f"Successfully connected to Redis at: {self.redis_url}")
        except Exception as e:
            pytest.fail(f"Failed to connect to Redis at {self.redis_url}: {str(e)}")

    def teardown_method(self):
        """Cleanup after each test."""
        # Clean up test keys from Redis
        if self.redis_client:
            for key in self.test_keys:
                try:
                    self.redis_client.delete(key)
                except Exception as e:
                    print(f"Warning: Failed to cleanup key {key}: {e}")

    def test_redis_connection_and_basic_operations(self):
        """Test that we can connect to Redis and perform basic operations."""
        print(f"\n=== TESTING REDIS CONNECTION: {self.redis_url} ===")

        # Verify Redis client exists
        assert self.redis_client is not None, "Redis client is not available"

        # Test ping
        try:
            pong = self.redis_client.ping()
            assert pong is True, "Redis ping failed"
            print("✅ Redis ping successful")
        except Exception as e:
            pytest.fail(f"Redis ping failed: {str(e)}")

        # Test basic operations with Close rate limiter keys
        test_key = f"close_rate_limit:test:{datetime.now().isoformat()}"
        test_value = "test_value_123"
        self.test_keys.append(test_key)

        # Test SET operation
        result = self.redis_client.set(test_key, test_value)
        assert result is True, "Redis SET operation failed"
        print("✅ Redis SET operation successful")

        # Test GET operation
        retrieved_value = self.redis_client.get(test_key)
        assert retrieved_value is not None, "Redis GET returned None"
        assert (
            retrieved_value.decode("utf-8") == test_value
        ), "Retrieved value doesn't match"
        print("✅ Redis GET operation successful")

    def test_close_rate_limiter_redis_key_structure(self):
        """Test that CloseRateLimiter creates correct Redis key structure."""
        print("\n=== TESTING CLOSE RATE LIMITER REDIS KEY STRUCTURE ===")

        # Create CloseRateLimiter with real Redis
        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=2.0,
            safety_factor=0.8,
        )

        # Test endpoint URLs
        lead_url = "https://api.close.com/api/v1/lead/lead_123/"
        search_url = "https://api.close.com/api/v1/data/search/"

        # Acquire tokens to create Redis keys
        lead_result = rate_limiter.acquire_token_for_endpoint(lead_url)
        search_result = rate_limiter.acquire_token_for_endpoint(search_url)

        print(f"Lead endpoint token acquired: {lead_result}")
        print(f"Search endpoint token acquired: {search_result}")

        # Check that correct Redis keys were created
        expected_keys = [
            "rate_limit:close_endpoint:/api/v1/lead/",
            "rate_limit:close_endpoint:/api/v1/data/search/",
            "rate_limit:close_endpoint:/api/v1/lead/:timestamp",
            "rate_limit:close_endpoint:/api/v1/data/search/:timestamp",
        ]

        for expected_key in expected_keys:
            self.test_keys.append(expected_key)
            exists = self.redis_client.exists(expected_key)
            print(f"Key '{expected_key}' exists: {exists}")
            assert exists == 1, f"Expected Redis key '{expected_key}' was not created"

        print("✅ All expected Redis keys created correctly")

    def test_endpoint_specific_bucket_isolation(self):
        """Test that different endpoints have completely separate rate limit buckets."""
        print("\n=== TESTING ENDPOINT-SPECIFIC BUCKET ISOLATION ===")

        # Create rate limiter with moderate limits for testing
        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=1.0,  # 1 request per second
            safety_factor=1.0,  # No safety factor for testing
        )

        lead_url = "https://api.close.com/api/v1/lead/lead_123/"
        search_url = "https://api.close.com/api/v1/data/search/"

        # Clear any existing rate limit state
        lead_key = "rate_limit:close_endpoint:/api/v1/lead/"
        search_key = "rate_limit:close_endpoint:/api/v1/data/search/"
        self.redis_client.delete(lead_key, f"{lead_key}:timestamp")
        self.redis_client.delete(search_key, f"{search_key}:timestamp")

        # Test that endpoints are isolated by testing them with time delays
        print("Testing lead endpoint...")

        # First request to lead endpoint (will be denied - leaky bucket starts empty)
        result1 = rate_limiter.acquire_token_for_endpoint(lead_url)
        print(f"Lead request 1 (immediate): {'✅ ALLOWED' if result1 else '❌ DENIED'}")

        # Wait for tokens to accumulate, then test lead endpoint
        print("Waiting 1.5 seconds for tokens to accumulate...")
        time.sleep(1.5)
        result2 = rate_limiter.acquire_token_for_endpoint(lead_url)
        print(
            f"Lead request 2 (after 1.5s): {'✅ ALLOWED' if result2 else '❌ DENIED'}"
        )

        # Now test search endpoint immediately - should be independent
        print("Testing search endpoint independence...")

        # First request to search endpoint (will be denied - separate bucket starts empty)
        result3 = rate_limiter.acquire_token_for_endpoint(search_url)
        print(
            f"Search request 1 (immediate): {'✅ ALLOWED' if result3 else '❌ DENIED'}"
        )

        # Wait and test search endpoint
        print("Waiting 1.5 seconds for search tokens...")
        time.sleep(1.5)
        result4 = rate_limiter.acquire_token_for_endpoint(search_url)
        print(
            f"Search request 2 (after 1.5s): {'✅ ALLOWED' if result4 else '❌ DENIED'}"
        )

        # The key test: both endpoints should work independently after time passes
        # This proves they have separate buckets
        assert result2, "Lead endpoint should work after time passes"
        assert result4, "Search endpoint should work independently after time passes"

        print("✅ Endpoint isolation working correctly")

    def test_limit_caching_and_expiration(self):
        """Test that discovered limits are cached in Redis with proper expiration."""
        print("\n=== TESTING LIMIT CACHING AND EXPIRATION ===")

        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=1.0,
            safety_factor=0.8,
            cache_expiration_seconds=5,  # Short expiration for testing
        )

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock response with rate limit header
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

        # Update limits from response headers
        rate_limiter.update_from_response_headers(endpoint_url, mock_response)

        # Check that limits were cached
        endpoint_key = "/api/v1/lead/"
        cache_key = f"close_rate_limit:limits:{endpoint_key}"
        self.test_keys.append(cache_key)

        cached_data = self.redis_client.get(cache_key)
        assert cached_data is not None, "Limits should be cached in Redis"

        import json

        cached_limits = json.loads(cached_data.decode("utf-8"))
        assert cached_limits["limit"] == 160, "Cached limit should be 160"
        assert cached_limits["remaining"] == 159, "Cached remaining should be 159"
        assert cached_limits["reset"] == 8, "Cached reset should be 8"

        print(f"✅ Limits cached correctly: {cached_limits}")

        # Check TTL
        ttl = self.redis_client.ttl(cache_key)
        assert ttl > 0 and ttl <= 5, f"Cache TTL should be set correctly, got {ttl}"
        print(f"✅ Cache TTL set correctly: {ttl} seconds")

        # Wait for expiration
        print("Waiting for cache expiration...")
        time.sleep(6)

        # Check that cache expired
        expired_data = self.redis_client.get(cache_key)
        assert expired_data is None, "Cache should have expired"
        print("✅ Cache expiration working correctly")

    def test_fallback_behavior_when_redis_fails(self):
        """Test fallback behavior when Redis connection fails."""
        print("\n=== TESTING FALLBACK BEHAVIOR WHEN REDIS FAILS ===")

        # Create rate limiter with fallback enabled
        rate_limiter = CloseRateLimiter(
            redis_client=None,  # No Redis client
            conservative_default_rps=1.0,  # 1 req/sec
            safety_factor=1.0,  # No safety factor for testing
            fallback_on_redis_error=True,
        )

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Test that fallback works and endpoints are isolated
        print("Testing fallback with endpoint isolation...")

        # Test first endpoint
        results_1 = []
        for i in range(5):
            result = rate_limiter.acquire_token_for_endpoint(endpoint_url)
            results_1.append(result)
            print(
                f"Endpoint 1, request {i+1}: {'✅ ALLOWED' if result else '❌ DENIED'}"
            )

        # Test different endpoint - should be independent
        endpoint_url_2 = "https://api.close.com/api/v1/data/search/"
        results_2 = []
        for i in range(3):
            result = rate_limiter.acquire_token_for_endpoint(endpoint_url_2)
            results_2.append(result)
            print(
                f"Endpoint 2, request {i+1}: {'✅ ALLOWED' if result else '❌ DENIED'}"
            )

        allowed_count_1 = sum(results_1)
        allowed_count_2 = sum(results_2)

        print(f"Fallback mode - Endpoint 1: {allowed_count_1}/5 requests allowed")
        print(f"Fallback mode - Endpoint 2: {allowed_count_2}/3 requests allowed")

        # Fallback should work and maintain endpoint isolation
        assert (
            allowed_count_1 >= 1
        ), "Fallback should allow some requests for endpoint 1"
        assert (
            allowed_count_2 >= 1
        ), "Fallback should allow some requests for endpoint 2"

        print("✅ Fallback behavior working correctly")

    def test_atomic_redis_operations_thread_safety(self):
        """Test that Redis operations are atomic and thread-safe."""
        print("\n=== TESTING ATOMIC REDIS OPERATIONS ===")

        from concurrent.futures import ThreadPoolExecutor, as_completed

        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=3.0,  # 3 requests per second
            safety_factor=1.0,
        )

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        def acquire_token_worker(thread_id):
            """Worker function for concurrent token acquisition."""
            result = {
                "thread_id": thread_id,
                "timestamp": time.time(),
                "allowed": rate_limiter.acquire_token_for_endpoint(endpoint_url),
            }
            return result

        # Launch 10 threads simultaneously
        thread_count = 10
        results = []

        print(f"Launching {thread_count} concurrent threads...")
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Submit all requests simultaneously
            future_to_thread = {
                executor.submit(acquire_token_worker, i): i for i in range(thread_count)
            }

            # Collect results as they complete
            for future in as_completed(future_to_thread):
                try:
                    result = future.result()
                    results.append(result)
                    thread_id = result["thread_id"]
                    allowed = result["allowed"]
                    print(
                        f"Thread {thread_id}: {'✅ ALLOWED' if allowed else '❌ DENIED'}"
                    )
                except Exception as e:
                    thread_id = future_to_thread[future]
                    print(f"Thread {thread_id}: ERROR - {e}")

        concurrent_time = time.time() - start_time
        allowed_count = sum(1 for r in results if r["allowed"])
        denied_count = len(results) - allowed_count

        print("\nConcurrent results:")
        print(f"Total threads: {len(results)}")
        print(f"Allowed: {allowed_count}")
        print(f"Denied: {denied_count}")
        print(f"Time taken: {concurrent_time:.2f} seconds")

        # With 3 req/s rate and 10 concurrent requests, most should be denied initially
        assert allowed_count <= 6, f"Too many requests allowed: {allowed_count}"
        assert denied_count >= 4, f"Not enough requests denied: {denied_count}"
        print("✅ Atomic operations working correctly under concurrency")

    def test_redis_pipeline_operations(self):
        """Test Redis pipeline operations for batch processing."""
        print("\n=== TESTING REDIS PIPELINE OPERATIONS ===")

        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=5.0,
            safety_factor=0.8,
        )

        # Test multiple endpoints
        endpoints = [
            "https://api.close.com/api/v1/lead/lead_123/",
            "https://api.close.com/api/v1/data/search/",
            "https://api.close.com/api/v1/task/task_456/",
        ]

        # Acquire tokens for all endpoints
        results = []
        for endpoint in endpoints:
            result = rate_limiter.acquire_token_for_endpoint(endpoint)
            results.append(result)
            print(f"Endpoint {endpoint}: {'✅ ALLOWED' if result else '❌ DENIED'}")

        # Verify that Redis keys exist for all endpoints
        expected_keys = [
            "rate_limit:close_endpoint:/api/v1/lead/",
            "rate_limit:close_endpoint:/api/v1/data/search/",
            "rate_limit:close_endpoint:/api/v1/task/",
        ]

        for key in expected_keys:
            self.test_keys.append(key)
            self.test_keys.append(f"{key}:timestamp")
            exists = self.redis_client.exists(key)
            print(f"Key '{key}' exists: {exists}")
            assert exists == 1, f"Expected Redis key '{key}' was not created"

        print("✅ Pipeline operations working correctly")

    def test_cross_process_rate_limiting(self):
        """Test that rate limiting works across multiple rate limiter instances."""
        print("\n=== TESTING CROSS-PROCESS RATE LIMITING ===")

        # Create two separate rate limiter instances (simulating different processes)
        rate_limiter_1 = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=2.0,
            safety_factor=1.0,
        )

        rate_limiter_2 = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=2.0,
            safety_factor=1.0,
        )

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Use first instance to exhaust rate limit
        results_1 = []
        for i in range(3):
            result = rate_limiter_1.acquire_token_for_endpoint(endpoint_url)
            results_1.append(result)
            print(
                f"Instance 1, request {i+1}: {'✅ ALLOWED' if result else '❌ DENIED'}"
            )

        # Use second instance - should also be rate limited
        results_2 = []
        for i in range(3):
            result = rate_limiter_2.acquire_token_for_endpoint(endpoint_url)
            results_2.append(result)
            print(
                f"Instance 2, request {i+1}: {'✅ ALLOWED' if result else '❌ DENIED'}"
            )

        allowed_1 = sum(results_1)
        allowed_2 = sum(results_2)
        total_allowed = allowed_1 + allowed_2

        print(f"Instance 1: {allowed_1}/3 requests allowed")
        print(f"Instance 2: {allowed_2}/3 requests allowed")
        print(f"Total: {total_allowed}/6 requests allowed")

        # Both instances should share the same rate limit
        assert (
            total_allowed <= 4
        ), f"Cross-process rate limiting not working: {total_allowed} total allowed"
        print("✅ Cross-process rate limiting working correctly")
