"""
Integration tests for Redis-based rate limiter functionality.
"""

import os
import time
import redis
import pytest
from datetime import datetime


class TestRedisRateLimiter:
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

    def test_redis_connection(self):
        """Test that we can connect to Redis."""
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

    def test_redis_basic_operations(self):
        """Test basic Redis operations: set, get, delete, expire."""
        print("\n=== TESTING REDIS BASIC OPERATIONS ===")

        # Test key/value for basic operations
        test_key = f"test_rate_limiter:{datetime.now().isoformat()}"
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

        # Test EXISTS operation
        exists = self.redis_client.exists(test_key)
        assert exists == 1, "Redis EXISTS returned incorrect value"
        print("✅ Redis EXISTS operation successful")

        # Test EXPIRE operation (set 2 second expiration)
        expire_result = self.redis_client.expire(test_key, 2)
        assert expire_result is True, "Redis EXPIRE operation failed"
        print("✅ Redis EXPIRE operation successful")

        # Test TTL operation
        ttl = self.redis_client.ttl(test_key)
        assert ttl > 0 and ttl <= 2, f"Redis TTL returned unexpected value: {ttl}"
        print(f"✅ Redis TTL operation successful: {ttl} seconds")

        # Wait for expiration and verify key is gone
        time.sleep(3)
        exists_after_expire = self.redis_client.exists(test_key)
        assert exists_after_expire == 0, "Key should have expired"
        print("✅ Redis key expiration verified")

        # Test DELETE operation with a new key
        delete_test_key = f"test_delete:{datetime.now().isoformat()}"
        self.test_keys.append(delete_test_key)

        self.redis_client.set(delete_test_key, "delete_me")
        delete_result = self.redis_client.delete(delete_test_key)
        assert delete_result == 1, "Redis DELETE operation failed"

        # Verify key is deleted
        exists_after_delete = self.redis_client.exists(delete_test_key)
        assert exists_after_delete == 0, "Key should have been deleted"
        print("✅ Redis DELETE operation successful")

    def test_redis_atomic_operations(self):
        """Test Redis atomic operations for rate limiting: INCR, SETEX."""
        print("\n=== TESTING REDIS ATOMIC OPERATIONS ===")

        # Test INCR operation (atomic increment)
        counter_key = f"test_counter:{datetime.now().isoformat()}"
        self.test_keys.append(counter_key)

        # Initial increment should create key with value 1
        count1 = self.redis_client.incr(counter_key)
        assert count1 == 1, f"First INCR should return 1, got {count1}"
        print("✅ Redis INCR operation (initial) successful")

        # Second increment should return 2
        count2 = self.redis_client.incr(counter_key)
        assert count2 == 2, f"Second INCR should return 2, got {count2}"
        print("✅ Redis INCR operation (increment) successful")

        # Test SETEX operation (set with expiration)
        setex_key = f"test_setex:{datetime.now().isoformat()}"
        self.test_keys.append(setex_key)

        setex_result = self.redis_client.setex(setex_key, 2, "setex_value")
        assert setex_result is True, "Redis SETEX operation failed"

        # Verify value was set
        setex_value = self.redis_client.get(setex_key)
        assert setex_value.decode("utf-8") == "setex_value", "SETEX value doesn't match"

        # Verify TTL was set
        setex_ttl = self.redis_client.ttl(setex_key)
        assert setex_ttl > 0 and setex_ttl <= 2, f"SETEX TTL incorrect: {setex_ttl}"
        print("✅ Redis SETEX operation successful")

    def test_redis_pipeline_operations(self):
        """Test Redis pipeline operations for batch processing."""
        print("\n=== TESTING REDIS PIPELINE OPERATIONS ===")

        # Create pipeline
        pipeline = self.redis_client.pipeline()

        # Test multiple operations in pipeline
        pipeline_keys = []
        for i in range(3):
            key = f"test_pipeline_{i}:{datetime.now().isoformat()}"
            pipeline_keys.append(key)
            self.test_keys.append(key)
            pipeline.set(key, f"value_{i}")

        # Execute pipeline
        results = pipeline.execute()
        assert (
            len(results) == 3
        ), f"Pipeline should return 3 results, got {len(results)}"
        assert all(
            result is True for result in results
        ), "All pipeline operations should succeed"
        print("✅ Redis pipeline SET operations successful")

        # Verify values were set
        for i, key in enumerate(pipeline_keys):
            value = self.redis_client.get(key)
            assert (
                value.decode("utf-8") == f"value_{i}"
            ), f"Pipeline value {i} doesn't match"
        print("✅ Redis pipeline values verified")

    def test_redis_error_handling(self):
        """Test Redis error handling scenarios."""
        print("\n=== TESTING REDIS ERROR HANDLING ===")

        # Test operation on non-existent key
        non_existent_key = f"non_existent:{datetime.now().isoformat()}"
        value = self.redis_client.get(non_existent_key)
        assert value is None, "GET on non-existent key should return None"
        print("✅ Redis GET on non-existent key handled correctly")

        # Test DELETE on non-existent key
        delete_result = self.redis_client.delete(non_existent_key)
        assert delete_result == 0, "DELETE on non-existent key should return 0"
        print("✅ Redis DELETE on non-existent key handled correctly")

        # Test TTL on non-existent key
        ttl_result = self.redis_client.ttl(non_existent_key)
        assert ttl_result == -2, "TTL on non-existent key should return -2"
        print("✅ Redis TTL on non-existent key handled correctly")

    def test_leaky_bucket_rate_limiting_algorithm(self):
        """Test leaky bucket rate limiting algorithm implementation."""
        print("\n=== TESTING LEAKY BUCKET RATE LIMITING ALGORITHM ===")

        # This test will initially FAIL since RedisRateLimiter class doesn't exist yet
        # Following the test-first approach as outlined in the plan

        try:
            from utils.rate_limiter import RedisRateLimiter
        except ImportError:
            pytest.fail(
                "RedisRateLimiter class not found. This test is expected to FAIL initially "
                "until Step 2.2 (Implement Redis Rate Limiter) is completed."
            )

        # Test configuration: Instantly API limits = 600 requests/minute = 10 requests/second
        # Using conservative rate: 5 requests/second for testing
        rate_limiter = RedisRateLimiter(
            redis_client=self.redis_client,
            requests_per_second=5,
            window_size_seconds=60,
            burst_allowance=10,
        )

        limiter_key = f"test_leaky_bucket:{datetime.now().isoformat()}"
        self.test_keys.append(limiter_key)

        print("Testing leaky bucket algorithm with 5 requests/second limit...")

        # Test 1: Should allow initial burst up to burst_allowance
        print("\n--- Test 1: Initial burst allowance ---")
        burst_results = []
        start_time = time.time()

        for i in range(8):  # Try 8 requests quickly (within burst_allowance of 10)
            allowed = rate_limiter.acquire_token(limiter_key)
            burst_results.append(allowed)
            print(f"Request {i+1}: {'✅ ALLOWED' if allowed else '❌ DENIED'}")

        burst_time = time.time() - start_time
        allowed_count = sum(burst_results)

        assert (
            allowed_count >= 5
        ), f"Should allow at least 5 requests in burst, got {allowed_count}"
        assert burst_time < 2, f"Burst should be fast, took {burst_time:.2f} seconds"
        print(
            f"✅ Burst test passed: {allowed_count}/8 requests allowed in {burst_time:.2f}s"
        )

        # Test 2: Should rate limit after burst is exhausted
        print("\n--- Test 2: Rate limiting after burst ---")

        # Try 5 more requests rapidly - should be rate limited
        rapid_results = []
        rapid_start = time.time()

        for i in range(5):
            allowed = rate_limiter.acquire_token(limiter_key)
            rapid_results.append(allowed)
            print(f"Rapid request {i+1}: {'✅ ALLOWED' if allowed else '❌ DENIED'}")

        rapid_denied = sum(1 for allowed in rapid_results if not allowed)
        assert (
            rapid_denied >= 3
        ), f"Should deny at least 3 rapid requests, denied {rapid_denied}"
        print(f"✅ Rate limiting test passed: {rapid_denied}/5 requests denied")

        # Test 3: Should allow requests again after time window
        print("\n--- Test 3: Token replenishment over time ---")

        # Wait for tokens to replenish (leaky bucket should refill)
        print("Waiting 2 seconds for token replenishment...")
        time.sleep(2)

        replenish_results = []
        for i in range(3):
            allowed = rate_limiter.acquire_token(limiter_key)
            replenish_results.append(allowed)
            print(
                f"Replenish request {i+1}: {'✅ ALLOWED' if allowed else '❌ DENIED'}"
            )
            time.sleep(0.5)  # Space out requests

        replenish_allowed = sum(replenish_results)
        assert (
            replenish_allowed >= 2
        ), f"Should allow at least 2 requests after replenishment, got {replenish_allowed}"
        print(
            f"✅ Token replenishment test passed: {replenish_allowed}/3 requests allowed"
        )

        # Test 4: Verify rate calculation
        print("\n--- Test 4: Rate calculation verification ---")

        total_start = time.time()
        total_requests = []

        # Make 10 requests with proper timing
        for i in range(10):
            allowed = rate_limiter.acquire_token(limiter_key)
            total_requests.append((time.time(), allowed))
            if allowed:
                print(f"Request {i+1}: ✅ ALLOWED at {time.time() - total_start:.2f}s")
            else:
                print(f"Request {i+1}: ❌ DENIED at {time.time() - total_start:.2f}s")
            time.sleep(0.1)  # Small delay between requests

        total_time = time.time() - total_start
        total_allowed = sum(1 for _, allowed in total_requests if allowed)
        actual_rate = total_allowed / total_time

        print(f"Total time: {total_time:.2f}s")
        print(f"Total allowed: {total_allowed}/10")
        print(f"Actual rate: {actual_rate:.2f} requests/second")

        # Should not exceed configured rate significantly
        assert (
            actual_rate <= 7
        ), f"Actual rate {actual_rate:.2f} should not exceed 7 req/s (5 + buffer)"
        print(f"✅ Rate calculation test passed: {actual_rate:.2f} req/s within limits")

    def test_concurrent_access_scenarios(self):
        """Test concurrent access scenarios for rate limiting."""
        print("\n=== TESTING CONCURRENT ACCESS SCENARIOS ===")

        # This test will initially FAIL since RedisRateLimiter class doesn't exist yet
        try:
            from utils.rate_limiter import RedisRateLimiter
        except ImportError:
            pytest.fail(
                "RedisRateLimiter class not found. This test is expected to FAIL initially "
                "until Step 2.2 (Implement Redis Rate Limiter) is completed."
            )

        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Create rate limiter with strict limits for testing
        rate_limiter = RedisRateLimiter(
            redis_client=self.redis_client,
            requests_per_second=3,  # Very strict limit for testing
            window_size_seconds=10,
            burst_allowance=5,
        )

        concurrent_key = f"test_concurrent:{datetime.now().isoformat()}"
        self.test_keys.append(concurrent_key)

        # Test 1: Multiple threads trying to acquire tokens simultaneously
        print("\n--- Test 1: Concurrent token acquisition ---")

        def acquire_token_worker(thread_id):
            """Worker function for concurrent token acquisition."""
            result = {
                "thread_id": thread_id,
                "timestamp": time.time(),
                "allowed": rate_limiter.acquire_token(concurrent_key),
                "attempt_time": datetime.now().isoformat(),
            }
            return result

        # Launch 20 threads simultaneously
        thread_count = 20
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

        # Verify only allowed number of requests proceeded
        # With burst_allowance=5 and 3 req/s, should allow ~5-8 requests max
        assert allowed_count <= 10, f"Too many requests allowed: {allowed_count}"
        assert denied_count >= 10, f"Not enough requests denied: {denied_count}"
        print(
            f"✅ Concurrent access control passed: {allowed_count} allowed, {denied_count} denied"
        )

        # Test 2: Verify window expiration and token refresh
        print("\n--- Test 2: Window expiration and token refresh ---")

        # Wait for rate limit window to expire
        print("Waiting for rate limit window to expire...")
        time.sleep(11)  # Window size is 10 seconds + buffer

        # Try requests again - should get fresh tokens
        refresh_results = []
        for i in range(8):
            allowed = rate_limiter.acquire_token(concurrent_key)
            refresh_results.append(allowed)
            print(f"Refresh request {i+1}: {'✅ ALLOWED' if allowed else '❌ DENIED'}")

        refresh_allowed = sum(refresh_results)
        assert (
            refresh_allowed >= 3
        ), f"Should allow at least 3 requests after window refresh, got {refresh_allowed}"
        print(
            f"✅ Window refresh test passed: {refresh_allowed}/8 requests allowed after window expiration"
        )

        # Test 3: Mixed concurrent and sequential access
        print("\n--- Test 3: Mixed concurrent and sequential access ---")

        mixed_key = f"test_mixed:{datetime.now().isoformat()}"
        self.test_keys.append(mixed_key)

        # First, make some sequential requests
        sequential_results = []
        for i in range(3):
            allowed = rate_limiter.acquire_token(mixed_key)
            sequential_results.append(allowed)
            print(f"Sequential {i+1}: {'✅ ALLOWED' if allowed else '❌ DENIED'}")
            time.sleep(0.2)

        # Then launch concurrent requests
        def mixed_worker(worker_id):
            return {
                "worker_id": worker_id,
                "allowed": rate_limiter.acquire_token(mixed_key),
                "timestamp": time.time(),
            }

        concurrent_results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(mixed_worker, i) for i in range(10)]
            for future in as_completed(futures):
                result = future.result()
                concurrent_results.append(result)
                worker_id = result["worker_id"]
                allowed = result["allowed"]
                print(
                    f"Concurrent worker {worker_id}: {'✅ ALLOWED' if allowed else '❌ DENIED'}"
                )

        total_sequential = sum(sequential_results)
        total_concurrent = sum(1 for r in concurrent_results if r["allowed"])
        total_mixed = total_sequential + total_concurrent

        print("\nMixed access results:")
        print(f"Sequential allowed: {total_sequential}/3")
        print(f"Concurrent allowed: {total_concurrent}/10")
        print(f"Total allowed: {total_mixed}/13")

        # Should still respect rate limits across both access patterns
        assert (
            total_mixed <= 12
        ), f"Mixed access allowed too many requests: {total_mixed}"
        print(
            f"✅ Mixed access test passed: total {total_mixed} requests allowed within limits"
        )

        print("\n✅ All concurrent access scenario tests completed")
