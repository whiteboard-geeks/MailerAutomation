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
