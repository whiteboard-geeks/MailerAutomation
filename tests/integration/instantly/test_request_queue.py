"""
Integration tests for request queue system functionality.

Step 3.1: Create Queue Test
This test is designed to FAIL initially to prove we need request queuing
before implementing the queue system. It tests queue creation, basic operations,
and worker pool functionality under load.
"""

import os
import time
import redis
import pytest
from datetime import datetime
from tests.utils.close_api import CloseAPI


class TestInstantlyRequestQueue:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Set up Redis for queue testing
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        try:
            self.redis_client = redis.from_url(self.redis_url)
            self.redis_client.ping()
            print(f"Successfully connected to Redis at: {self.redis_url}")
        except Exception as e:
            print(f"Warning: Failed to connect to Redis at {self.redis_url}: {e}")
            self.redis_client = None

        # Generate timestamp for unique queue testing
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.campaign_name = "QueueTest"

        # Track queue keys for cleanup
        self.queue_keys = []

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete test leads if they were created
        for lead_id in self.test_data.get("lead_ids", []):
            try:
                self.close_api.delete_lead(lead_id)
            except Exception as e:
                print(f"Warning: Could not delete test lead {lead_id}: {e}")

        # Clean up queue keys from Redis
        if self.redis_client:
            for key in self.queue_keys:
                try:
                    self.redis_client.delete(key)
                except Exception as e:
                    print(f"Warning: Could not cleanup queue key {key}: {e}")

    def test_redis_connection_for_queue(self):
        """Test that Redis connection is available for queue operations."""
        if not self.redis_client:
            pytest.skip("Redis not available for queue testing")

        print("\n=== TESTING REDIS CONNECTION FOR QUEUE ===")

        # Test basic queue operations that we'll need
        queue_key = f"test_queue:{self.timestamp}"
        self.queue_keys.append(queue_key)

        # Test LPUSH (add to queue)
        push_result = self.redis_client.lpush(queue_key, "test_item_1")
        assert push_result == 1, "LPUSH should return 1 for first item"

        # Test LLEN (queue length)
        length = self.redis_client.llen(queue_key)
        assert length == 1, "Queue should have 1 item"

        # Test RPOP (remove from queue)
        item = self.redis_client.rpop(queue_key)
        assert item.decode("utf-8") == "test_item_1", "Should retrieve the same item"

        # Test empty queue
        empty_length = self.redis_client.llen(queue_key)
        assert empty_length == 0, "Queue should be empty after pop"

        print("✅ Redis queue operations working correctly")

    def test_request_queue_creation_and_basic_operations(self):
        """Test queue creation and basic operations - this should FAIL initially."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING REQUEST QUEUE CREATION AND BASIC OPERATIONS ===")

        # This test should FAIL because InstantlyRequestQueue doesn't exist yet
        try:
            from utils.async_queue import InstantlyRequestQueue
        except ImportError:
            pytest.fail(
                "InstantlyRequestQueue class not found in utils.async_queue. "
                "This test is expected to FAIL initially until Step 3.2 "
                "(Implement Request Queue) is completed."
            )

        # Initialize the queue
        queue = InstantlyRequestQueue(
            redis_client=self.redis_client,
            max_workers=5,
            queue_name=f"test_queue_{self.timestamp}",
        )

        # Test basic queue operations
        test_request = {
            "campaign_id": "test_campaign_123",
            "email": f"test+{self.timestamp}@example.com",
            "first_name": "Test",
            "last_name": "User",
            "company_name": "Test Company",
            "date_location": "Test Location",
        }

        # Test adding request to queue
        future = queue.enqueue_request(test_request)
        assert future is not None, "Should return a Future object"

        # Test queue status
        status = queue.get_queue_status()
        assert status["queued"] >= 1, "Should have at least 1 queued request"

        print("✅ Request queue basic operations working")

    def test_worker_pool_functionality(self):
        """Test worker pool functionality - this should FAIL initially."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING WORKER POOL FUNCTIONALITY ===")

        try:
            from utils.async_queue import InstantlyRequestQueue
        except ImportError:
            pytest.fail(
                "InstantlyRequestQueue class not found. "
                "This test is expected to FAIL initially until Step 3.2 is completed."
            )

        # Initialize queue with worker pool
        queue = InstantlyRequestQueue(
            redis_client=self.redis_client,
            max_workers=3,  # Small worker pool for testing
            queue_name=f"test_worker_pool_{self.timestamp}",
        )

        # Test worker pool initialization
        assert queue.max_workers == 3, "Should have 3 workers"
        assert not queue.is_running(), "Should not be running initially"

        # Start the worker pool
        queue.start_workers()
        assert queue.is_running(), "Should be running after start"

        # Test worker processing (add some test requests)
        futures = []
        for i in range(5):
            test_request = {
                "campaign_id": "test_campaign_123",
                "email": f"test+{self.timestamp}+{i}@example.com",
                "first_name": "Test",
                "last_name": f"User{i}",
            }
            future = queue.enqueue_request(test_request)
            futures.append(future)

        # Wait for processing (with timeout)
        start_time = time.time()
        completed = 0
        while completed < len(futures) and time.time() - start_time < 30:
            for future in futures:
                if future.done() and not hasattr(future, "_counted"):
                    completed += 1
                    future._counted = True
            time.sleep(0.1)

        # Stop the worker pool
        queue.stop_workers()
        assert not queue.is_running(), "Should not be running after stop"

        # Verify some requests were processed
        assert completed > 0, f"Should have processed some requests, got {completed}"

        print(f"✅ Worker pool processed {completed}/{len(futures)} requests")

    def test_queue_processing_under_load(self):
        """Test queue processing under load with 100 simultaneous requests."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING QUEUE PROCESSING UNDER LOAD (100 requests) ===")

        try:
            from utils.async_queue import InstantlyRequestQueue
        except ImportError:
            pytest.fail(
                "InstantlyRequestQueue class not found. "
                "This test is expected to FAIL initially until Step 3.2 is completed."
            )

        # Initialize queue for load testing
        queue = InstantlyRequestQueue(
            redis_client=self.redis_client,
            max_workers=5,
            queue_name=f"test_load_{self.timestamp}",
        )

        # Start workers
        queue.start_workers()

        print("Queueing 100 requests simultaneously...")
        futures = []
        start_time = time.time()

        # Queue 100 requests simultaneously
        for i in range(100):
            test_request = {
                "campaign_id": "test_campaign_load",
                "email": f"test+load+{self.timestamp}+{i}@example.com",
                "first_name": "Load",
                "last_name": f"Test{i}",
                "company_name": f"Load Test Company {i}",
            }
            future = queue.enqueue_request(test_request)
            futures.append(future)

        queuing_time = time.time() - start_time
        print(f"Queued 100 requests in {queuing_time:.2f} seconds")

        # Monitor processing
        processing_start = time.time()
        completed = 0
        last_completed = 0

        while (
            completed < len(futures) and time.time() - processing_start < 120
        ):  # 2 minute timeout
            completed = sum(1 for f in futures if f.done())

            if completed != last_completed:
                elapsed = time.time() - processing_start
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"Progress: {completed}/100 completed, Rate: {rate:.2f} req/s")
                last_completed = completed

            time.sleep(1)  # Check every second

        processing_time = time.time() - processing_start

        # Stop workers
        queue.stop_workers()

        # Verify controlled processing rate
        if completed > 0:
            avg_rate = completed / processing_time
            print(
                f"Final: {completed}/100 requests processed in {processing_time:.1f}s"
            )
            print(f"Average processing rate: {avg_rate:.2f} requests/second")

            # Verify rate respects limits (should be ≤8 req/s due to rate limiter integration)
            assert (
                avg_rate <= 10
            ), f"Processing rate {avg_rate:.2f} should be ≤10 req/s (Instantly limit)"

            if avg_rate <= 8:
                print("✅ Processing rate respects rate limiter (≤8 req/s)")
            else:
                print(f"⚠️  Processing rate {avg_rate:.2f} req/s is close to limit")

        else:
            pytest.fail("No requests were processed within timeout period")

        print("✅ Queue processing under load test completed")

    def test_queue_integration_with_rate_limiter(self):
        """Test that queue system integrates properly with rate limiter from Step 2."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING QUEUE INTEGRATION WITH RATE LIMITER ===")

        try:
            from utils.async_queue import InstantlyRequestQueue
            from utils.rate_limiter import RedisRateLimiter, APIRateConfig
        except ImportError:
            pytest.fail(
                "Required classes not found. "
                "This test is expected to FAIL initially until Step 3.2 is completed."
            )

        # Initialize rate limiter (from Step 2)
        rate_limiter = RedisRateLimiter(
            redis_client=self.redis_client,
            api_config=APIRateConfig.instantly(),
            safety_factor=0.8,
        )

        # Initialize queue with rate limiter integration
        queue = InstantlyRequestQueue(
            redis_client=self.redis_client,
            max_workers=3,
            rate_limiter=rate_limiter,
            queue_name=f"test_integration_{self.timestamp}",
        )

        # Test that queue respects rate limiting
        queue.start_workers()

        # Queue several requests
        futures = []
        for i in range(10):
            test_request = {
                "campaign_id": "test_campaign_integration",
                "email": f"test+integration+{self.timestamp}+{i}@example.com",
                "first_name": "Integration",
                "last_name": f"Test{i}",
            }
            future = queue.enqueue_request(test_request)
            futures.append(future)

        # Monitor processing with timing
        start_time = time.time()
        completed = 0

        while completed < len(futures) and time.time() - start_time < 60:
            completed = sum(1 for f in futures if f.done())
            time.sleep(0.5)

        processing_time = time.time() - start_time
        queue.stop_workers()

        if completed > 0:
            avg_rate = completed / processing_time
            print(f"Processed {completed}/10 requests in {processing_time:.1f}s")
            print(f"Rate with integration: {avg_rate:.2f} requests/second")

            # Should be rate limited to ≤8 req/s
            assert (
                avg_rate <= 8
            ), f"Rate should be limited to ≤8 req/s, got {avg_rate:.2f}"
            print("✅ Queue properly integrates with rate limiter")
        else:
            pytest.fail("No requests processed - integration may be broken")
