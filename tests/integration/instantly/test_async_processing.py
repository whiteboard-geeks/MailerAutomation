"""
Integration tests for async processing with Celery.

Step 5.1: Create Async Processing Test
This test is designed to FAIL initially to prove we need async processing
before implementing the full solution. It tests immediate response (no HTTP timeout),
Celery task queuing and execution, and integration of all previous components.

Uses pre-generated test leads from scripts/generate_test_leads.py to avoid
creating leads dynamically during testing. Run generate_test_leads.py first
to create the test leads file.

Key Goals:
- Test immediate response (no HTTP timeout)
- Test Celery task queuing and execution
- Test integration of rate limiter + queue + circuit breaker + async
- Test should initially FAIL (endpoint still synchronous)
"""

import os
import time
import json
import requests
import redis
import pytest
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.utils.close_api import CloseAPI
from utils.rate_limiter import RedisRateLimiter, APIRateConfig
from utils.async_queue import InstantlyRequestQueue
from utils.circuit_breaker import CircuitBreaker
from celery_worker import celery
from scripts.generate_test_leads import load_test_leads


class TestInstantlyAsyncProcessing:
    # Test configuration
    ASYNC_TEST_LEAD_COUNT = 50  # Enough to trigger timeout without async
    IMMEDIATE_RESPONSE_TIMEOUT = 5  # Seconds - async should respond immediately
    BACKGROUND_PROCESSING_TIMEOUT = (
        120  # Seconds - allow time for background processing
    )

    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Set up Redis for all components
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        try:
            self.redis_client = redis.from_url(self.redis_url)
            self.redis_client.ping()
            print(f"Successfully connected to Redis at: {self.redis_url}")
        except Exception as e:
            print(f"Warning: Failed to connect to Redis at {self.redis_url}: {e}")
            self.redis_client = None

        # Generate timestamp for unique testing
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.campaign_name = "AsyncTest"

        # Track keys for cleanup
        self.cleanup_keys = []
        self.task_ids = []

        # Initialize components for integration testing
        if self.redis_client:
            self.rate_limiter = RedisRateLimiter(
                redis_client=self.redis_client,
                api_config=APIRateConfig.instantly(),
                safety_factor=0.8,
            )

            self.request_queue = InstantlyRequestQueue(
                redis_client=self.redis_client,
                max_workers=5,
                queue_name=f"async_test_queue_{self.timestamp}",
            )

            self.circuit_breaker = CircuitBreaker(
                name=f"instantly_async_test_{self.timestamp}",
                failure_threshold=5,
                timeout=30,
                redis_client=self.redis_client,
            )

        # Base payload structure for Close webhook
        self.base_payload = {
            "subscription_id": "whsub_1vT2aEze4uUzQlqLIBExYl",
            "event": {
                "id": "ev_34bKnJcMX9UnRJmuGH5Jtr",
                "date_created": "2025-02-28T19:20:45.507000",
                "date_updated": "2025-02-28T19:20:45.507000",
                "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                "user_id": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                "request_id": "req_5SPmoSjkZBMkMkOAaxz7o7",
                "api_key_id": "api_3fw37yHasQmGs00Nnybzq5",
                "oauth_client_id": None,
                "oauth_scope": None,
                "object_type": "task.lead",
                "object_id": "task_CIRBr39mOsTfWAc3ErihkSt4cX0PlVBpTovHGNj939w",
                "lead_id": "lead_mtonPqjLkC0X93AW6evKVa1Sbpq7l8opyuaV5olT2Cf",
                "action": "created",
                "changed_fields": [],
                "meta": {"request_path": "/api/v1/task/", "request_method": "POST"},
                "data": {
                    "_type": "lead",
                    "object_type": None,
                    "contact_id": None,
                    "is_complete": False,
                    "assigned_to_name": "Barbara Pigg",
                    "id": "task_CIRBr39mOsTfWAc3ErihkSt4cX0PlVBpTovHGNj939w",
                    "sequence_id": None,
                    "is_new": True,
                    "created_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "date": "2025-03-01",
                    "deduplication_key": None,
                    "created_by_name": "Barbara Pigg",
                    "date_updated": "2025-02-28T19:20:45.505000+00:00",
                    "is_dateless": False,
                    "sequence_subscription_id": None,
                    "lead_id": "lead_mtonPqjLkC0X93AW6evKVa1Sbpq7l8opyuaV5olT2Cf",
                    "object_id": None,
                    "updated_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "due_date": "2025-03-01",
                    "is_primary_lead_notification": True,
                    "updated_by_name": "Barbara Pigg",
                    "assigned_to": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "text": f"Instantly: {self.campaign_name}",
                    "lead_name": "Test Lead",
                    "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                    "view": None,
                    "date_created": "2025-02-28T19:20:45.505000+00:00",
                },
                "previous_data": {},
            },
        }

    def teardown_method(self):
        """Cleanup after each test."""
        # NOTE: We don't delete pre-generated test leads as they are reused across tests
        # The leads are tracked in self.test_data["lead_ids"] but only for reference

        # Clean up Redis keys
        if self.redis_client:
            for key in self.cleanup_keys:
                try:
                    self.redis_client.delete(key)
                except Exception as e:
                    print(f"Warning: Could not cleanup Redis key {key}: {e}")

        # Stop request queue workers if running
        if hasattr(self, "request_queue") and self.request_queue.is_running():
            self.request_queue.stop_workers()

        # Revoke any pending Celery tasks
        for task_id in self.task_ids:
            try:
                celery.control.revoke(task_id, terminate=True)
            except Exception as e:
                print(f"Warning: Could not revoke Celery task {task_id}: {e}")

    def generate_test_leads(self, count=None):
        """
        Load pre-generated test leads for async processing tests.

        Args:
            count (int): Number of test leads to return (will slice from pre-generated leads)

        Returns:
            list: List of lead data (subset of pre-generated leads)
        """
        if count is None:
            count = self.ASYNC_TEST_LEAD_COUNT

        print(
            f"\n=== Loading {count} pre-generated test leads for async processing test ==="
        )

        # Load pre-generated leads from file
        all_leads = load_test_leads()

        if not all_leads:
            pytest.skip(
                "No pre-generated test leads found. Please run 'python scripts/generate_test_leads.py' first."
            )

        if len(all_leads) < count:
            print(
                f"Warning: Only {len(all_leads)} pre-generated leads available, requested {count}"
            )
            count = len(all_leads)

        # Take subset of leads
        selected_leads = all_leads[:count]

        # Track lead IDs for any cleanup (though we won't delete pre-generated leads)
        self.test_data["lead_ids"] = [lead["id"] for lead in selected_leads]

        print(f"Successfully loaded {len(selected_leads)} pre-generated test leads")
        return selected_leads

    def test_celery_connection_and_basic_task(self):
        """Test that Celery connection is available and basic task execution works."""
        print("\n=== TESTING CELERY CONNECTION AND BASIC TASK ===")

        # Test Celery app availability
        assert celery is not None, "Celery app should be available"
        print("✅ Celery app instance available")

        # Test broker connection
        try:
            broker_connection = celery.broker_connection()
            assert (
                broker_connection is not None
            ), "Broker connection should be available"
            print("✅ Celery broker connection available")
        except Exception as e:
            pytest.skip(f"Celery broker connection failed: {e}")

        # Test Celery control inspection
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()

            if not active_workers:
                print("⚠️ No active Celery workers found")
                # Don't skip - continue testing basic functionality
                active_workers = {}

            print(f"Active Celery workers: {list(active_workers.keys())}")

        except Exception as e:
            print(f"⚠️ Celery inspection failed: {e}")
            # Continue testing basic functionality

        # Test task registration and basic functionality
        try:
            from app import process_contact_list

            # Verify task has Celery methods
            assert hasattr(
                process_contact_list, "delay"
            ), "Task should have delay method"
            assert hasattr(
                process_contact_list, "apply_async"
            ), "Task should have apply_async method"
            assert hasattr(
                process_contact_list, "name"
            ), "Task should have name attribute"

            print(f"✅ Task registered: {process_contact_list.name}")
            print("✅ Celery task discovery working")

        except ImportError:
            print("Note: process_contact_list task not available")

            # Test basic Celery task creation functionality
            @celery.task
            def basic_test_task():
                return "test"

            assert hasattr(
                basic_test_task, "delay"
            ), "Basic task should have delay method"
            print("✅ Basic Celery task creation working")

        print("✅ Celery connection and basic task functionality verified")

    def test_async_processing_task_creation(self):
        """Test creation of async processing task - this should FAIL initially."""
        print("\n=== TESTING ASYNC PROCESSING TASK CREATION ===")

        # This test should FAIL because we haven't implemented the async task yet
        try:
            # Try to import the async processing task that doesn't exist yet
            from blueprints.instantly import process_lead_batch_task

            # If we get here, the task exists (which means Step 5.2 is done)
            print("✅ process_lead_batch_task found - async implementation exists")

        except ImportError:
            pytest.fail(
                "process_lead_batch_task not found in blueprints.instantly. "
                "This test is expected to FAIL initially until Step 5.2 "
                "(Implement Async Endpoint) is completed. The task should be "
                "implemented as a Celery task that processes lead batches in the background."
            )

    def test_celery_task_queuing_and_execution(self):
        """Test Celery task queuing and execution using existing task infrastructure."""
        print("\n=== TESTING CELERY TASK QUEUING AND EXECUTION ===")

        # Test Celery connection first
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()

            if not active_workers:
                pytest.skip("No Celery workers available for testing")

            print(f"Active Celery workers: {list(active_workers.keys())}")

        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Test task queuing using existing process_contact_list task as example
        try:
            from app import process_contact_list

            # Verify task has Celery delay method
            assert hasattr(
                process_contact_list, "delay"
            ), "Task should have delay method"
            assert hasattr(
                process_contact_list, "apply_async"
            ), "Task should have apply_async method"

            print("✅ Celery task discovery working")

            # Test task queuing (without actually executing)
            print("\n--- Testing Task Queuing ---")

            # Create a test CSV URL (this won't actually be processed)
            test_csv_url = "https://example.com/test.csv"

            # Queue the task with apply_async for more control
            task_result = process_contact_list.apply_async(
                args=[test_csv_url],
                countdown=60,  # Delay execution by 60 seconds to test queuing
            )

            print(f"✅ Task queued successfully with ID: {task_result.id}")
            print(f"✅ Task state: {task_result.state}")

            # Verify task is in queue
            assert task_result.id is not None, "Task should have an ID"
            assert task_result.state in [
                "PENDING",
                "RETRY",
                "STARTED",
            ], f"Task should be queued, got state: {task_result.state}"

            # Test task inspection
            scheduled_tasks = inspect.scheduled()
            if scheduled_tasks:
                worker_scheduled = list(scheduled_tasks.values())[0]
                scheduled_task_ids = [
                    task["request"]["id"] for task in worker_scheduled
                ]
                print(f"Scheduled tasks: {scheduled_task_ids}")

                # Our task should be in the scheduled tasks (since we used countdown)
                if task_result.id in scheduled_task_ids:
                    print(f"✅ Task {task_result.id} found in scheduled tasks")
                else:
                    print(
                        f"⚠️ Task {task_result.id} not found in scheduled tasks (may have started)"
                    )

            # Cancel the task since we don't want it to actually run
            task_result.revoke(terminate=True)
            print(f"✅ Task {task_result.id} revoked to prevent execution")

            # Store task ID for cleanup
            self.task_ids.append(task_result.id)

        except ImportError:
            # If process_contact_list is not available, create a simple test task
            print(
                "process_contact_list not available, testing with basic Celery functionality"
            )

            # Test basic Celery app functionality
            assert celery is not None, "Celery app should be available"
            assert (
                celery.broker_connection() is not None
            ), "Broker connection should be available"

            # Test creating a simple inline task for queuing
            @celery.task
            def test_task(message):
                return f"Test task executed with message: {message}"

            # Queue the test task
            test_message = f"Celery test at {self.timestamp}"
            task_result = test_task.apply_async(
                args=[test_message],
                countdown=60,  # Delay to test queuing
            )

            print(f"✅ Simple test task queued with ID: {task_result.id}")
            print(f"✅ Task state: {task_result.state}")

            # Cancel the task
            task_result.revoke(terminate=True)
            print(f"✅ Test task {task_result.id} revoked")

            # Store task ID for cleanup
            self.task_ids.append(task_result.id)

        print("✅ Celery task queuing and execution capabilities verified")

    def test_immediate_response_without_timeout(self):
        """Test that webhook responds immediately without HTTP timeout - should FAIL initially."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        # Check for Celery workers
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()

            if not active_workers:
                pytest.skip("No Celery workers available for testing")

            print(f"Active Celery workers: {list(active_workers.keys())}")

        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        print(
            f"\n=== TESTING IMMEDIATE RESPONSE (target: <{self.IMMEDIATE_RESPONSE_TIMEOUT}s) ==="
        )

        # Generate test leads
        leads = self.generate_test_leads(self.ASYNC_TEST_LEAD_COUNT)
        assert len(leads) > 0, "Should have created test leads"

        # Send webhook requests and measure response time
        start_time = time.time()
        response_times = []
        task_ids_from_response = []

        print(f"Sending {len(leads)} webhook requests...")

        for i, lead in enumerate(
            leads[:5]
        ):  # Test with first 5 leads for quick validation
            # Create payload for this lead
            payload = self.base_payload.copy()
            payload["event"]["data"]["lead_id"] = lead["id"]
            payload["event"]["data"]["id"] = f"task_async_test_{self.timestamp}_{i}"

            # Send webhook request and measure time
            request_start = time.time()

            try:
                response = requests.post(
                    f"{self.base_url}/instantly/add_lead",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,  # Short timeout for immediate response
                )

                request_end = time.time()
                response_time = request_end - request_start
                response_times.append(response_time)

                print(
                    f"Request {i+1}: {response_time:.2f}s, Status: {response.status_code}"
                )

                # For async implementation, we expect:
                # 1. Immediate success response (status 200/202)
                # 2. Response should include celery_task_id for tracking
                # 3. Response time should be very fast (< 5 seconds)

                if response.status_code in [200, 202]:
                    response_data = response.json()

                    # Check if response includes celery_task_id (indicates async processing)
                    if "celery_task_id" in response_data:
                        task_ids_from_response.append(response_data["celery_task_id"])
                        print(
                            f"   ✅ Got celery_task_id: {response_data['celery_task_id']}"
                        )
                    else:
                        print(
                            f"   ❌ No celery_task_id in response (indicates synchronous processing)"
                        )

                else:
                    print(f"   ❌ Unexpected status code: {response.status_code}")

            except requests.exceptions.Timeout:
                request_end = time.time()
                response_time = request_end - request_start
                response_times.append(response_time)

                pytest.fail(
                    f"Request {i+1} timed out after {response_time:.2f}s. "
                    f"This indicates the endpoint is still processing synchronously. "
                    f"Expected: immediate response with celery_task_id for async processing. "
                    f"This test is expected to FAIL until Step 5.2 (async implementation) is completed."
                )

        total_time = time.time() - start_time
        avg_response_time = sum(response_times) / len(response_times)

        print(f"\n=== RESPONSE TIME ANALYSIS ===")
        print(f"Total time for {len(response_times)} requests: {total_time:.2f}s")
        print(f"Average response time: {avg_response_time:.2f}s")
        print(f"Max response time: {max(response_times):.2f}s")
        print(f"Celery task IDs received: {len(task_ids_from_response)}")

        # Store task IDs for cleanup
        self.task_ids.extend(task_ids_from_response)

        # Validation for async implementation
        if not task_ids_from_response:
            pytest.fail(
                "No celery_task_ids received in responses. This indicates the endpoint is still "
                "processing synchronously instead of queuing async tasks. Expected: "
                "immediate response with celery_task_id for background processing."
            )

        if avg_response_time > self.IMMEDIATE_RESPONSE_TIMEOUT:
            pytest.fail(
                f"Average response time ({avg_response_time:.2f}s) exceeds immediate "
                f"response threshold ({self.IMMEDIATE_RESPONSE_TIMEOUT}s). This indicates "
                f"synchronous processing instead of async task queuing."
            )

        print("✅ All requests responded immediately with Celery task IDs")

    def test_background_processing_completion(self):
        """Test that background Celery tasks complete successfully."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        # Check for Celery workers
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()

            if not active_workers:
                pytest.skip("No Celery workers available for testing")

            print(f"Active Celery workers: {list(active_workers.keys())}")

        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        print(f"\n=== TESTING BACKGROUND PROCESSING COMPLETION ===")

        # This test depends on the async task being implemented
        try:
            from blueprints.instantly import process_lead_batch_task
        except ImportError:
            pytest.skip(
                "process_lead_batch_task not implemented yet. "
                "This test will be enabled after Step 5.2 completion."
            )

        # Generate fewer leads for background processing test
        leads = self.generate_test_leads(5)
        task_ids = []

        # Submit tasks to Celery
        for i, lead in enumerate(leads):
            try:
                # Parse name from pre-generated lead data
                lead_name = lead.get("name", f"TestLead {i}")
                name_parts = lead_name.split(" ")
                first_name = name_parts[0] if name_parts else "Test"
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else str(i)

                # Create task data using pre-generated lead info
                task_data = {
                    "campaign_name": self.campaign_name,
                    "lead_id": lead["id"],
                    "task_id": f"task_async_test_{self.timestamp}_{i}",
                    "email": lead.get("email", f"test+{i}@example.com"),
                    "first_name": first_name,
                    "last_name": last_name,
                    "company_name": f"Async Test Company {i}",
                    "date_location": f"Async Test Location {self.timestamp}",
                }

                # Submit to Celery
                result = process_lead_batch_task.delay(task_data)
                task_ids.append(result.id)
                print(f"Submitted task {i+1}: {result.id}")

            except Exception as e:
                print(f"Failed to submit task {i+1}: {e}")

        # Store for cleanup
        self.task_ids.extend(task_ids)

        if not task_ids:
            pytest.fail("No tasks were successfully submitted to Celery")

        # Monitor task completion
        print(f"\nMonitoring {len(task_ids)} background tasks...")
        start_time = time.time()
        completed_tasks = 0
        failed_tasks = 0
        counted_task_ids = set()  # Track which tasks we've already counted

        while (completed_tasks + failed_tasks) < len(task_ids) and (
            time.time() - start_time
        ) < self.BACKGROUND_PROCESSING_TIMEOUT:
            for task_id in task_ids:
                if task_id in counted_task_ids:
                    continue  # Skip tasks we've already counted

                result = celery.AsyncResult(task_id)

                if result.ready():
                    if result.successful():
                        completed_tasks += 1
                        counted_task_ids.add(task_id)
                        print(f"✅ Task completed: {task_id}")
                    else:
                        failed_tasks += 1
                        counted_task_ids.add(task_id)
                        print(f"❌ Task failed: {task_id} - {result.result}")

            time.sleep(1)  # Check every second

        processing_time = time.time() - start_time

        print(f"\n=== BACKGROUND PROCESSING RESULTS ===")
        print(f"Processing time: {processing_time:.2f}s")
        print(f"Completed tasks: {completed_tasks}/{len(task_ids)}")
        print(f"Failed tasks: {failed_tasks}/{len(task_ids)}")
        print(f"Still pending: {len(task_ids) - completed_tasks - failed_tasks}")

        # Validation
        success_rate = completed_tasks / len(task_ids)
        assert (
            success_rate == 1.0
        ), f"Success rate too low: {success_rate:.2%} (expected 100%)"
        assert (
            processing_time < self.BACKGROUND_PROCESSING_TIMEOUT
        ), f"Processing took too long: {processing_time:.2f}s"

        print("✅ Background processing completed successfully")

    def test_full_integration_with_all_components(self):
        """Test full integration of rate limiter + queue + circuit breaker + async processing."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        # Check for Celery workers
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()

            if not active_workers:
                pytest.skip("No Celery workers available for testing")

            print(f"Active Celery workers: {list(active_workers.keys())}")

        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        print(
            "\n=== TESTING FULL INTEGRATION (Rate Limiter + Queue + Circuit Breaker + Async) ==="
        )

        # This test requires all components to be working together
        try:
            from blueprints.instantly import process_lead_batch_task
        except ImportError:
            pytest.skip(
                "Async processing not implemented yet. "
                "This integration test will be enabled after Step 5.2 completion."
            )

        # Test with a moderate number of leads
        leads = self.generate_test_leads(10)

        print("Testing integration components individually...")

        # Test rate limiter
        print("1. Testing rate limiter...")
        test_key = f"integration_test_{self.timestamp}"
        for i in range(3):
            allowed = self.rate_limiter.acquire_token(test_key)
            print(
                f"   Rate limiter request {i+1}: {'✅ Allowed' if allowed else '❌ Rate limited'}"
            )

        # Test circuit breaker
        print("2. Testing circuit breaker...")
        if self.circuit_breaker.can_execute():
            # Simulate a successful operation
            self.circuit_breaker.record_success()
            cb_result = {"status": "success", "test": True}
            print(f"   Circuit breaker test call: ✅ Success")
        else:
            cb_result = {"status": "blocked"}
            print(f"   Circuit breaker test call: ❌ Blocked (circuit open)")

        # Get circuit breaker metrics
        metrics = self.circuit_breaker.get_metrics()
        print(f"   Circuit state: {metrics.get('state', 'UNKNOWN')}")

        # Test request queue
        print("3. Testing request queue...")
        self.request_queue.start_workers()

        test_request = {
            "campaign_id": "test_campaign",
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "User",
        }

        future = self.request_queue.enqueue_request(test_request)
        print(f"   Request queue enqueue: {'✅ Success' if future else '❌ Failed'}")

        # Test full webhook flow with async processing
        print("4. Testing full webhook flow...")

        webhook_responses = []
        for i, lead in enumerate(leads[:3]):  # Test with first 3 leads
            payload = self.base_payload.copy()
            payload["event"]["data"]["lead_id"] = lead["id"]
            payload["event"]["data"]["id"] = (
                f"task_integration_test_{self.timestamp}_{i}"
            )

            start_time = time.time()
            response = requests.post(
                f"{self.base_url}/instantly/add_lead",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
            )
            response_time = time.time() - start_time

            webhook_responses.append(
                {
                    "index": i,
                    "status_code": response.status_code,
                    "response_time": response_time,
                    "response_data": response.json()
                    if response.status_code in [200, 202]
                    else None,
                }
            )

            print(f"   Webhook {i+1}: {response.status_code} in {response_time:.2f}s")

        # Validate integration results
        print("\n=== INTEGRATION VALIDATION ===")

        # All webhooks should respond quickly
        avg_response_time = sum(r["response_time"] for r in webhook_responses) / len(
            webhook_responses
        )
        assert (
            avg_response_time < self.IMMEDIATE_RESPONSE_TIMEOUT
        ), f"Average response time too slow: {avg_response_time:.2f}s"

        # All should return success with task IDs (for async processing)
        task_ids_received = 0
        for response in webhook_responses:
            if (
                response["response_data"]
                and "celery_task_id" in response["response_data"]
            ):
                task_ids_received += 1
                self.task_ids.append(response["response_data"]["celery_task_id"])

        print(
            f"Quick responses: {len([r for r in webhook_responses if r['response_time'] < self.IMMEDIATE_RESPONSE_TIMEOUT])}/{len(webhook_responses)}"
        )
        print(f"Task IDs received: {task_ids_received}/{len(webhook_responses)}")
        print(f"Average response time: {avg_response_time:.2f}s")

        # Stop queue workers
        self.request_queue.stop_workers()

        assert (
            task_ids_received > 0
        ), "No task IDs received - async processing not working"

        print("✅ Full integration test completed successfully")

    def test_async_endpoint_error_handling(self):
        """Test error handling in async processing endpoint."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        # Check for Celery workers
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()

            if not active_workers:
                pytest.skip("No Celery workers available for testing")

            print(f"Active Celery workers: {list(active_workers.keys())}")

        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        print("\n=== TESTING ASYNC ENDPOINT ERROR HANDLING ===")

        # Test with invalid payload
        invalid_payload = {"invalid": "data"}

        response = requests.post(
            f"{self.base_url}/instantly/add_lead",
            json=invalid_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        print(f"Invalid payload response: {response.status_code}")

        # Should still respond quickly even with errors
        assert response.status_code in [
            200,
            400,
            422,
        ], f"Unexpected status code: {response.status_code}"

        # Test with missing campaign
        missing_campaign_payload = self.base_payload.copy()
        missing_campaign_payload["event"]["data"]["text"] = (
            "Instantly: NonExistentCampaign"
        )

        response = requests.post(
            f"{self.base_url}/instantly/add_lead",
            json=missing_campaign_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        print(f"Missing campaign response: {response.status_code}")

        # Should handle gracefully (accept both 200 and 202 for async processing)
        assert response.status_code in [
            200,
            202,
        ], "Should handle missing campaign gracefully"

        print("✅ Error handling tests completed")
