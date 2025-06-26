"""
Integration tests for async EasyPost delivery status processing.

This test converts the synchronous delivery_status tests to async processing,
following TDD approach where tests should FAIL initially until async implementation is complete.

Key differences from synchronous tests:
- Expects 202 status code (not 200)
- Expects celery_task_id in response
- Tests immediate response time (<5 seconds)
- Tests background task completion
- Uses WebhookTracker to monitor async progress
"""

import os
import time
import json
import pytest
import requests
from datetime import datetime
from tests.utils.close_api import CloseAPI
from tests.utils.easypost_mock import EasyPostMock
from celery_worker import celery


class TestAsyncEasyPostDeliveryStatus:
    # Test configuration
    IMMEDIATE_RESPONSE_TIMEOUT = 5  # Seconds - async should respond immediately
    BACKGROUND_PROCESSING_TIMEOUT = (
        120  # Seconds - allow time for background processing
    )

    @classmethod
    def setup_class(cls):
        """Setup before all tests in the class."""
        # Clean up any lingering test data from previous runs
        close_api = CloseAPI()

        # Search for any leads with test tracking numbers
        for test_number in ["EZ1000000001", "EZ4000000004"]:
            test_leads = close_api.search_leads_by_tracking_number(test_number)
            for lead in test_leads:
                print(f"Cleaning up existing test lead with ID: {lead['id']}")
                close_api.delete_lead(lead["id"])

    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Track Celery task IDs for cleanup
        self.task_ids = []

        # Generate timestamp for unique identification
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        env_type = os.environ.get("ENV_TYPE", "testing")

        # Test tracking number that will return 'delivered' status
        self.test_tracking_number = "EZ1000000001"
        self.test_carrier = "USPS"

        # Generate unique test data
        self.test_first_name = "Lance"
        self.test_last_name = f"AsyncDelivery{self.timestamp}"
        self.test_email = (
            f"lance+{env_type}.async.delivery{self.timestamp}@whiteboardgeeks.com"
        )

        # Load the mock webhook payloads
        with open(
            "tests/integration/easypost/close_tracking_number_and_carrier_updated.json",
            "r",
        ) as f:
            self.mock_payload = json.load(f)

        with open(
            "tests/integration/easypost/easypost_package_delivered.json", "r"
        ) as f:
            self.delivery_webhook_payload = json.load(f)

        # Save original ENV_TYPE value to restore later
        self.original_env_type = os.environ.get("ENV_TYPE")
        print(f"Original ENV_TYPE: {self.original_env_type}")

        # Set ENV_TYPE to testing for this test
        os.environ["ENV_TYPE"] = "testing"
        print("Set ENV_TYPE to 'testing' for this test")

    def teardown_method(self):
        """Cleanup after each test."""
        # Restore original ENV_TYPE
        if self.original_env_type:
            os.environ["ENV_TYPE"] = self.original_env_type
        else:
            os.environ.pop("ENV_TYPE", None)

        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            result = self.close_api.delete_lead(self.test_data["lead_id"])
            if result == {}:  # Successful deletion returns empty dict
                print(f"Deleted lead with ID: {self.test_data['lead_id']}")
            else:
                print(f"Warning: Lead deletion may have failed: {result}")

        # Revoke any pending Celery tasks
        for task_id in self.task_ids:
            try:
                celery.control.revoke(task_id, terminate=True)
                print(f"Revoked Celery task: {task_id}")
            except Exception as e:
                print(f"Warning: Could not revoke Celery task {task_id}: {e}")

    def wait_for_async_task_completion(self, task_id, timeout=None):
        """Wait for async Celery task to complete."""
        if timeout is None:
            timeout = self.BACKGROUND_PROCESSING_TIMEOUT

        start_time = time.time()

        while (time.time() - start_time) < timeout:
            try:
                # Check task status using Celery AsyncResult
                result = celery.AsyncResult(task_id)

                if result.ready():
                    if result.successful():
                        return {
                            "status": "success",
                            "result": result.result,
                            "task_id": task_id,
                        }
                    else:
                        return {
                            "status": "failed",
                            "error": str(result.result),
                            "task_id": task_id,
                        }

                # Print progress occasionally
                elapsed = time.time() - start_time
                if elapsed % 30 < 1:  # Print every ~30 seconds
                    print(
                        f"Still waiting for task {task_id}... {int(elapsed)}s elapsed"
                    )

                time.sleep(1)  # Check every second

            except Exception as e:
                print(f"Error checking task status: {e}")
                time.sleep(1)

        # Timeout
        raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")

    def wait_for_webhook_tracker_update(
        self, tracker_id=None, tracking_code=None, timeout=None
    ):
        """Wait for webhook tracker to be updated with processing results."""
        webhook_endpoint = f"{self.base_url}/easypost/webhooks/status"

        # Add filters if provided
        if tracker_id:
            webhook_endpoint += f"?tracker_id={tracker_id}"
        elif tracking_code:
            webhook_endpoint += f"?tracking_code={tracking_code}"

        start_time = time.time()
        if timeout is None:
            timeout = self.BACKGROUND_PROCESSING_TIMEOUT

        while (time.time() - start_time) < timeout:
            try:
                response = requests.get(webhook_endpoint)

                if response.status_code == 200:
                    webhook_data = response.json().get("data", {})
                    if webhook_data and webhook_data.get("processed") is True:
                        return webhook_data

                time.sleep(1)

                # Print progress occasionally
                elapsed = time.time() - start_time
                if elapsed % 30 < 1:  # Print every ~30 seconds
                    print(
                        f"Still waiting for webhook tracker... {int(elapsed)}s elapsed"
                    )

            except Exception as e:
                print(f"Error querying webhook tracker: {e}")
                time.sleep(1)

        raise TimeoutError(f"Webhook tracker not updated within {timeout} seconds")

    @pytest.fixture(autouse=True)
    def setup_easypost_mock(self, monkeypatch):
        """Setup EasyPost mock for all tests in this class."""
        # Mock the EasyPost tracker create method
        self.mock_tracker = EasyPostMock.mock_tracker_create(
            monkeypatch,
            mock_response_file="tests/integration/easypost/mock_create_tracker_response.json",
        )

        # Update the mock response with our test data
        self.mock_tracker.create.return_value.tracking_code = self.test_tracking_number
        self.mock_tracker.create.return_value.carrier = self.test_carrier

    def test_async_task_creation(self):
        """Test creation of async processing task - should FAIL initially."""
        print("\n=== TESTING ASYNC TASK CREATION ===")

        # This test should FAIL because we haven't implemented the async task yet
        try:
            # Try to import the async processing task that doesn't exist yet
            from blueprints.easypost import process_delivery_status_task

            # If we get here, the task exists (which means async implementation is done)
            print("✅ process_delivery_status_task found - async implementation exists")

            # Verify it's a proper Celery task
            assert hasattr(
                process_delivery_status_task, "delay"
            ), "Task should have delay method"
            assert hasattr(
                process_delivery_status_task, "apply_async"
            ), "Task should have apply_async method"
            assert hasattr(
                process_delivery_status_task, "name"
            ), "Task should have name attribute"

            print(f"✅ Task registered: {process_delivery_status_task.name}")

        except ImportError:
            pytest.fail(
                "process_delivery_status_task not found in blueprints.easypost. "
                "This test is expected to FAIL initially until async implementation "
                "is completed. The task should be implemented as a Celery task that "
                "processes delivery status updates in the background."
            )

    def test_celery_connection_and_workers(self):
        """Test that Celery connection is available and workers are running."""
        print("\n=== TESTING CELERY CONNECTION AND WORKERS ===")

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
                pytest.skip(
                    "No active Celery workers found - required for async processing tests"
                )

            print(f"✅ Active Celery workers: {list(active_workers.keys())}")

        except Exception as e:
            pytest.skip(f"Celery inspection failed: {e}")

        print("✅ Celery connection and workers verified")

    def test_delivery_status_immediate_response(self):
        """Test /delivery_status returns 202 immediately - should FAIL initially."""
        print(
            f"\n=== TESTING IMMEDIATE RESPONSE (target: <{self.IMMEDIATE_RESPONSE_TIMEOUT}s) ==="
        )

        # Check for Celery workers first
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Create a test lead and tracker first
        print("Creating test lead and tracker...")
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=self.test_last_name,
            email=self.test_email,
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": self.test_tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": self.test_carrier,
            },
            include_date_location=False,
        )
        self.test_data["lead_id"] = lead_data["id"]

        # Create tracker first (using synchronous endpoint for setup)
        tracker_payload = {
            "event": {
                "data": {
                    "id": lead_data["id"],
                    "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": self.test_tracking_number,
                    "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": self.test_carrier,
                }
            }
        }

        # Create tracker (this might be sync or async depending on implementation state)
        tracker_response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=tracker_payload,
            headers={"Content-Type": "application/json"},
        )

        if tracker_response.status_code not in [200, 202]:
            pytest.skip(
                f"Could not create tracker for test: {tracker_response.status_code}"
            )

        # Get tracker ID from response or lead
        tracker_id = None
        if tracker_response.status_code == 200:
            tracker_id = tracker_response.json().get("tracker_id")

        if not tracker_id:
            # Wait a bit and check lead for tracker ID
            time.sleep(5)
            updated_lead = self.close_api.get_lead(lead_data["id"])
            tracker_id = updated_lead.get(
                "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
            )

        if not tracker_id:
            pytest.skip("Could not get tracker ID for delivery status test")

        print(f"Using tracker ID: {tracker_id}")

        # Prepare delivery status webhook payload
        delivery_payload = {
            "id": f"evt_test_async_{self.timestamp}",
            "result": self.delivery_webhook_payload.copy(),
        }

        # Update the delivery webhook payload with the test data
        delivery_payload["result"]["id"] = tracker_id
        delivery_payload["result"]["tracking_code"] = self.test_tracking_number
        delivery_payload["result"]["carrier"] = self.test_carrier

        # Send delivery status webhook and measure response time
        print("Sending delivery status webhook...")
        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/easypost/delivery_status",
                json=delivery_payload,
                headers={"Content-Type": "application/json"},
                timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
            )

            response_time = time.time() - start_time
            print(f"Response received in {response_time:.2f}s")
            print(f"Status code: {response.status_code}")

            # For async implementation, we expect:
            # 1. Status code 202 (Accepted)
            # 2. Response should include celery_task_id for tracking
            # 3. Response time should be very fast (< 5 seconds)

            if response.status_code == 200:
                # This indicates synchronous processing - test should fail
                pytest.fail(
                    f"Endpoint returned 200 (synchronous processing) instead of 202 (async). "
                    f"Response time: {response_time:.2f}s. This test is expected to FAIL "
                    f"until async implementation is completed."
                )

            elif response.status_code == 202:
                # This indicates async processing - what we want
                response_data = response.json()
                print(f"Response data: {response_data}")

                # Check if response includes celery_task_id
                if "celery_task_id" in response_data:
                    task_id = response_data["celery_task_id"]
                    self.task_ids.append(task_id)
                    print(f"✅ Got celery_task_id: {task_id}")
                else:
                    pytest.fail(
                        "Response missing celery_task_id - indicates incomplete async implementation"
                    )

                # Verify response time is fast
                if response_time > self.IMMEDIATE_RESPONSE_TIMEOUT:
                    pytest.fail(
                        f"Response too slow: {response_time:.2f}s (expected <{self.IMMEDIATE_RESPONSE_TIMEOUT}s)"
                    )

                print(
                    "✅ Async endpoint responding correctly with immediate 202 response"
                )

            else:
                pytest.fail(f"Unexpected status code: {response.status_code}")

        except requests.exceptions.Timeout:
            response_time = time.time() - start_time
            pytest.fail(
                f"Request timed out after {response_time:.2f}s. This indicates the endpoint "
                f"is still processing synchronously. Expected: immediate 202 response with "
                f"celery_task_id for async processing."
            )

    def test_background_task_completion(self):
        """Test that background Celery task completes successfully."""
        print("\n=== TESTING BACKGROUND TASK COMPLETION ===")
        # Check for Celery workers
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
            print(f"Active Celery workers: {list(active_workers.keys())}")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Create test lead and tracker
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=self.test_last_name,
            email=self.test_email,
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": self.test_tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": self.test_carrier,
            },
            include_date_location=False,
        )
        self.test_data["lead_id"] = lead_data["id"]

        # Create tracker first
        tracker_payload = {
            "event": {
                "data": {
                    "id": lead_data["id"],
                    "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": self.test_tracking_number,
                    "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": self.test_carrier,
                }
            }
        }

        tracker_response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=tracker_payload,
            headers={"Content-Type": "application/json"},
        )

        # Get tracker ID
        tracker_id = None
        if tracker_response.status_code == 200:
            tracker_id = tracker_response.json().get("tracker_id")
        elif tracker_response.status_code == 202:
            # Wait for async tracker creation to complete
            time.sleep(10)
            updated_lead = self.close_api.get_lead(lead_data["id"])
            tracker_id = updated_lead.get(
                "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
            )

        if not tracker_id:
            pytest.skip("Could not create tracker for delivery status test")

        print(f"Using tracker ID: {tracker_id}")

        # Send async delivery status request
        delivery_payload = {
            "id": f"evt_test_async_{self.timestamp}",
            "result": self.delivery_webhook_payload.copy(),
        }

        delivery_payload["result"]["id"] = tracker_id
        delivery_payload["result"]["tracking_code"] = self.test_tracking_number
        delivery_payload["result"]["carrier"] = self.test_carrier

        response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=delivery_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        assert response.status_code == 202, f"Expected 202, got {response.status_code}"
        response_data = response.json()
        task_id = response_data.get("celery_task_id")
        assert task_id, "Response should include celery_task_id"

        self.task_ids.append(task_id)
        print(f"Task queued with ID: {task_id}")

        # Wait for background task to complete
        print("Waiting for background task to complete...")
        task_result = self.wait_for_async_task_completion(task_id)

        assert (
            task_result["status"] == "success"
        ), f"Task failed: {task_result.get('error')}"
        print("✅ Background task completed successfully")

        # Verify the lead was updated with delivery information
        print("Verifying lead was updated with delivery information...")
        updated_lead = self.close_api.get_lead(lead_data["id"])

        # Check that package_delivered field is set to "Yes"
        assert (
            updated_lead.get("custom.cf_wkZ5ptOR1Ro3YPxJPYipI35M7ticuYvJHFgp2y4fzdQ")
            == "Yes"
        ), "Lead should be updated with package_delivered=Yes"

        # Check that delivery city and state were updated
        assert (
            updated_lead.get("custom.cf_1hWUFxiA6QhUXrYT3lDh96JSWKxVBBAKCB3XO8EXGUW")
            is not None
        ), "Lead should be updated with delivery city"

        assert (
            updated_lead.get("custom.cf_vxfsYfTrFk6oYrnSx0ViYrUMpE7y5sxi0NnRgTyOf30")
            is not None
        ), "Lead should be updated with delivery state"

        print("✅ Lead updated with delivery information")

        # Verify webhook tracker was updated
        print("Verifying webhook tracker was updated...")
        webhook_data = self.wait_for_webhook_tracker_update(tracker_id=tracker_id)

        assert (
            webhook_data.get("processed") is True
        ), "Webhook should be marked as processed"
        assert (
            webhook_data.get("result") == "Success"
        ), f"Webhook processing should succeed: {webhook_data.get('error')}"
        print("✅ Webhook tracker updated successfully")

    def test_async_duplicate_delivery_prevention(self):
        """Test that duplicate delivery webhooks don't create duplicate activities in async mode."""
        print("\n=== TESTING ASYNC DUPLICATE DELIVERY PREVENTION ===")
        # Check for Celery workers
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Use a different tracking number to avoid conflicts
        duplicate_test_tracking_number = "EZ4000000004"
        duplicate_test_carrier = "USPS"

        # Create test lead and tracker
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=f"{self.test_last_name}Duplicate",
            email=f"lance+duplicate.async.{self.timestamp}@whiteboardgeeks.com",
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": duplicate_test_tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": duplicate_test_carrier,
            },
            include_date_location=False,
        )
        duplicate_lead_id = lead_data["id"]

        # Create tracker first
        tracker_payload = {
            "event": {
                "data": {
                    "id": duplicate_lead_id,
                    "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": duplicate_test_tracking_number,
                    "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": duplicate_test_carrier,
                }
            }
        }

        tracker_response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=tracker_payload,
            headers={"Content-Type": "application/json"},
        )

        # Get tracker ID
        tracker_id = None
        if tracker_response.status_code == 200:
            tracker_id = tracker_response.json().get("tracker_id")
        elif tracker_response.status_code == 202:
            # Wait for async tracker creation
            time.sleep(10)
            updated_lead = self.close_api.get_lead(duplicate_lead_id)
            tracker_id = updated_lead.get(
                "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
            )

        if not tracker_id:
            pytest.skip("Could not create tracker for duplicate test")

        print(f"Using tracker ID: {tracker_id}")

        # Prepare delivery payload
        delivery_payload = {
            "id": f"evt_test_duplicate1_{self.timestamp}",
            "result": self.delivery_webhook_payload.copy(),
        }

        delivery_payload["result"]["id"] = tracker_id
        delivery_payload["result"]["tracking_code"] = duplicate_test_tracking_number
        delivery_payload["result"]["carrier"] = duplicate_test_carrier

        # Send FIRST delivery webhook
        print("Sending FIRST async delivery status webhook...")
        first_response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=delivery_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        assert (
            first_response.status_code == 202
        ), f"Expected 202, got {first_response.status_code}"
        first_task_id = first_response.json().get("celery_task_id")
        assert first_task_id, "First response should include celery_task_id"
        self.task_ids.append(first_task_id)

        # Wait for first task to complete
        print("Waiting for first delivery task to complete...")
        first_result = self.wait_for_async_task_completion(first_task_id)
        assert (
            first_result["status"] == "success"
        ), f"First task failed: {first_result.get('error')}"

        # Check custom activities after first webhook - should be exactly 1
        mailer_delivered_activity_type = "custom.actitype_3KhBfWgjtVfiGYbczbgOWv"
        activities_after_first = self.close_api.get_lead_custom_activities(
            duplicate_lead_id, mailer_delivered_activity_type
        )

        print(f"Custom activities after first webhook: {len(activities_after_first)}")
        assert (
            len(activities_after_first) == 1
        ), f"Expected exactly 1 custom activity after first webhook, but found {len(activities_after_first)}"

        # Send SECOND delivery webhook (duplicate)
        print("Sending SECOND (duplicate) async delivery status webhook...")
        delivery_payload["id"] = f"evt_test_duplicate2_{self.timestamp}"

        second_response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=delivery_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        assert (
            second_response.status_code == 202
        ), f"Expected 202, got {second_response.status_code}"
        second_task_id = second_response.json().get("celery_task_id")
        assert second_task_id, "Second response should include celery_task_id"
        self.task_ids.append(second_task_id)

        # Wait for second task to complete
        print("Waiting for second delivery task to complete...")
        second_result = self.wait_for_async_task_completion(second_task_id)
        assert (
            second_result["status"] == "success"
        ), f"Second task failed: {second_result.get('error')}"

        # Check custom activities after second webhook - should STILL be exactly 1
        activities_after_second = self.close_api.get_lead_custom_activities(
            duplicate_lead_id, mailer_delivered_activity_type
        )

        print(f"Custom activities after second webhook: {len(activities_after_second)}")
        assert (
            len(activities_after_second) == 1
        ), f"Expected exactly 1 custom activity after second webhook (no duplicate), but found {len(activities_after_second)}"

        print("✅ Async duplicate delivery prevention working correctly!")

        # Cleanup
        self.close_api.delete_lead(duplicate_lead_id)

    def test_async_error_handling(self):
        """Test error handling in async delivery status processing."""
        print("\n=== TESTING ASYNC ERROR HANDLING ===")
        # Check for Celery workers
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Test with invalid payload (missing result)
        print("Testing with missing result...")
        invalid_payload = {"id": "evt_invalid"}

        response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=invalid_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        # Should still respond quickly even with errors
        assert response.status_code in [
            200,
            202,
            400,
        ], f"Unexpected status code: {response.status_code}"
        print(f"✅ Invalid payload handled gracefully: {response.status_code}")

        # Test with non-delivered status
        print("Testing with non-delivered status...")
        non_delivered_payload = {
            "id": f"evt_test_non_delivered_{self.timestamp}",
            "result": self.delivery_webhook_payload.copy(),
        }

        # Modify status to be non-delivered
        non_delivered_payload["result"]["status"] = "in_transit"
        non_delivered_payload["result"]["tracking_code"] = "EZ9999999999"

        response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=non_delivered_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        # Should handle gracefully (non-delivered packages should be processed but not update leads)
        assert response.status_code in [
            200,
            202,
        ], "Should handle non-delivered status gracefully"
        print(f"✅ Non-delivered status handled gracefully: {response.status_code}")

        print("✅ Error handling tests completed")
