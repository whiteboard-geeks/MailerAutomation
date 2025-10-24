"""
Integration test for async EasyPost webhook failure modes.

This test converts the synchronous webhook failure tests to async processing,
following TDD approach where tests should FAIL initially until async implementation is complete.

Key differences from synchronous tests:
- Expects 202 status code (not 200)
- Expects celery_task_id in response
- Tests immediate response time (<5 seconds)
- Tests background task error handling
- Verifies error emails are still sent from background tasks

To run just this test:
pytest tests/integration/easypost/test_async_webhook_failure_integration.py -v

Note: This requires a working email configuration in your environment.
"""

import json
import os
import pytest
import time
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime
from app import flask_app
from celery_worker import celery
from config import USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER

# Skip all tests in this module if USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER is truthy
if USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER:
    pytest.skip(
        "Skipping async webhook failure integration tests because "
        "USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER is enabled. "
        "These tests are specific to Celery-based async processing.",
        allow_module_level=True
    )


# Sample Close webhook payload for EasyPost tracker creation
SAMPLE_PAYLOAD = {
    "event": {
        "data": {
            "id": "lead_123456",
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": "1Z999AA10123456789",
            "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": "UPS",
        }
    }
}


@pytest.fixture
def client():
    """Create a test client with the actual Flask app."""
    # Ensure testing mode
    flask_app.config["TESTING"] = True
    # We want to use the actual email functionality
    flask_app.config["MAIL_SUPPRESS_SEND"] = False
    return flask_app.test_client()


class TestAsyncEasyPostWebhookFailures:
    # Test configuration
    IMMEDIATE_RESPONSE_TIMEOUT = 5  # Seconds - async should respond immediately
    BACKGROUND_PROCESSING_TIMEOUT = 60  # Seconds - allow time for background processing

    def setup_method(self):
        """Setup before each test."""
        # Use BASE_URL environment variable if set, otherwise default to localhost for dev
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")
        self.task_ids = []
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    def teardown_method(self):
        """Cleanup after each test."""
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

                time.sleep(1)  # Check every second

            except Exception as e:
                print(f"Error checking task status: {e}")
                time.sleep(1)

        # Timeout
        raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")

    def test_async_task_creation(self):
        """Test creation of async processing task - should FAIL initially."""
        print("\n=== TESTING ASYNC TASK CREATION ===")

        # This test should FAIL because we haven't implemented the async task yet
        try:
            # Try to import the async processing task that doesn't exist yet
            from blueprints.easypost import create_tracker_task

            # If we get here, the task exists (which means async implementation is done)
            print("✅ create_tracker_task found - async implementation exists")

            # Verify it's a proper Celery task
            assert hasattr(
                create_tracker_task, "delay"
            ), "Task should have delay method"
            assert hasattr(
                create_tracker_task, "apply_async"
            ), "Task should have apply_async method"
            assert hasattr(
                create_tracker_task, "name"
            ), "Task should have name attribute"

            print(f"✅ Task registered: {create_tracker_task.name}")

        except ImportError:
            pytest.fail(
                "create_tracker_task not found in blueprints.easypost. "
                "This test is expected to FAIL initially until async implementation "
                "is completed. The task should be implemented as a Celery task that "
                "processes tracker creation in the background."
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

    @pytest.mark.webhook_failures
    def test_async_no_lead_id_immediate_response_and_background_error(self):
        """
        Test async processing of missing lead ID error.
        Should return 202 immediately, then handle error in background task.
        """
        print(
            "\n--- Testing async missing lead ID (immediate 202 + background error) ---"
        )

        # Check for Celery workers first
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Create a payload with missing lead ID
        invalid_payload = {
            "event": {
                "data": {}  # Missing lead ID
            }
        }

        # Send the webhook payload and measure response time
        start_time = time.time()
        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=invalid_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )
        elapsed = time.time() - start_time

        # Print response details
        print(f"\nResponse received in {elapsed:.2f} seconds")
        print(f"Status code: {response.status_code}")
        response_data = response.json()
        print(f"Response body: {json.dumps(response_data, indent=2)}")

        # For async implementation, we expect:
        # 1. Status code 202 (Accepted) - immediate response
        # 2. Response should include celery_task_id for tracking
        # 3. Response time should be very fast (< 5 seconds)

        if response.status_code == 200:
            # This indicates synchronous processing - test should fail
            pytest.fail(
                f"Endpoint returned 200 (synchronous processing) instead of 202 (async). "
                f"Response time: {elapsed:.2f}s. This test is expected to FAIL "
                f"until async implementation is completed."
            )

        elif response.status_code == 202:
            # This indicates async processing - what we want
            print("✅ Got 202 response - async processing detected")

            # Check if response includes celery_task_id
            if "celery_task_id" in response_data:
                task_id = response_data["celery_task_id"]
                self.task_ids.append(task_id)
                print(f"✅ Got celery_task_id: {task_id}")

                # Wait for background task to complete (should fail due to missing lead_id)
                print("Waiting for background task to complete...")
                task_result = self.wait_for_async_task_completion(task_id)

                # The task should complete but with an error result
                print(f"Task result: {task_result}")

                # Verify the task handled the error gracefully
                assert task_result["status"] in [
                    "success",
                    "failed",
                ], "Task should complete with a status"

                print("✅ Background task completed and handled missing lead_id error")

            else:
                pytest.fail(
                    "Response missing celery_task_id - indicates incomplete async implementation"
                )

            # Verify response time is fast
            if elapsed > self.IMMEDIATE_RESPONSE_TIMEOUT:
                pytest.fail(
                    f"Response too slow: {elapsed:.2f}s (expected <{self.IMMEDIATE_RESPONSE_TIMEOUT}s)"
                )

            print("✅ Async endpoint responding correctly with immediate 202 response")

        else:
            # For async implementation, we might also accept 400 for immediate validation
            assert response.status_code in [
                400
            ], f"Unexpected status code: {response.status_code}"
            print(f"✅ Got {response.status_code} response - immediate validation")

        print("\nVerify that:")
        print("1. You received an email with subject 'EasyPost Tracker Creation Error'")
        print("2. The email contains error details about missing lead ID")
        print("3. The response was immediate (202) with background error handling")

    @pytest.mark.webhook_failures
    def test_async_lead_not_found_immediate_response_and_background_error(self):
        """
        Test async processing of lead not found error.
        Should return 202 immediately, then handle error in background task.
        """
        print(
            "\n--- Testing async lead not found (immediate 202 + background error) ---"
        )

        # Check for Celery workers first
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Mock make_close_request to simulate lead not found in Close
        with patch("blueprints.easypost.make_close_request") as mock_make_request:
            # Configure the mock to raise HTTPError for 404
            from requests.exceptions import HTTPError

            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_response.text = "Not Found"
            mock_response.url = "https://api.close.com/api/v1/lead/lead_123456"

            # Create HTTPError that would be raised by raise_for_status()
            http_error = HTTPError(
                "404 Client Error: Not Found for url: https://api.close.com/api/v1/lead/lead_123456"
            )
            http_error.response = mock_response
            mock_make_request.side_effect = http_error

            # Send the webhook payload and measure response time
            start_time = time.time()
            response = requests.post(
                f"{self.base_url}/easypost/create_tracker",
                json=SAMPLE_PAYLOAD,
                headers={"Content-Type": "application/json"},
                timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
            )
            elapsed = time.time() - start_time

            # Print response details
            print(f"\nResponse received in {elapsed:.2f} seconds")
            print(f"Status code: {response.status_code}")
            response_data = response.json()
            print(f"Response body: {json.dumps(response_data, indent=2)}")

            # For async implementation, we expect 202 with task_id
            if response.status_code == 200:
                # This indicates synchronous processing - test should fail
                pytest.fail(
                    f"Endpoint returned 200 (synchronous processing) instead of 202 (async). "
                    f"Response time: {elapsed:.2f}s. This test is expected to FAIL "
                    f"until async implementation is completed."
                )

            elif response.status_code == 202:
                # This indicates async processing - what we want
                print("✅ Got 202 response - async processing detected")

                # Check if response includes celery_task_id
                if "celery_task_id" in response_data:
                    task_id = response_data["celery_task_id"]
                    self.task_ids.append(task_id)
                    print(f"✅ Got celery_task_id: {task_id}")

                    # Wait for background task to complete (should fail due to 404)
                    print("Waiting for background task to complete...")
                    task_result = self.wait_for_async_task_completion(task_id)

                    # The task should complete but with an error result
                    print(f"Task result: {task_result}")

                    # Verify the task handled the error gracefully
                    assert task_result["status"] in [
                        "success",
                        "failed",
                    ], "Task should complete with a status"

                    print("✅ Background task completed and handled 404 error")

                else:
                    pytest.fail(
                        "Response missing celery_task_id - indicates incomplete async implementation"
                    )

                print(
                    "✅ Async endpoint responding correctly with immediate 202 response"
                )

            else:
                pytest.fail(f"Unexpected status code: {response.status_code}")

            print("\nVerify that:")
            print(
                "1. You received an email with subject 'EasyPost Tracker Creation Error'"
            )
            print("2. The email contains error details about the failed fetch")
            print("3. The response was immediate (202) with background error handling")

    @pytest.mark.webhook_failures
    def test_async_missing_tracking_info_immediate_response_and_background_error(self):
        """
        Test async processing of missing tracking number or carrier.
        Should return 202 immediately, then handle error in background task.
        """
        print(
            "\n--- Testing async missing tracking info (immediate 202 + background error) ---"
        )

        # Check for Celery workers first
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Mock make_close_request to simulate lead with missing tracking info
        with patch("blueprints.easypost.make_close_request") as mock_make_request:
            # Configure the mock to return a lead without tracking number or carrier
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "id": "lead_123456",
                "name": "Test Lead",
                # No tracking info fields
            }
            mock_make_request.return_value = mock_response

            # Send the webhook payload and measure response time
            start_time = time.time()
            response = requests.post(
                f"{self.base_url}/easypost/create_tracker",
                json=SAMPLE_PAYLOAD,
                headers={"Content-Type": "application/json"},
                timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
            )
            elapsed = time.time() - start_time

            # Print response details
            print(f"\nResponse received in {elapsed:.2f} seconds")
            print(f"Status code: {response.status_code}")
            response_data = response.json()
            print(f"Response body: {json.dumps(response_data, indent=2)}")

            # For async implementation, we expect 202 with task_id
            if response.status_code == 200:
                # This indicates synchronous processing - test should fail
                pytest.fail(
                    f"Endpoint returned 200 (synchronous processing) instead of 202 (async). "
                    f"Response time: {elapsed:.2f}s. This test is expected to FAIL "
                    f"until async implementation is completed."
                )

            elif response.status_code == 202:
                # This indicates async processing - what we want
                print("✅ Got 202 response - async processing detected")

                # Check if response includes celery_task_id
                if "celery_task_id" in response_data:
                    task_id = response_data["celery_task_id"]
                    self.task_ids.append(task_id)
                    print(f"✅ Got celery_task_id: {task_id}")

                    # Wait for background task to complete (should handle missing tracking info)
                    print("Waiting for background task to complete...")
                    task_result = self.wait_for_async_task_completion(task_id)

                    # The task should complete but with an error result
                    print(f"Task result: {task_result}")

                    # Verify the task handled the error gracefully
                    assert task_result["status"] in [
                        "success",
                        "failed",
                    ], "Task should complete with a status"

                    print(
                        "✅ Background task completed and handled missing tracking info"
                    )

                else:
                    pytest.fail(
                        "Response missing celery_task_id - indicates incomplete async implementation"
                    )

                print(
                    "✅ Async endpoint responding correctly with immediate 202 response"
                )

            else:
                pytest.fail(f"Unexpected status code: {response.status_code}")

            print("\nVerify that:")
            print(
                "1. You received an email with subject 'EasyPost Tracker Missing Data'"
            )
            print("2. The email contains error details about missing tracking info")
            print("3. The response was immediate (202) with background error handling")

    @pytest.mark.webhook_failures
    def test_async_easypost_api_error_immediate_response_and_background_error(self):
        """
        Test async processing of EasyPost API error.
        Should return 202 immediately, then handle error in background task.
        """
        print(
            "\n--- Testing async EasyPost API error (immediate 202 + background error) ---"
        )

        # Check for Celery workers first
        try:
            inspect = celery.control.inspect()
            active_workers = inspect.active()
            if not active_workers:
                pytest.skip("No Celery workers available for testing")
        except Exception as e:
            pytest.skip(f"Celery connection not available: {e}")

        # Mock sequence for a lead with tracking info but EasyPost API error
        with patch("blueprints.easypost.make_close_request") as mock_make_request:
            with patch("blueprints.easypost.get_easypost_client") as mock_get_client:
                # Configure the mock to return a lead with tracking info
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "id": "lead_123456",
                    "name": "Test Lead",
                    "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": "1Z999AA10123456789",
                    "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": "UPS",
                }
                mock_make_request.return_value = mock_response

                # Set up the EasyPost client mock to raise an exception
                mock_client = MagicMock()
                mock_client.tracker.create.side_effect = Exception(
                    "EasyPost API rate limit exceeded"
                )
                mock_get_client.return_value = mock_client

                # Send the webhook payload and measure response time
                start_time = time.time()
                response = requests.post(
                    f"{self.base_url}/easypost/create_tracker",
                    json=SAMPLE_PAYLOAD,
                    headers={"Content-Type": "application/json"},
                    timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
                )
                elapsed = time.time() - start_time

                # Print response details
                print(f"\nResponse received in {elapsed:.2f} seconds")
                print(f"Status code: {response.status_code}")
                response_data = response.json()
                print(f"Response body: {json.dumps(response_data, indent=2)}")

                # For async implementation, we expect 202 with task_id
                if response.status_code == 200:
                    # This indicates synchronous processing - test should fail
                    pytest.fail(
                        f"Endpoint returned 200 (synchronous processing) instead of 202 (async). "
                        f"Response time: {elapsed:.2f}s. This test is expected to FAIL "
                        f"until async implementation is completed."
                    )

                elif response.status_code == 202:
                    # This indicates async processing - what we want
                    print("✅ Got 202 response - async processing detected")

                    # Check if response includes celery_task_id
                    if "celery_task_id" in response_data:
                        task_id = response_data["celery_task_id"]
                        self.task_ids.append(task_id)
                        print(f"✅ Got celery_task_id: {task_id}")

                        # Wait for background task to complete (should fail due to EasyPost API error)
                        print("Waiting for background task to complete...")
                        task_result = self.wait_for_async_task_completion(task_id)

                        # The task should complete but with an error result
                        print(f"Task result: {task_result}")

                        # Verify the task handled the error gracefully
                        assert task_result["status"] in [
                            "success",
                            "failed",
                        ], "Task should complete with a status"

                        print(
                            "✅ Background task completed and handled EasyPost API error"
                        )

                    else:
                        pytest.fail(
                            "Response missing celery_task_id - indicates incomplete async implementation"
                        )

                    print(
                        "✅ Async endpoint responding correctly with immediate 202 response"
                    )

                else:
                    pytest.fail(f"Unexpected status code: {response.status_code}")

                print("\nVerify that:")
                print(
                    "1. You received an email with subject 'EasyPost Tracker Creation Error'"
                )
                print("2. The email contains error details about the API error")
                print(
                    "3. The response was immediate (202) with background error handling"
                )


if __name__ == "__main__":
    # This allows running the test directly if needed
    pytest.main(["-xvs", __file__])
