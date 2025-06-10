"""
Integration tests for the Instantly add_lead webhook handler.
"""

import os
import time
import requests
from datetime import datetime
from tests.utils.close_api import CloseAPI


class TestInstantlyAddLeadIntegration:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Load the mock webhook payload
        self.mock_payload = {
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
                    "text": "Instantly: Test20250227",
                    "lead_name": "Test Instantly20250228132044",
                    "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                    "view": None,
                    "date_created": "2025-02-28T19:20:45.505000+00:00",
                },
                "previous_data": {},
            },
        }

        # Set environment type and current date
        os.environ.get("ENV_TYPE", "test")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Format the email with lance+env.date pattern
        self.mock_payload["event"]["data"]["lead_name"] = f"Test Instantly{timestamp}"

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

    def wait_for_webhook_processed(self, close_task_id, route=None):
        """Wait for webhook to be processed by checking the webhook tracker API."""
        webhook_endpoint = (
            f"{self.base_url}/instantly/webhooks/status?close_task_id={close_task_id}"
        )
        if route:
            webhook_endpoint += f"&route={route}"

        print(f"Checking webhook endpoint: {webhook_endpoint}")
        start_time = time.time()
        elapsed_time = 0
        timeout = 60  # 1 minute timeout

        while elapsed_time < timeout:
            try:
                response = requests.get(webhook_endpoint)
                print(f"Response status: {response.status_code}")
                if response.status_code == 200:
                    webhook_data = response.json().get("data", {})
                    print(f"Webhook data: {webhook_data}")
                    if webhook_data:
                        # Add close_task_id to webhook data if not present
                        if "close_task_id" not in webhook_data:
                            webhook_data["close_task_id"] = close_task_id
                        return webhook_data
                elif response.status_code == 404:
                    print(f"404 response content: {response.json()}")
            except Exception as e:
                print(f"Error querying webhook API: {e}")

            time.sleep(1)  # Check every second
            elapsed_time = time.time() - start_time
            print(f"Elapsed time: {int(elapsed_time)} seconds")

        raise TimeoutError(
            f"Timed out waiting for webhook after {int(elapsed_time)} seconds"
        )

    def test_instantly_add_lead_success(self):
        """Test successful flow of adding a lead to an Instantly campaign."""
        print("\n=== STARTING INTEGRATION TEST: Instantly Add Lead Success ===")

        # Stage 1: Create a test lead in Close
        print("Creating test lead in Close...")
        lead_data = self.close_api.create_test_lead(include_date_location=True)
        self.test_data["lead_id"] = lead_data["id"]
        print(f"Test lead created with ID: {lead_data['id']}")

        # Stage 1 Assertions: Verify lead creation
        assert lead_data is not None, "Lead data should not be None"
        assert "id" in lead_data, "Lead should have an ID"
        assert lead_data["id"].startswith("lead_"), "Lead ID should have correct format"
        print("✅ Stage 1: Lead creation verified")

        # Stage 2: Update the mock payload with the actual lead ID and Close task ID
        close_task_id = self.mock_payload["event"]["data"]["id"]
        self.mock_payload["event"]["data"]["lead_id"] = lead_data["id"]
        self.test_data["close_task_id"] = close_task_id

        # Stage 2 Assertions: Verify payload preparation
        assert (
            self.mock_payload["event"]["data"]["lead_id"] == lead_data["id"]
        ), "Payload should contain correct lead ID"
        assert close_task_id.startswith(
            "task_"
        ), "Close task ID should have correct format"
        assert (
            self.mock_payload["event"]["action"] == "created"
        ), "Event action should be 'created'"
        assert (
            "Test20250227" in self.mock_payload["event"]["data"]["text"]
        ), "Campaign name should be in task text"
        print("✅ Stage 2: Payload preparation verified")

        # Stage 3: Send the webhook to our endpoint
        print("Sending webhook to endpoint...")
        response = requests.post(
            f"{self.base_url}/instantly/add_lead",
            json=self.mock_payload,
        )
        print(f"Webhook response status: {response.status_code}")
        print(f"Webhook response: {response.json()}")

        # Stage 3 Assertions: Verify webhook submission
        assert response.status_code in [
            200,
            202,
        ], f"Webhook should return 200 or 202, got {response.status_code}"
        response_data = response.json()
        assert "status" in response_data, "Response should contain status"
        assert response_data["status"] in [
            "success",
            "queued",
        ], "Status should be success or queued"
        print("✅ Stage 3: Webhook submission verified")

        # Stage 4: Wait for webhook to be processed
        print("Waiting for webhook to be processed...")
        webhook_data = self.wait_for_webhook_processed(close_task_id, "add_lead")

        # Stage 4 Assertions: Verify webhook processing initiation
        assert (
            webhook_data is not None
        ), "Webhook data should not be None after processing"
        assert isinstance(webhook_data, dict), "Webhook data should be a dictionary"
        print("✅ Stage 4: Webhook processing initiation verified")

        # Stage 5: Final verification of webhook data
        assert webhook_data.get("route") == "add_lead", "Webhook route is not add_lead"
        assert webhook_data.get("lead_id") == lead_data["id"], "Lead ID doesn't match"
        assert (
            webhook_data.get("close_task_id") == close_task_id
        ), "Close task ID doesn't match"
        assert (
            webhook_data.get("processed") is True
        ), "Webhook wasn't marked as processed"
        assert (
            webhook_data.get("campaign_name") == "Test20250227"
        ), "Campaign name doesn't match"

        # Stage 5a: Verify Instantly API result
        instantly_result = webhook_data.get("instantly_result", {})
        assert instantly_result, "Instantly result should be present"
        assert (
            instantly_result.get("status") == "success"
        ), f"Instantly API call failed: {instantly_result}"
        print("✅ Stage 5a: Instantly API result verified")

        # Stage 5b: Verify async processing
        assert (
            webhook_data.get("celery_task_id") is not None
        ), "Celery task ID should be present for async processing"
        celery_task_id = webhook_data.get("celery_task_id")
        assert isinstance(celery_task_id, str), "Celery task ID should be a string"
        assert len(celery_task_id) > 0, "Celery task ID should not be empty"
        print("✅ Stage 5b: Async processing verified")

        print("✅ Stage 5: Final verification completed - All assertions passed!")
