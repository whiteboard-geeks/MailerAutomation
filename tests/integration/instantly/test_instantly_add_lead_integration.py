"""
Integration tests for the Instantly add_lead webhook handler.
"""

import os
import time
import requests
from datetime import datetime
from tests.utils.close_api import CloseAPI, Lead
from utils.instantly import search_campaigns_by_lead_email


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

        # Generate unique task ID for this test run
        import uuid

        unique_task_id = f"task_test_{uuid.uuid4().hex[:20]}"

        # Update the mock payload with unique identifiers
        self.mock_payload["event"]["object_id"] = unique_task_id
        self.mock_payload["event"]["data"]["id"] = unique_task_id
        self.mock_payload["event"]["data"]["lead_name"] = f"Test Instantly{timestamp}"

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

    def test_instantly_add_lead_success(self):
        instantly_campaign_name = "Test20250227"

        """Test successful flow of adding a lead to an Instantly campaign."""
        print("\n=== STARTING INTEGRATION TEST: Instantly Add Lead Success ===")

        # Stage 1: Create a test lead in Close
        print("Creating test lead in Close...")
        lead_data = self.close_api.create_test_lead(include_date_location=True)
        lead = Lead(**lead_data)
        self.test_data["lead_id"] = lead.id
        print(f"Test lead created with ID: {lead.id}")

        # Stage 1 Assertions: Verify lead creation
        assert lead is not None, "Lead data should not be None"
        assert lead.id is not None, "Lead should have an ID"
        assert lead.id.startswith("lead_"), "Lead ID should have correct format"
        print("✅ Stage 1: Lead creation verified")

        # Stage 2: Update the mock payload with the actual lead ID and Close task ID
        close_task_id = self.mock_payload["event"]["data"]["id"]
        self.mock_payload["event"]["data"]["lead_id"] = lead.id
        self.test_data["close_task_id"] = close_task_id

        # Stage 2 Assertions: Verify payload preparation
        assert (
            self.mock_payload["event"]["data"]["lead_id"] == lead.id
        ), "Payload should contain correct lead ID"
        assert close_task_id.startswith(
            "task_"
        ), "Close task ID should have correct format"
        assert (
            self.mock_payload["event"]["action"] == "created"
        ), "Event action should be 'created'"
        assert (
            instantly_campaign_name in self.mock_payload["event"]["data"]["text"]
        ), "Campaign name should be in task text"
        print("✅ Stage 2: Payload preparation verified")

        time.sleep(2)

        # Stage 2: Assert that the lead is not in any campaign in Instantly
        campaigns_before_add_lead = search_campaigns_by_lead_email(lead.contacts[0].emails[0].email)
        assert len(campaigns_before_add_lead) == 0, "Lead should not be in any campaign in Instantly"

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
        time.sleep(7)

        # Stage 4: Assert that the lead is in the campaign in Instantly
        campaigns_after_add_lead = search_campaigns_by_lead_email(lead.contacts[0].emails[0].email)
        assert len(campaigns_after_add_lead) == 1, "Lead should be in one campaign in Instantly"
        assert campaigns_after_add_lead[0].name == instantly_campaign_name, "Campaign name should match"
        print("✅ Stage 4: Lead added to Instantly campaign verified")
