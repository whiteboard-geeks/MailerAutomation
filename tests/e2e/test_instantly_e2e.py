# tests/e2e/test_instantly_e2e.py
import pytest
import requests
import os
from tests.utils.close_api import CloseAPI


class TestInstantlyE2E:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

    def test_create_lead_and_task_workflow(self):
        """Test the full workflow from creating a lead to handling webhook."""
        # 1. Create a test lead in Close
        lead_data = self.close_api.create_test_lead()
        self.test_data["lead_id"] = lead_data["id"]

        # 2. Create a task with Instantly campaign name
        campaign_name = "Test20250227"
        task_data = self.close_api.create_task_for_lead(lead_data["id"], campaign_name)

        # 3. Wait for the webhook to be processed
        # In a real test, we'd need to either:
        # - Mock the webhook call
        # - Have Close actually call our webhook (needs public endpoint)
        # - Have our test simulate the webhook call

        # For this example, we'll simulate the webhook call
        webhook_data = {
            "event": {
                "action": "created",
                "data": {
                    "lead_id": lead_data["id"],
                    "text": f"Instantly: {campaign_name}",
                    # Include other relevant task data
                },
            }
        }

        # Call our webhook endpoint directly
        response = requests.post(
            "http://localhost:5000/close_webhook",  # Adjust URL for your actual endpoint
            json=webhook_data,
            headers={"X-API-KEY": os.environ.get("WEBHOOK_API_KEY")},
        )

        # 4. Assert the response
        assert response.status_code == 200
        assert response.json()["status"] == "success"

        # 5. Verify the lead was properly processed
        # This would depend on what your webhook actually does
        # For example, check if a lead was created in Instantly
