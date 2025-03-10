import os
import json
import requests
from tests.utils.close_api import CloseAPI


class TestInstantlyEmailSentIntegration:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Load the mock webhook payload
        with open("instantly_email_sent_payload.json", "r") as f:
            self.mock_payload = json.load(f)

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

        # Delete the task if it was created
        if self.test_data.get("task_id"):
            self.close_api.delete_task(self.test_data["task_id"])

        # Delete the email activity if it was created
        if self.test_data.get("email_id"):
            self.close_api.delete_email_activity(self.test_data["email_id"])

    def test_instantly_email_sent_webhook(self):
        """Test handling of Instantly email sent webhook."""
        print("\n=== STARTING INTEGRATION TEST: Instantly Email Sent Webhook ===")

        # Create a test lead in Close with the email from the mock payload
        print("Creating test lead in Close...")
        lead_data = self.close_api.create_test_lead(
            email=self.mock_payload["lead_email"],
            first_name=self.mock_payload["firstName"],
            last_name=self.mock_payload["lastName"],
        )
        self.test_data["lead_id"] = lead_data["id"]
        print(f"Test lead created with ID: {lead_data['id']}")

        # Create a task with the campaign name from the mock payload
        campaign_name = self.mock_payload["campaign_name"]
        print(f"Creating task for lead with campaign name: {campaign_name}...")
        task_data = self.close_api.create_task_for_lead(lead_data["id"], campaign_name)
        self.test_data["task_id"] = task_data["id"]
        print(f"Task created with ID: {task_data['id']}")

        # Send the mock webhook to our endpoint
        print("Sending mock webhook to endpoint...")
        response = requests.post(
            f"{self.base_url}/instantly/email_sent",
            json=self.mock_payload,
        )
        print(f"Webhook response status: {response.status_code}")
        print(f"Webhook response: {response.json()}")

        # Verify the response
        assert response.status_code == 200, "Webhook endpoint returned non-200 status"
        response_data = response.json()
        assert response_data["status"] == "success", "Webhook processing failed"

        # Verify the task was marked as complete
        task = self.close_api.get_task(self.test_data["task_id"])
        assert task["status"] == "completed", "Task was not marked as complete"

        # Verify the email activity was created
        email_activities = self.close_api.get_lead_email_activities(lead_data["id"])
        assert len(email_activities) > 0, "No email activity was created"

        # Find the matching email activity
        matching_email = None
        for email in email_activities:
            if email["subject"] == self.mock_payload["email_subject"]:
                matching_email = email
                break

        assert matching_email is not None, "Matching email activity not found"
        assert matching_email["status"] == "sent", "Email activity status is not 'sent'"
        assert (
            matching_email["body_html"] == self.mock_payload["email_html"]
        ), "Email body doesn't match"

        # Store the email ID for cleanup
        self.test_data["email_id"] = matching_email["id"]

        print("All assertions passed!")
