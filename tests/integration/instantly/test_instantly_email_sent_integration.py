import os
import json
from time import sleep
import requests
from tests.utils.close_api import CloseAPI
from datetime import datetime


class TestInstantlyEmailSentIntegration:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Load the mock webhook payload
        with open(
            "tests/integration/instantly/instantly_email_sent_payload.json", "r"
        ) as f:
            self.mock_payload = json.load(f)

        # Set environment type and current date
        env_type = os.environ.get("ENV_TYPE", "test")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Format the email with lance+env.date pattern
        self.mock_payload["lead_email"] = (
            f"lance+{env_type}.instantly{timestamp}@whiteboardgeeks.com"
        )

        # Update name to match the date pattern
        self.mock_payload["lastName"] = f"Test{timestamp}"

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

    def test_instantly_email_sent_webhook(self):
        """Test handling of Instantly email sent webhook."""
        print("\n=== STARTING INTEGRATION TEST: Instantly Email Sent Webhook ===")

        # Create a test lead in Close with the email from the mock payload
        print("Creating test lead in Close...")
        lead_data = self.close_api.create_test_lead(
            email=self.mock_payload["lead_email"],
            first_name=self.mock_payload["firstName"],
            last_name=self.mock_payload["lastName"],
            include_date_location=True,
        )
        self.test_data["lead_id"] = lead_data["id"]
        print(f"Test lead created with ID: {lead_data['id']}")

        # Create a task with the campaign name from the mock payload
        campaign_name = self.mock_payload["campaign_name"]
        print(f"Creating task for lead with campaign name: {campaign_name}...")
        task_data = self.close_api.create_task_for_lead(lead_data["id"], campaign_name)
        self.test_data["task_id"] = task_data["id"]
        print(f"Task created with ID: {task_data['id']}")

        print("Waiting for Close to populate lead data for search...")
        self.close_api.wait_for_lead_by_email(self.mock_payload["lead_email"])

        # Send the mock webhook to our endpoint
        print("Sending mock webhook to endpoint...")
        response = requests.post(
            f"{self.base_url}/instantly/email_sent",
            json=self.mock_payload,
        )
        print(f"Webhook response status: {response.status_code}")
        print(f"Webhook response: {response.json()}")
        
        sleep(4)

        # Define verification functions with retries
        print("Checking if task is complete...")
        task = self.close_api.get_task(self.test_data["task_id"])
        assert task["is_complete"], "Task was not marked as complete"

        print("Checking for email activities...")
        email_activities = self.close_api.get_lead_email_activities(lead_data["id"])
        assert len(email_activities) > 0, "No email activity was created"

        print(f"Found {len(email_activities)} email activities")

        print(f"Looking for email with subject: {self.mock_payload['email_subject']}")

        for email in email_activities:
            if email["subject"] == self.mock_payload["email_subject"]:
                matching_email = email
                break

        assert matching_email is not None, "Matching email activity not found"

        print(f"Found matching email with ID: {matching_email['id']}")

        # Verify email properties
        assert matching_email["status"] == "sent", "Email activity status is not 'sent'"
        assert (
            matching_email["body_html"] == self.mock_payload["email_html"]
        ), "Email body doesn't match"

        print("All assertions passed!")

