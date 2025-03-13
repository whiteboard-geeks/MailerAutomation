import os
import json
import requests
from tests.utils.close_api import CloseAPI
from datetime import datetime
from time import sleep
from utils.instantly_constants import format_instantly_reply_task_text


class TestInstantlyReplyReceivedIntegration:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Load the mock webhook payload
        with open(
            "tests/integration/instantly/instantly_reply_received_payload.json", "r"
        ) as f:
            self.mock_payload = json.load(f)

        # Set environment type and current date
        env_type = os.environ.get("ENV_TYPE", "test")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Format the email with lance+env.date pattern
        email = f"lance+{env_type}.instantly{timestamp}@whiteboardgeeks.com"
        self.mock_payload["lead_email"] = email
        self.mock_payload["email"] = email

        # Update name to match the date pattern
        self.mock_payload["lastName"] = f"Test{timestamp}"

        # Update the timestamps
        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        self.mock_payload["timestamp"] = current_time

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

    def test_instantly_reply_received_webhook(self):
        """Test handling of Instantly reply received webhook."""
        print("\n=== STARTING INTEGRATION TEST: Instantly Reply Received Webhook ===")

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

        print("Waiting for Close to populate lead data for search...")
        sleep(10)

        # Send the mock webhook to our endpoint
        print("Sending mock webhook to endpoint...")
        response = requests.post(
            f"{self.base_url}/instantly/reply_received",
            json=self.mock_payload,
        )
        print(f"Webhook response status: {response.status_code}")
        print(f"Webhook response: {response.json()}")

        # Check for email activities
        print("Checking for email activities...")
        email_activities = self.close_api.get_lead_email_activities(lead_data["id"])
        assert len(email_activities) > 0, "No email activity was created"

        print(f"Found {len(email_activities)} email activities")

        print(f"Looking for email with subject: {self.mock_payload['reply_subject']}")

        matching_email = None
        for email in email_activities:
            if email["subject"] == self.mock_payload["reply_subject"]:
                matching_email = email
                break

        assert matching_email is not None, "Matching email activity not found"

        print(f"Found matching email with ID: {matching_email['id']}")

        # Verify email properties
        assert (
            matching_email["status"] == "inbox"
        ), "Email activity status is not 'inbox'"
        assert (
            matching_email["direction"] == "incoming"
        ), "Email direction is not 'incoming'"

        # Verify email content
        if self.mock_payload.get("reply_html"):
            assert (
                matching_email["body_html"] == self.mock_payload["reply_html"]
            ), "Email HTML body doesn't match"

        if self.mock_payload.get("reply_text"):
            assert (
                matching_email["body_text"] == self.mock_payload["reply_text"]
            ), "Email text body doesn't match"

        # Check for task creation
        print("Checking for task creation...")

        # Retrieve tasks for the lead
        tasks = self.close_api.get_lead_tasks(lead_data["id"])
        print(f"Found {len(tasks)} tasks for the lead")

        # Print all task texts for debugging
        print("Task texts:")
        for task in tasks:
            print(f"  - {task.get('text', '')}")

        # Check if there's a task with the expected text
        expected_task_text = format_instantly_reply_task_text(
            self.mock_payload["reply_subject"], self.mock_payload["campaign_name"]
        )
        print(f"Expected task text: {expected_task_text}")
        matching_task = None
        for task in tasks:
            task_text = task.get("text", "")
            # Use case-insensitive comparison to be more flexible
            if expected_task_text.lower() in task_text.lower():
                matching_task = task
                break

        assert matching_task is not None, "Task was not created"
        print(f"Found matching task with ID: {matching_task['id']}")

        # Verify task is not complete
        assert matching_task["is_complete"] is False, "Task should not be complete"

        print("All assertions passed!")
