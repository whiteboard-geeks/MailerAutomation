import os
import json
import requests
from tests.utils.close_api import CloseAPI
from datetime import datetime
from time import sleep


class TestInstantlyReplyReceivedIntegration:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Check if Gmail credentials are available by querying the Flask server
        try:
            env_response = requests.get(f"{self.base_url}/debug/env")
            if env_response.status_code == 200:
                env_data = env_response.json()
                self.gmail_configured = "Found" in env_data.get(
                    "gmail_service_account_info", ""
                )
                print(
                    f"\nGmail configuration status from server: {self.gmail_configured}"
                )
                print(
                    f"Gmail info from server: {env_data.get('gmail_service_account_info')}"
                )
            else:
                print(
                    f"\nCould not check Gmail configuration - /debug/env returned {env_response.status_code}"
                )
                self.gmail_configured = False
        except Exception as e:
            print(f"\nError checking Gmail configuration: {str(e)}")
            self.gmail_configured = False

        if not self.gmail_configured:
            print(
                "\nWARNING: Gmail service account credentials not found in environment. Test will fail."
            )

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

        # Strictly require Gmail credentials
        assert self.gmail_configured, "Gmail service account credentials are not configured in the environment. This test requires proper Gmail configuration."

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

        # Get the first contact from the lead
        lead_details = self.close_api.get_lead(lead_data["id"])
        contacts = lead_details.get("contacts", [])
        assert len(contacts) > 0, "No contacts found on the lead"
        contact = contacts[0]
        contact_id = contact["id"]

        # Subscribe the contact to a test sequence
        print(f"Subscribing contact {contact_id} to test sequence...")
        subscription = self.close_api.subscribe_contact_to_sequence(
            contact_id=contact_id, sequence_id="seq_5cIemWAjO0ln2WacqpMs6S"
        )
        subscription_id = subscription["id"]
        print(f"Contact subscribed to sequence with subscription ID: {subscription_id}")

        # Verify the subscription is active
        assert subscription["status"] == "active", "Sequence subscription is not active"

        print("Waiting 10 seconds for Close to populate lead data for search...")
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

        # Verify email activity
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

        # Verify the webhook response indicates success
        print("Checking webhook response for successful processing...")
        assert (
            response.status_code == 200
        ), f"Webhook response status code is not 200, got {response.status_code}"
        response_data = response.json()
        assert (
            response_data.get("status") == "success"
        ), f"Webhook response status is not 'success', got {response_data.get('status')}"
        assert (
            response_data.get("message")
            == "Reply received webhook processed successfully"
        ), "Webhook response message doesn't indicate success"

        # Verify email notification was sent successfully
        print("Checking if notification email was sent successfully...")
        notification_status = response_data.get("data", {}).get("notification_status")
        print(f"Notification status: {notification_status}")

        # Strict check that will fail the test if email sending fails
        assert notification_status in [
            "success",
            "success_mailgun",
        ], f"Email notification failed with status: {notification_status}"

        # Verify 'task_id' is None in the response (since we don't create tasks anymore)
        print("Verifying no task was created...")
        assert (
            response_data.get("data", {}).get("task_id") is None
        ), "Task ID should be None in the response"

        # Check if the sequence subscription was paused
        print("Checking if sequence subscription was paused...")

        # Give some time for the pause operation to complete
        sleep(3)

        # Get the updated subscription status
        updated_subscription = self.close_api.check_subscription_status(subscription_id)

        print(f"Updated subscription status: {updated_subscription.get('status')}")
        assert (
            updated_subscription.get("status") == "paused"
        ), "Sequence subscription was not paused"

        # Verify the paused subscription is included in the response
        paused_subscriptions = response_data.get("data", {}).get(
            "paused_subscriptions", []
        )
        assert (
            len(paused_subscriptions) > 0
        ), "No paused subscriptions reported in response"

        subscription_ids = [sub.get("subscription_id") for sub in paused_subscriptions]
        assert (
            subscription_id in subscription_ids
        ), f"Subscription ID {subscription_id} not found in response"

        print("All assertions passed!")
