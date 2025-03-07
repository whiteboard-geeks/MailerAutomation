import os
import time
import requests
from tests.utils.close_api import CloseAPI


class TestInstantlyE2E:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}

        # Always disable timeout - wait indefinitely
        self.webhook_timeout = None  # No timeout
        print("WEBHOOK TIMEOUT: NONE (waiting indefinitely)")

        self.webhook_check_interval = 1  # Check interval in seconds
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Save original ENV_TYPE value to restore later
        self.original_env_type = os.environ.get("ENV_TYPE")
        print(f"Original ENV_TYPE: {self.original_env_type}")

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the lead if it was created
        if self.test_data.get("lead_id"):
            result = self.close_api.delete_lead(self.test_data["lead_id"])
            if result == {}:  # Successful deletion returns empty dict
                print(f"Deleted lead with ID: {self.test_data['lead_id']}")
            else:
                print(f"Warning: Lead deletion may have failed: {result}")

        # Restore original ENV_TYPE if it was changed
        if self.original_env_type:
            os.environ["ENV_TYPE"] = self.original_env_type
        elif "ENV_TYPE" in os.environ:
            del os.environ["ENV_TYPE"]

    def wait_for_webhook_processed(self, task_id, route=None):
        """Wait for webhook to be processed by checking the webhook tracker API."""
        webhook_endpoint = (
            f"{self.base_url}/instantly/webhooks/status?task_id={task_id}"
        )
        if route:
            webhook_endpoint += f"&route={route}"

        start_time = time.time()
        elapsed_time = 0

        # Loop indefinitely since timeout is None
        while True:
            try:
                # Query the webhook tracker API
                response = requests.get(webhook_endpoint)

                if response.status_code == 200:
                    # We found webhook data
                    webhook_data = response.json().get("data", {})
                    if webhook_data:  # Make sure it's not empty
                        return webhook_data

                # If not found or empty, continue waiting
            except Exception as e:
                print(f"Error querying webhook API: {e}")

            # Sleep before trying again
            time.sleep(self.webhook_check_interval)

            # Print progress occasionally
            elapsed_time = time.time() - start_time
            if elapsed_time % 60 < 1:  # Print every ~60 seconds
                waiting_for = (
                    "email sent webhook"
                    if route == "email_sent"
                    else "add_lead webhook"
                )
                print(
                    f"Still waiting for {waiting_for}... {int(elapsed_time)}s elapsed"
                )

    def test_instantly_e2e(self):
        """Test the full workflow from creating a lead through email sending."""
        print("\n=== STARTING E2E TEST: Instantly Full Workflow ===")

        # Create a test lead in Close
        print("Creating test lead in Close...")
        lead_data = self.close_api.create_test_lead()
        self.test_data["lead_id"] = lead_data["id"]
        print(f"Test lead created with ID: {lead_data['id']}")

        # Create a webhook to send the task details to our endpoint.
        webhook_id = None
        try:
            print("Creating webhook in Close...")
            webhook_id = self.close_api.create_webhook_to_catch_task_created()
            self.test_data["webhook_id"] = webhook_id
            print(f"Webhook created with ID: {webhook_id}")

            # Create a task with Instantly campaign name
            campaign_name = "Test20250305"
            print(f"Creating task for lead with campaign name: {campaign_name}...")
            task_data = self.close_api.create_task_for_lead(
                lead_data["id"], campaign_name
            )

            # Store the task ID for verification
            task_id = task_data["id"]
            self.test_data["task_id"] = task_id
            print(f"Task created with ID: {task_id}")

            # Wait for the initial webhook to be processed
            print("Waiting for add_lead webhook to be processed (no timeout)...")
            webhook_data = self.wait_for_webhook_processed(task_id, "add_lead")

            # Verify initial webhook data
            assert webhook_data is not None, "Initial webhook was not processed"
            assert (
                webhook_data.get("route") == "add_lead"
            ), "Webhook route is not add_lead"
            assert (
                webhook_data.get("campaign_name") == campaign_name
            ), "Campaign name doesn't match"
            assert (
                webhook_data.get("lead_id") == lead_data["id"]
            ), "Lead ID doesn't match"
            assert (
                webhook_data.get("processed") is True
            ), "Initial webhook wasn't marked as processed"
            assert (
                webhook_data.get("instantly_result", {}).get("status") == "success"
            ), "Instantly API call failed"
            print("Initial webhook assertions passed!")

            # Clean up the webhook after successful processing
            if webhook_data and webhook_id:
                print(f"Deleting webhook with ID: {webhook_id}...")
                self.close_api.delete_webhook(webhook_id)
                print("Webhook deleted successfully")

            # Now wait for the email sent webhook
            print("\nWaiting for email sent webhook...")
            email_sent_webhook = self.wait_for_webhook_processed(task_id, "email_sent")
            # Verify email sent webhook data
            assert (
                email_sent_webhook is not None
            ), "Email sent webhook was not processed"
            assert (
                email_sent_webhook.get("route") == "email_sent"
            ), "Webhook route is not email_sent"
            assert (
                email_sent_webhook.get("processed") is True
            ), "Email sent webhook wasn't marked as processed"

            # Verify task was updated in Close
            task_data = self.close_api.get_task(task_id)
            assert (
                task_data.get("is_complete") is True
            ), "Task was not marked as completed"

            print("Email sent webhook assertions passed!")
            print("All assertions passed!")
        except Exception as e:
            print(f"Error during test execution: {e}")
            raise
