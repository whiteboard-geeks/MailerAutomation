import os
import json
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

        # Make sure ENV_TYPE is set to 'test' for the webhook tracking to work
        if os.environ.get("ENV_TYPE") != "test":
            os.environ["ENV_TYPE"] = "test"
        print(f"ENV_TYPE: {os.environ.get('ENV_TYPE')}")

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

        # Delete the webhook if it was created
        if self.test_data.get("webhook_id"):
            self.close_api.delete_webhook(self.test_data["webhook_id"])

    def wait_for_webhook_processed(self, task_id):
        """Wait for webhook to be processed by checking the webhook tracker API."""
        webhook_endpoint = f"{self.base_url}/instantly/test/webhooks?task_id={task_id}"

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
                print(f"Still waiting... {int(elapsed_time)}s elapsed")

    def test_create_lead_and_task_workflow(self):
        """Test the full workflow from creating a lead to handling webhook."""
        print("\n=== STARTING E2E TEST: Create lead and task workflow ===")

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
            campaign_name = "Test20250227"
            print(f"Creating task for lead with campaign name: {campaign_name}...")
            task_data = self.close_api.create_task_for_lead(
                lead_data["id"], f"Instantly: {campaign_name}"
            )

            # Store the task ID for verification
            task_id = task_data["id"]
            self.test_data["task_id"] = task_id
            print(f"Task created with ID: {task_id}")

            # Wait for the webhook to be processed
            print("Waiting for webhook to be processed (no timeout)...")
            webhook_data = self.wait_for_webhook_processed(task_id)
            # Clean up the webhook after successful processing
            if webhook_data and webhook_id:
                print(f"Deleting webhook with ID: {webhook_id}...")
                self.close_api.delete_webhook(webhook_id)
                print("Webhook deleted successfully")
            # Verify webhook data
            assert webhook_data is not None, "Webhook was not processed"
            assert (
                webhook_data.get("campaign_name") == campaign_name
            ), "Campaign name doesn't match"
            assert (
                webhook_data.get("lead_id") == lead_data["id"]
            ), "Lead ID doesn't match"
            assert (
                webhook_data.get("processed") is True
            ), "Webhook wasn't marked as processed"

            print("All assertions passed!")

            # 4. Verify the lead was properly processed
            # In an actual implementation, check if the lead was added to Instantly campaign
            # For example:
            # - Check if a record exists in your database
            # - Call Instantly API to verify the lead is in the campaign
            # - Check for specific updates to the task or lead in Close
        except Exception as e:
            print(f"Error during test execution: {e}")
            raise
