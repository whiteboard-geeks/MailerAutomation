import os
import json
import time
import tempfile
from tests.utils.close_api import CloseAPI


class TestInstantlyE2E:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}

        # Check if we should use timeout or wait indefinitely (for development)
        webhook_timeout_env = os.environ.get("WEBHOOK_TIMEOUT")
        if webhook_timeout_env == "NONE" or webhook_timeout_env == "0":
            self.webhook_timeout = None  # No timeout - wait indefinitely
            print("WEBHOOK TIMEOUT: NONE (waiting indefinitely)")
        else:
            self.webhook_timeout = (
                int(webhook_timeout_env) if webhook_timeout_env else 600
            )  # Default 10 minutes
            print(f"WEBHOOK TIMEOUT: {self.webhook_timeout} seconds")

        self.webhook_check_interval = 1  # Check interval in seconds
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Make sure ENV_TYPE is set to 'test' for the webhook tracking to work
        if os.environ.get("ENV_TYPE") != "test":
            os.environ["ENV_TYPE"] = "test"
        print(f"ENV_TYPE: {os.environ.get('ENV_TYPE')}")

        # Create temp directory for webhook notifications if it doesn't exist
        self.webhook_dir = os.path.join(
            tempfile.gettempdir(), "instantly_webhook_tests"
        )
        os.makedirs(self.webhook_dir, exist_ok=True)

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

        # Delete the webhook if it was created
        if self.test_data.get("webhook_id"):
            self.close_api.delete_webhook(self.test_data["webhook_id"])

        # Clean up any webhook notification files
        if self.test_data.get("task_id"):
            notification_file = os.path.join(
                self.webhook_dir, f"{self.test_data['task_id']}.json"
            )
            if os.path.exists(notification_file):
                os.remove(notification_file)

    def wait_for_webhook_processed(self, task_id):
        """Wait for webhook to be processed by checking for a notification file."""
        notification_file = os.path.join(self.webhook_dir, f"{task_id}.json")

        start_time = time.time()
        elapsed_time = 0

        # Loop with or without timeout based on configuration
        while self.webhook_timeout is None or elapsed_time < self.webhook_timeout:
            # Check if notification file exists
            if os.path.exists(notification_file):
                # Read the file to get webhook data
                try:
                    with open(notification_file, "r") as f:
                        webhook_data = json.load(f)
                    return webhook_data
                except Exception as e:
                    print(f"Error reading webhook notification file: {e}")

            # Sleep before trying again
            time.sleep(self.webhook_check_interval)

            # Update elapsed time (only needed if we have a timeout)
            if self.webhook_timeout is not None:
                elapsed_time = time.time() - start_time
                if elapsed_time % 60 < 1:  # Print every ~60 seconds
                    print(f"Still waiting... {int(elapsed_time)}s elapsed")

        return None

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
            # Close should call our webhook endpoint asynchronously
            # We'll wait for a notification file to be created
            print(
                f"Waiting for webhook to be processed (timeout: {self.webhook_timeout}s)..."
            )
            webhook_data = self.wait_for_webhook_processed(task_id)

            if webhook_data is not None:
                print("Webhook processed successfully!")
                print(f"Webhook data: {json.dumps(webhook_data, indent=2)}")
            else:
                print("ERROR: Webhook was not processed within the timeout period")

            assert (
                webhook_data is not None
            ), "Webhook was not processed within the timeout period"
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
        finally:
            # Make sure webhook is deleted even if test fails
            if webhook_id and not self.test_data.get("webhook_id"):
                print(f"Cleaning up webhook with ID: {webhook_id}...")
                self.close_api.delete_webhook(webhook_id)
                print("Webhook deleted")
            print("=== E2E TEST COMPLETED ===")
