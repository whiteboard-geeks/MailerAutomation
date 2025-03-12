import os
import time
import requests
from tests.utils.close_api import CloseAPI
from tests.utils.easypost_api import EasyPostAPI
from datetime import datetime


class TestEasyPostE2E:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.easypost_api = EasyPostAPI()
        self.test_data = {}

        # Set timeout to 10 minutes (600 seconds)
        self.webhook_timeout = 600  # 10 minute timeout
        print("WEBHOOK TIMEOUT: 600 seconds (10 minutes)")

        self.webhook_check_interval = 1  # Check interval in seconds
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Save original ENV_TYPE value to restore later
        self.original_env_type = os.environ.get("ENV_TYPE")
        print(f"Original ENV_TYPE: {self.original_env_type}")

        # Set ENV_TYPE to testing for this test
        os.environ["ENV_TYPE"] = "testing"
        print("Set ENV_TYPE to 'testing' for this test")

        # Test tracking number that will return 'delivered' status
        # Using EZ4000000004 which EasyPost will automatically mark as delivered
        self.test_tracking_number = "EZ2000000002"
        self.test_carrier = "USPS"

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the lead if it was created
        if self.test_data.get("lead_id"):
            result = self.close_api.delete_lead(self.test_data["lead_id"])
            if result == {}:  # Successful deletion returns empty dict
                print(f"Deleted lead with ID: {self.test_data['lead_id']}")
            else:
                print(f"Warning: Lead deletion may have failed: {result}")

        # Delete the Close webhook if it was created
        if self.test_data.get("close_webhook_id"):
            result = self.close_api.delete_webhook(self.test_data["close_webhook_id"])
            print(
                f"Deleted Close webhook with ID: {self.test_data['close_webhook_id']}"
            )

        # Delete the EasyPost webhook if it was created
        if self.test_data.get("easypost_webhook_id"):
            try:
                result = self.easypost_api.delete_webhook(
                    self.test_data["easypost_webhook_id"]
                )
                print(
                    f"Deleted EasyPost webhook with ID: {self.test_data['easypost_webhook_id']}"
                )
            except Exception as e:
                print(f"Warning: EasyPost webhook deletion may have failed: {e}")

        # Restore original ENV_TYPE if it was changed
        if self.original_env_type:
            os.environ["ENV_TYPE"] = self.original_env_type
        elif "ENV_TYPE" in os.environ:
            del os.environ["ENV_TYPE"]

    def wait_for_webhook_processed(
        self, tracker_id=None, tracking_code=None, timeout=None
    ):
        """Wait for webhook to be processed by checking the webhook tracker API."""
        webhook_endpoint = f"{self.base_url}/easypost/webhooks/status"

        # Add filters if provided
        if tracker_id:
            webhook_endpoint += f"?tracker_id={tracker_id}"
        elif tracking_code:
            webhook_endpoint += f"?tracking_code={tracking_code}"

        start_time = time.time()
        elapsed_time = 0

        # Use provided timeout or default
        if timeout is None:
            timeout = self.webhook_timeout

        # Check for timeout
        while elapsed_time < timeout:
            try:
                # Query the webhook tracker API
                response = requests.get(webhook_endpoint)

                if response.status_code == 200:
                    # We found webhook data
                    webhook_data = response.json().get("data", {})
                    if webhook_data:  # Make sure it's not empty
                        # If we're looking for a specific tracker and it's processed
                        if tracker_id and isinstance(webhook_data, dict):
                            if webhook_data.get("processed") is True:
                                return webhook_data
                        # If we're looking for any tracker with this tracking code
                        elif tracking_code and isinstance(webhook_data, dict):
                            # Find the first processed webhook
                            for tracker_id, data in webhook_data.items():
                                if data.get("processed") is True:
                                    return data

                # If not found or not processed, continue waiting
            except Exception as e:
                print(f"Error querying webhook API: {e}")

            # Sleep before trying again
            time.sleep(self.webhook_check_interval)

            # Print progress occasionally
            elapsed_time = time.time() - start_time
            if elapsed_time % 60 < 1:  # Print every ~60 seconds
                print(f"Still waiting for webhook... {int(elapsed_time)}s elapsed")

        # If we get here, we've timed out
        raise TimeoutError(
            f"Timed out waiting for webhook after {int(elapsed_time)} seconds"
        )

    def test_easypost_tracker_creation_and_delivery_update(self):
        """Test the full workflow from creating a lead through delivery status update."""
        print(
            "\n=== STARTING E2E TEST: EasyPost Tracker Creation and Delivery Update ==="
        )

        # Generate timestamp for unique identification
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format as YYYYMMDDhhmmss
        env_type = os.environ.get("ENV_TYPE", "testing")

        # First, create a webhook in Close to catch leads with tracking info
        print("Creating webhook in Close...")
        webhook_id = self.close_api.create_webhook_for_tracking_id_and_carrier()
        self.test_data["close_webhook_id"] = webhook_id
        print(f"Close webhook created with ID: {webhook_id}")

        # Create an EasyPost webhook to send delivery updates to our endpoint
        print("Creating webhook in EasyPost...")
        easypost_webhook = self.easypost_api.create_or_update_webhook()
        self.test_data["easypost_webhook_id"] = easypost_webhook["id"]
        print(f"EasyPost webhook created with ID: {easypost_webhook['id']}")

        # Create a test lead in Close with tracking number and carrier already included
        print("Creating test lead in Close with tracking information...")
        lead_data = self.close_api.create_test_lead(
            first_name="Lance",
            last_name=f"EasyPost{timestamp}",
            email=f"lance+{env_type}.easypost{timestamp}@whiteboardgeeks.com",
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": self.test_tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": self.test_carrier,
            },
            include_date_location=False,  # Exclude Date & Location Mailer Delivered field
        )
        self.test_data["lead_id"] = lead_data["id"]
        print(
            f"Test lead created with ID: {lead_data['id']} including tracking number and carrier"
        )

        # Wait for the Close webhook to trigger the create_tracker route and create the EasyPost tracker
        print("Waiting for Close webhook to trigger create_tracker route...")
        time.sleep(5)  # Give some time for the webhook to process

        # Verify the Close lead was updated with the tracker ID
        print("Verifying Close lead was updated with tracker ID...")
        updated_lead = self.close_api.get_lead(lead_data["id"])

        # Check if the lead has an EasyPost tracker ID
        tracker_id = updated_lead.get(
            "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
        )

        # Assert that the create_tracker route triggered automatically and created a tracker
        assert (
            tracker_id is not None
        ), "create_tracker webhook failed to trigger automatically and create an EasyPost tracker"
        print(
            f"create_tracker webhook successfully triggered and created EasyPost tracker with ID: {tracker_id}"
        )
        print("All assertions passed!")
