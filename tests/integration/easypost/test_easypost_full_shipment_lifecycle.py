import os
import json
import requests
import time
import pytest
from tests.utils.close_api import CloseAPI
from tests.utils.easypost_mock import EasyPostMock
from datetime import datetime


class TestEasyPostFullShipmentLifecycleIntegration:
    @classmethod
    def setup_class(cls):
        """Setup before all tests in the class."""
        # Clean up any lingering test data from previous runs
        close_api = CloseAPI()

        # Search for any leads with test tracking numbers
        for test_number in ["EZ1000000001", "EZ4000000004"]:
            test_leads = close_api.search_leads_by_tracking_number(test_number)
            for lead in test_leads:
                print(f"Cleaning up existing test lead with ID: {lead['id']}")
                close_api.delete_lead(lead["id"])

    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Webhook timeout (in seconds)
        self.webhook_timeout = 300  # 5 minute timeout
        self.webhook_check_interval = 1  # Check interval in seconds

        # Load the mock webhook payloads
        with open(
            "tests/integration/easypost/close_tracking_number_and_carrier_updated.json",
            "r",
        ) as f:
            self.mock_payload = json.load(f)

        with open(
            "tests/integration/easypost/easypost_package_delivered.json", "r"
        ) as f:
            self.delivery_webhook_payload = json.load(f)

        # Set environment type and current date
        env_type = os.environ.get("ENV_TYPE", "test")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Test tracking number that will return 'delivered' status
        # Use one of the valid EasyPost test tracking numbers
        self.test_tracking_number = "EZ1000000001"  # Different from the other test
        self.test_carrier = "USPS"

        # Generate a unique name for the test lead
        self.test_first_name = "Lance"
        self.test_last_name = f"EasyPost{timestamp}"
        self.test_email = f"lance+{env_type}.easypost{timestamp}@whiteboardgeeks.com"

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            self.close_api.delete_lead(self.test_data["lead_id"])

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

    @pytest.fixture(autouse=True)
    def setup_easypost_mock(self, monkeypatch):
        """Setup EasyPost mock for all tests in this class."""
        # Mock the EasyPost tracker create method
        self.mock_tracker = EasyPostMock.mock_tracker_create(
            monkeypatch,
            mock_response_file="tests/integration/easypost/mock_create_tracker_response.json",
        )

        # Update the mock response with our test data
        self.mock_tracker.create.return_value.tracking_code = self.test_tracking_number
        self.mock_tracker.create.return_value.carrier = self.test_carrier

    def test_easypost_integration_full_shipment_lifecycle(self):
        """Test the complete EasyPost integration flow from creating a tracker to receiving and processing delivery status updates."""
        print("\n=== STARTING INTEGRATION TEST: EasyPost Full Shipment Lifecycle ===")

        # Create a test lead in Close with tracking number and carrier
        print("Creating test lead in Close with tracking information...")
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=self.test_last_name,
            email=self.test_email,
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": self.test_tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": self.test_carrier,
            },
            include_date_location=False,  # Exclude Date & Location Mailer Delivered field
        )
        self.test_data["lead_id"] = lead_data["id"]
        print(f"Test lead created with ID: {lead_data['id']}")

        # Update the mock payload with the lead ID, name, and other fields
        self.mock_payload["event"]["lead_id"] = lead_data["id"]
        self.mock_payload["event"]["object_id"] = lead_data["id"]
        self.mock_payload["event"]["data"]["id"] = lead_data["id"]
        self.mock_payload["event"]["data"]["name"] = (
            f"{self.test_first_name} {self.test_last_name}"
        )
        self.mock_payload["event"]["data"]["display_name"] = (
            f"{self.test_first_name} {self.test_last_name}"
        )

        # Update the carrier and tracking_number in the mock payload
        self.mock_payload["event"]["data"][
            "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l"
        ] = [self.test_carrier]
        self.mock_payload["event"]["data"][
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
        ] = self.test_tracking_number

        # Send the mock webhook to create_tracker endpoint
        print("Sending mock webhook to create_tracker endpoint...")
        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=self.mock_payload,
        )

        # Check response
        print(f"Create tracker response status: {response.status_code}")
        print(f"Create tracker response: {response.json()}")

        assert response.status_code == 200, "Create tracker request failed"
        assert (
            response.json()["status"] == "success"
        ), "Create tracker request was not successful"

        # Store tracker ID
        tracker_id = response.json()["tracker_id"]
        self.test_data["tracker_id"] = tracker_id
        print(f"EasyPost tracker created with ID: {tracker_id}")

        # Verify the lead was updated with the tracker ID
        print("Verifying Close lead was updated with tracker ID...")
        updated_lead = self.close_api.get_lead(lead_data["id"])

        # Check if the lead has an EasyPost tracker ID
        lead_tracker_id = updated_lead.get(
            "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
        )

        assert (
            lead_tracker_id is not None
        ), "Lead was not updated with EasyPost tracker ID"
        assert (
            lead_tracker_id == tracker_id
        ), "Lead's tracker ID doesn't match the created tracker"

        print("Lead was successfully updated with the EasyPost tracker ID")

        # Prepare mock delivery webhook payload
        # Create a copy of the delivery webhook payload and update with the tracker ID
        delivery_payload = {
            "id": f"evt_test_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "result": self.delivery_webhook_payload,
        }

        # Update the delivery webhook payload with the test data
        delivery_payload["result"]["id"] = tracker_id
        delivery_payload["result"]["tracking_code"] = self.test_tracking_number
        delivery_payload["result"]["carrier"] = self.test_carrier

        # Send the mock delivery webhook
        print("Sending mock delivery status webhook...")
        delivery_response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=delivery_payload,
        )

        print(f"Delivery status response code: {delivery_response.status_code}")
        print(f"Delivery status response: {delivery_response.json()}")

        assert delivery_response.status_code == 200, "Delivery status update failed"

        # Wait for the webhook to be processed
        print("Waiting for delivery status to be processed...")
        webhook_data = self.wait_for_webhook_processed(
            tracking_code=self.test_tracking_number
        )

        # Verify webhook data
        assert webhook_data is not None, "delivery_status webhook was not processed"
        assert (
            webhook_data.get("processed") is True
        ), "delivery_status webhook wasn't marked as processed"
        assert (
            webhook_data.get("result") == "Success"
        ), f"delivery_status webhook processing failed: {webhook_data.get('error', 'Unknown error')}"
        print("delivery_status webhook was successfully processed")

        # Verify lead was updated with delivery information
        print("Verifying lead was updated with delivery information...")
        final_lead = self.close_api.get_lead(lead_data["id"])

        # Check that package_delivered field is set to "Yes"
        assert (
            final_lead.get("custom.cf_wkZ5ptOR1Ro3YPxJPYipI35M7ticuYvJHFgp2y4fzdQ")
            == "Yes"
        ), "Lead was not updated with package_delivered=Yes"

        # Check that delivery city and state were updated
        assert (
            final_lead.get("custom.cf_1hWUFxiA6QhUXrYT3lDh96JSWKxVBBAKCB3XO8EXGUW")
            is not None
        ), "Lead was not updated with delivery city"

        assert (
            final_lead.get("custom.cf_vxfsYfTrFk6oYrnSx0ViYrUMpE7y5sxi0NnRgTyOf30")
            is not None
        ), "Lead was not updated with delivery state"

        print("Lead was successfully updated with delivery information")
        print("All assertions passed!")

    def test_duplicate_delivery_webhook_prevention(self):
        """Test that duplicate delivery webhooks don't create duplicate custom activities."""
        print(
            "\n=== STARTING INTEGRATION TEST: Duplicate Delivery Webhook Prevention ==="
        )

        # Use a different tracking number to avoid conflicts with other tests
        duplicate_test_tracking_number = "EZ4000000004"
        duplicate_test_carrier = "USPS"

        # Create a test lead in Close with tracking number and carrier
        print("Creating test lead in Close with tracking information...")
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=f"{self.test_last_name}Duplicate",
            email=f"lance+duplicate.easypost{datetime.now().strftime('%Y%m%d%H%M%S')}@whiteboardgeeks.com",
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": duplicate_test_tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": duplicate_test_carrier,
            },
            include_date_location=False,  # Exclude Date & Location Mailer Delivered field
        )
        duplicate_lead_id = lead_data["id"]
        print(f"Test lead created with ID: {duplicate_lead_id}")

        # Update the mock payload for this test
        duplicate_mock_payload = self.mock_payload.copy()
        duplicate_mock_payload["event"]["lead_id"] = duplicate_lead_id
        duplicate_mock_payload["event"]["object_id"] = duplicate_lead_id
        duplicate_mock_payload["event"]["data"]["id"] = duplicate_lead_id
        duplicate_mock_payload["event"]["data"]["name"] = (
            f"{self.test_first_name} {self.test_last_name}Duplicate"
        )
        duplicate_mock_payload["event"]["data"]["display_name"] = (
            f"{self.test_first_name} {self.test_last_name}Duplicate"
        )
        duplicate_mock_payload["event"]["data"][
            "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l"
        ] = [duplicate_test_carrier]
        duplicate_mock_payload["event"]["data"][
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
        ] = duplicate_test_tracking_number

        # Send the mock webhook to create_tracker endpoint
        print("Sending mock webhook to create_tracker endpoint...")
        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=duplicate_mock_payload,
        )

        assert response.status_code == 200, "Create tracker request failed"
        assert (
            response.json()["status"] == "success"
        ), "Create tracker request was not successful"

        # Store tracker ID
        tracker_id = response.json()["tracker_id"]
        print(f"EasyPost tracker created with ID: {tracker_id}")

        # Prepare mock delivery webhook payload
        delivery_payload = {
            "id": f"evt_test_duplicate_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "result": self.delivery_webhook_payload.copy(),
        }

        # Update the delivery webhook payload with the test data
        delivery_payload["result"]["id"] = tracker_id
        delivery_payload["result"]["tracking_code"] = duplicate_test_tracking_number
        delivery_payload["result"]["carrier"] = duplicate_test_carrier

        # Send the FIRST delivery webhook
        print("Sending FIRST mock delivery status webhook...")
        first_delivery_response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=delivery_payload,
        )

        print(
            f"First delivery status response code: {first_delivery_response.status_code}"
        )
        print(f"First delivery status response: {first_delivery_response.json()}")

        assert (
            first_delivery_response.status_code == 200
        ), "First delivery status update failed"

        # Wait for the first webhook to be processed
        print("Waiting for first delivery status to be processed...")
        first_webhook_data = self.wait_for_webhook_processed(
            tracking_code=duplicate_test_tracking_number
        )

        # Verify first webhook was processed successfully
        assert (
            first_webhook_data is not None
        ), "First delivery_status webhook was not processed"
        assert (
            first_webhook_data.get("processed") is True
        ), "First delivery_status webhook wasn't marked as processed"
        assert (
            first_webhook_data.get("result") == "Success"
        ), f"First delivery_status webhook processing failed: {first_webhook_data.get('error', 'Unknown error')}"
        print("First delivery_status webhook was successfully processed")

        # Check custom activities after first webhook - should be exactly 1
        mailer_delivered_activity_type = "custom.actitype_3KhBfWgjtVfiGYbczbgOWv"
        activities_after_first = self.close_api.get_lead_custom_activities(
            duplicate_lead_id, mailer_delivered_activity_type
        )

        print(f"Custom activities after first webhook: {len(activities_after_first)}")
        assert (
            len(activities_after_first) == 1
        ), f"Expected exactly 1 custom activity after first webhook, but found {len(activities_after_first)}"

        # Send the SECOND delivery webhook (duplicate)
        print("Sending SECOND (duplicate) mock delivery status webhook...")

        # Create a new event ID for the second webhook to simulate a real duplicate scenario
        delivery_payload["id"] = (
            f"evt_test_duplicate2_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )

        second_delivery_response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=delivery_payload,
        )

        print(
            f"Second delivery status response code: {second_delivery_response.status_code}"
        )
        print(f"Second delivery status response: {second_delivery_response.json()}")

        assert (
            second_delivery_response.status_code == 200
        ), "Second delivery status update failed"

        # Wait a moment for the second webhook to be processed
        print("Waiting for second delivery status to be processed...")
        time.sleep(
            2
        )  # Short wait since we don't expect it to create new webhook tracker data

        # Check custom activities after second webhook - should STILL be exactly 1 (no duplicate created)
        activities_after_second = self.close_api.get_lead_custom_activities(
            duplicate_lead_id, mailer_delivered_activity_type
        )

        print(f"Custom activities after second webhook: {len(activities_after_second)}")
        assert (
            len(activities_after_second) == 1
        ), f"Expected exactly 1 custom activity after second webhook (no duplicate), but found {len(activities_after_second)}"

        # Verify the single activity has the correct information
        activity = activities_after_second[0]

        # Handle potential prefix differences in activity type ID
        expected_type = mailer_delivered_activity_type.replace("custom.", "")
        actual_type = activity["custom_activity_type_id"].replace("custom.", "")
        assert (
            actual_type == expected_type
        ), f"Activity has wrong type. Expected: {expected_type}, Got: {actual_type}"

        assert (
            activity["lead_id"] == duplicate_lead_id
        ), "Activity is associated with wrong lead"

        print("✅ SUCCESS: Duplicate delivery webhook prevention working correctly!")
        print(
            "✅ Only one custom activity exists after two identical delivery webhooks"
        )

        # Cleanup: Delete the test lead
        self.close_api.delete_lead(duplicate_lead_id)
        print(f"Cleaned up test lead: {duplicate_lead_id}")
        print("All assertions passed!")
