from datetime import datetime
import asyncio
import json
import os
import time
from typing import Any
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result

import pytest
import requests
from temporalio.client import WorkflowExecutionStatus

from temporal.client_provider import get_temporal_client
from temporal.temporal_workflows_client import TemporalWorkflowsClient
from temporal.workflows.easypost.webhook_delivery_status_workflow import Status, WebhookDeliveryStatusResult
from tests.utils.close_api import CloseAPI


@pytest.mark.skipif(
    os.environ.get("USE_TEMPORAL_FOR_EASYPOST_DELIVERY_STATUS", "false").lower() != "true",
    reason="USE_TEMPORAL_FOR_EASYPOST_DELIVERY_STATUS is not set to true",
)


class TestAsyncEasyPostDeliveryStatusTemporal:
    # Test configuration
    IMMEDIATE_RESPONSE_TIMEOUT = 5  # Seconds - async should respond immediately

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
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        print("Getting temporal client...")
        temporal_client = loop.run_until_complete(get_temporal_client())
        print("Got temporal client")
        self.temporal_workflows_client = TemporalWorkflowsClient(temporal_client)
        self.temporal_workflows_to_terminate : list[str] = []

        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Generate timestamp for unique identification
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        env_type = os.environ.get("ENV_TYPE", "testing")

        # Test tracking number that will return 'delivered' status
        self.test_tracking_number = "EZ1000000001"
        self.test_carrier = "USPS"

        # Generate unique test data
        self.test_first_name = "Lance"
        self.test_last_name = f"AsyncDelivery{self.timestamp}"
        self.test_email = (
            f"lance+{env_type}.async.delivery{self.timestamp}@whiteboardgeeks.com"
        )

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

        # Save original ENV_TYPE value to restore later
        self.original_env_type = os.environ.get("ENV_TYPE")
        print(f"Original ENV_TYPE: {self.original_env_type}")

        # Set ENV_TYPE to testing for this test
        os.environ["ENV_TYPE"] = "testing"
        print("Set ENV_TYPE to 'testing' for this test")

    def teardown_method(self):
        """Cleanup after each test."""
        # Restore original ENV_TYPE
        if self.original_env_type:
            os.environ["ENV_TYPE"] = self.original_env_type
        else:
            os.environ.pop("ENV_TYPE", None)

        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            result = self.close_api.delete_lead(self.test_data["lead_id"])
            if result == {}:  # Successful deletion returns empty dict
                print(f"Deleted lead with ID: {self.test_data['lead_id']}")
            else:
                print(f"Warning: Lead deletion may have failed: {result}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for workflow_id in self.temporal_workflows_to_terminate:
            print(f"Terminating workflow: {workflow_id}")
            try:
                loop.run_until_complete(self.temporal_workflows_client.terminate_workflow(workflow_id))
            except Exception as e:
                print(f"Warning: Could not terminate workflow {workflow_id}: {e}")

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Test happy path for async delivery status processing.
        
        Asserts that
        1. the endpoint returns 202 within 5 seconds
        2. on Close the lead is updated with the correct delivery information
        3. on Close the lead is updated with the correct custom activity
        """
        # Create test lead and tracker
        lead_id = self.close_create_test_lead(self.test_tracking_number, self.test_carrier)

        tracker_id = self.close_get_tracker_id(lead_id)
        print(f"Using tracker ID: {tracker_id}")

        delivery_payload = self.build_delivery_payload(
            tracker_id, self.test_tracking_number, self.test_carrier
        )

        response, response_time = self.post_easypost_delivery_status_webhook(delivery_payload)
        self.assert_response_is_immediate(response_time)
        self.assert_response_is_202(response)
        workflow_id = self.get_temporal_workflow_id_or_fail(response)

        workflow_result = await self.get_temporal_workflow_result(workflow_id)
        assert workflow_result.status == Status.SUCCESS

        # Wait a bit for the updates to propagate
        time.sleep(1)

        # Verify the lead was updated with delivery information
        print("Verifying lead was updated with delivery information...")
        updated_lead = self.close_api.get_lead(lead_id)

        self.assert_package_delivered_field_is_yes(updated_lead)
        self.assert_delivery_city_is_updated(updated_lead)
        self.assert_delivery_state_is_updated(updated_lead)

        print("✅ Lead updated with delivery information")

    @pytest.mark.asyncio
    async def test_duplicate_delivery_prevention(self):
        duplicate_test_tracking_number = "EZ4000000004"
        duplicate_test_carrier = "USPS"

        lead_id = self.close_create_duplicate_test_lead(duplicate_test_tracking_number, duplicate_test_carrier)

        tracker_id = self.close_get_tracker_id(lead_id)
        print(f"Using tracker ID: {tracker_id}")

        delivery_payload = self.build_delivery_payload(
            tracker_id, duplicate_test_tracking_number, duplicate_test_carrier
        )

        response_1, _ = self.post_easypost_delivery_status_webhook(delivery_payload)
        self.assert_response_is_202(response_1)
        workflow_id_1 = self.get_temporal_workflow_id_or_fail(response_1)
        workflow_result_1 = await self.get_temporal_workflow_result(workflow_id_1)
        assert workflow_result_1.status == Status.SUCCESS

        self.assert_close_num_mailer_delivered_activities(lead_id, 1)

        delivery_payload["id"] = f"evt_test_duplicate_{self.timestamp}"
        response_2, _ = self.post_easypost_delivery_status_webhook(delivery_payload)
        workflow_id_2 = self.get_temporal_workflow_id_or_fail(response_2)
        workflow_result_2 = await self.get_temporal_workflow_result(workflow_id_2)
        assert workflow_result_2.status == Status.NO_OP_DUPLICATE_ACTIVITY

        self.assert_close_num_mailer_delivered_activities(lead_id, 1)

        self.close_api.delete_lead(lead_id)

    @pytest.mark.asyncio
    async def test_error_handling_missing_result_field(self):
        invalid_payload = {"id": "evt_invalid"}

        response, _ = self.post_easypost_delivery_status_webhook(invalid_payload)
        self.assert_response_is_400_invalid_request_format(response)

    @pytest.mark.asyncio
    async def test_status_non_delivered(self):
        non_delivered_payload = self.build_non_delivered_payload()

        response, _ = self.post_easypost_delivery_status_webhook(non_delivered_payload)
        self.assert_is_non_delivered_response(response)
    
    @pytest.mark.asyncio
    async def test_delivered_to_sender(self):
        delivered_to_sender_payload = self.build_delivered_to_sender_payload()

        response, _ = self.post_easypost_delivery_status_webhook(delivered_to_sender_payload)
        self.assert_response_is_202(response)

        temporal_workflow_id = self.get_temporal_workflow_id_or_fail(response)

        workflow_result = await self.get_temporal_workflow_result(temporal_workflow_id)
        assert workflow_result.status == Status.NO_OP_RETURNED_TO_SENDER

    def close_create_test_lead(self, tracking_number: str, carrier: str) -> str:
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=self.test_last_name,
            email=self.test_email,
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": carrier,
            },
            include_date_location=False,
        )
        self.test_data["lead_id"] = lead_data["id"]

        return lead_data["id"]

    def close_create_duplicate_test_lead(self, tracking_number: str, carrier: str) -> str:
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=f"{self.test_last_name}Duplicate",
            email=f"lance+duplicate.async.{self.timestamp}@whiteboardgeeks.com",
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": carrier,
            },
            include_date_location=False,
        )
        return lead_data["id"]

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(1))
    def close_get_tracker_id(self, lead_id: str) -> str:
        updated_lead = self.close_api.get_lead(lead_id)
        tracker_id = updated_lead.get(
            "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
        )
        assert tracker_id, "Tracker ID should be set"
        return tracker_id

    def build_delivery_payload(self, tracker_id: str, tracking_number: str, carrier: str) -> dict:
        delivery_payload = {
            "id": f"evt_test_async_{self.timestamp}",
            "result": self.delivery_webhook_payload.copy(),
        }

        delivery_payload["result"]["id"] = tracker_id
        delivery_payload["result"]["tracking_code"] = tracking_number
        delivery_payload["result"]["carrier"] = carrier

        return delivery_payload

    def build_non_delivered_payload(self) -> dict:
        non_delivered_payload = {
            "id": f"evt_test_non_delivered_{self.timestamp}",
            "result": self.delivery_webhook_payload.copy(),
        }

        # Modify status to be non-delivered
        non_delivered_payload["result"]["status"] = "in_transit"
        non_delivered_payload["result"]["tracking_code"] = "EZ9999999999"

        return non_delivered_payload
    
    def build_delivered_to_sender_payload(self) -> dict:
        delivered_to_sender_payload = {
            "id": f"evt_test_delivered_to_sender_{self.timestamp}",
            "result": self.delivery_webhook_payload.copy(),
        }

        # Modify status to be delivered to sender
        delivered_to_sender_payload["result"]["tracking_details"][-1]["message"] = "Delivered, To Original Sender"

        return delivered_to_sender_payload

    def post_easypost_delivery_status_webhook(self, payload: dict) -> tuple[requests.Response, float]:
        start_time = time.time()
        response = requests.post(
            f"{self.base_url}/easypost/delivery_status",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT + 10,
        )
        response_time = time.time() - start_time
        return response, response_time

    def assert_response_is_immediate(self, response_time: float) -> None:
        if response_time > self.IMMEDIATE_RESPONSE_TIMEOUT:
            pytest.fail(
                f"Response too slow: {response_time:.2f}s (expected <{self.IMMEDIATE_RESPONSE_TIMEOUT}s)"
            )

    @staticmethod
    def assert_response_is_202(response: requests.Response) -> None:
        if response.status_code != 202:
            pytest.fail(f"Expected 202, got {response.status_code}")

    @staticmethod
    def assert_response_is_400_invalid_request_format(response: requests.Response) -> None:
        assert response.status_code == 400
        assert response.json() == {
            "status": "error",
            "message": "Invalid request format",
        }

    @staticmethod
    def assert_is_non_delivered_response(response: requests.Response) -> None:
        assert response.status_code == 200

        response_data = response.json()
        expected_response = {
            "status": "success",
            "message": "Tracking status is not 'delivered' so did not run.",
        }

        assert response_data == expected_response

    def assert_close_num_mailer_delivered_activities(self, lead_id: str, expected_num: int) -> None:
        # Check custom activities after first webhook - should be exactly 1
        mailer_delivered_activity_type = "custom.actitype_3KhBfWgjtVfiGYbczbgOWv"
        activities_actual = self.close_api.get_lead_custom_activities(
            lead_id, mailer_delivered_activity_type
        )

        print(f"Custom activities after first webhook: {len(activities_actual)}")
        assert (
            len(activities_actual) == expected_num
        ), f"Expected exactly 1 custom activity after first webhook, but found {len(activities_actual)}"

    async def assert_temporal_workflow_failed(self, temporal_workflow_id: str) -> None:
        status, _ = await self._get_workflow_status_and_result(temporal_workflow_id)
        assert status == WorkflowExecutionStatus.FAILED
    
    def get_temporal_workflow_id_or_fail(self, response: requests.Response) -> str:
        response_data = response.json()
        temporal_workflow_id = response_data.get("temporal_workflow_id")
        assert temporal_workflow_id, "Response should include temporal_workflow_id"
        self.temporal_workflows_to_terminate.append(temporal_workflow_id)
        return temporal_workflow_id

    async def get_temporal_workflow_result(self, temporal_workflow_id: str) -> WebhookDeliveryStatusResult:
        _, result = await self._get_workflow_status_and_result(temporal_workflow_id)
        # print type of result
        print(f"get_temporal_workflow_result Result type: {type(result)}")
        return WebhookDeliveryStatusResult.model_validate(result)

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(1), retry=retry_if_result(lambda r: r[0] == WorkflowExecutionStatus.RUNNING))
    async def _get_workflow_status_and_result(self, temporal_workflow_id: str) -> tuple[WorkflowExecutionStatus | None, Any]:
        return await self.temporal_workflows_client.get_workflow_status_and_result(temporal_workflow_id)

    @staticmethod
    def assert_package_delivered_field_is_yes(lead: dict) -> None:
       assert (
            lead.get("custom.cf_wkZ5ptOR1Ro3YPxJPYipI35M7ticuYvJHFgp2y4fzdQ")
            == "Yes"
        ), "Lead should be updated with package_delivered=Yes"

    @staticmethod
    def assert_delivery_city_is_updated(lead: dict) -> None:
        assert (
            lead.get("custom.cf_1hWUFxiA6QhUXrYT3lDh96JSWKxVBBAKCB3XO8EXGUW")
            is not None
        ), "Lead should be updated with delivery city"

    @staticmethod
    def assert_delivery_state_is_updated(lead: dict) -> None:
        assert (
            lead.get("custom.cf_vxfsYfTrFk6oYrnSx0ViYrUMpE7y5sxi0NnRgTyOf30")
            is not None
        ), "Lead should be updated with delivery state"