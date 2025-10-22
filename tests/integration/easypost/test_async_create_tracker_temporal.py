"""
Integration tests for the Temporal-based EasyPost tracker creation workflow.
"""

import os
import time
import pytest
import requests
from datetime import datetime
from blueprints.easypost import get_easypost_client
from tests.utils.close_api import CloseAPI
from tests.utils.easypost_mock import EasyPostMock


TEMPORAL_FLAG = os.getenv("USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER", "false")
TEMPORAL_ENABLED = TEMPORAL_FLAG.strip().lower() in {"1", "true", "yes", "on"}

pytestmark = pytest.mark.skipif(
    not TEMPORAL_ENABLED,
    reason="Temporal integration tests require USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER to be true.",
)


class TestAsyncEasyPostTrackerCreationTemporal:
    IMMEDIATE_RESPONSE_TIMEOUT = 5
    BACKGROUND_PROCESSING_TIMEOUT = 120

    @classmethod
    def setup_class(cls):
        close_api = CloseAPI()
        test_leads = close_api.search_leads_by_tracking_number("EZ2000000002")
        for lead in test_leads:
            print(f"Cleaning up existing test lead with ID: {lead['id']}")
            close_api.delete_lead(lead["id"])

    def setup_method(self):
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        env_type = os.environ.get("ENV_TYPE", "testing")
        self.test_tracking_number = "EZ2000000002"
        self.test_carrier = "USPS"
        self.test_first_name = "Lance"
        self.test_last_name = f"AsyncEasyPostTemporal{self.timestamp}"
        self.test_email = (
            f"lance+{env_type}.async.easypost.temporal{self.timestamp}@whiteboardgeeks.com"
        )

        self.original_env_type = os.environ.get("ENV_TYPE")
        os.environ["ENV_TYPE"] = "testing"

    def teardown_method(self):
        if self.original_env_type:
            os.environ["ENV_TYPE"] = self.original_env_type
        else:
            os.environ.pop("ENV_TYPE", None)

        if self.test_data.get("lead_id"):
            result = self.close_api.delete_lead(self.test_data["lead_id"])
            if result == {}:
                print(f"Deleted lead with ID: {self.test_data['lead_id']}")
            else:
                print(f"Warning: Lead deletion may have failed: {result}")

        if self.test_data.get("close_webhook_id"):
            result = self.close_api.delete_webhook(self.test_data["close_webhook_id"])
            print(f"Deleted Close webhook with ID: {self.test_data['close_webhook_id']}")

    @pytest.fixture(autouse=True)
    def setup_easypost_mock(self, monkeypatch):
        self.mock_tracker = EasyPostMock.mock_tracker_create(
            monkeypatch,
            mock_response_file="tests/integration/easypost/mock_create_tracker_response.json",
        )
        self.mock_tracker.create.return_value.tracking_code = self.test_tracking_number
        self.mock_tracker.create.return_value.carrier = self.test_carrier

    def test_background_task_completion(self):
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=self.test_last_name,
            email=self.test_email,
            custom_fields={
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": self.test_tracking_number,
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": self.test_carrier,
            },
            include_date_location=False,
        )
        self.test_data["lead_id"] = lead_data["id"]

        webhook_payload = {
            "event": {
                "data": {
                    "id": lead_data["id"],
                }
            }
        }

        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=webhook_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        assert response.status_code == 202
        response_data = response.json()
        assert "celery_task_id" not in response_data

        tracker_id = None
        start_time = time.time()
        while (time.time() - start_time) < self.BACKGROUND_PROCESSING_TIMEOUT:
            updated_lead = self.close_api.get_lead(lead_data["id"])
            tracker_id = updated_lead.get(
                "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
            )
            if tracker_id:
                break
            time.sleep(2)

        assert tracker_id is not None, "Lead should be updated with EasyPost tracker ID"

        easypost_client = get_easypost_client(tracking_number=self.test_tracking_number)
        easypost_tracker = easypost_client.tracker.retrieve(tracker_id)
        assert easypost_tracker.id == tracker_id
        assert easypost_tracker.tracking_code == self.test_tracking_number
        assert easypost_tracker.carrier == self.test_carrier

    def test_async_error_handling_missing_lead_id(self):
        invalid_payload = {"event": {"data": {}}}

        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=invalid_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        assert response.status_code == 400
        response_data = response.json()
        assert response_data["status"] == "error"
        assert "No lead_id provided" in response_data["message"]

    def test_async_error_handling_missing_tracking_info(self):
        lead_data = self.close_api.create_test_lead(
            first_name=self.test_first_name,
            last_name=f"{self.test_last_name}NoTracking",
            email=f"lance+temporal.notracking.{self.timestamp}@whiteboardgeeks.com",
            custom_fields={},
            include_date_location=False,
        )
        self.test_data["lead_id"] = lead_data["id"]

        missing_tracking_payload = {
            "event": {
                "data": {
                    "id": lead_data["id"],
                }
            }
        }

        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=missing_tracking_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        assert response.status_code == 202
        response_data = response.json()
        assert "celery_task_id" not in response_data
