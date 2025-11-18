"""
Integration tests for the Temporal-based EasyPost tracker creation workflow.
"""

from datetime import datetime
import os
import time

from easypost.models.tracker import Tracker
import pytest
import requests
from utils.easypost import get_easypost_client

from close_utils import make_close_request
from tests.utils.close_api import CloseAPI
from tests.utils.easypost_mock import EasyPostMock


class TestAsyncEasyPostTrackerCreationTemporal:
    IMMEDIATE_RESPONSE_TIMEOUT = 5
    BACKGROUND_PROCESSING_TIMEOUT = 10

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

    def test_when_lead_is_created_then_close_crm_triggers_mailerautomation_webhook(self):
        """Test the integration in prod between Close CRM, Temporal, and EasyPost.
        
        Asserts the following:

        WHEN Close CRM receives a request to create a lead with tracking information
        THEN Close CRM triggers MailerAutomation's endpoint /easypost/create_tracker
        AND MailerAutomation creates a tracker in EasyPost
        AND MailerAutomation updates the lead in Close with the EasyPost tracker ID
        """
        lead_id = self.close_crm_create_test_lead_with_tracking_info(tracking_number=self.test_tracking_number,
                                                                     carrier=self.test_carrier)
        print(f"Test lead created with ID: {lead_id}")

        tracker_id = self.wait_for_tracker_id_from_close(lead_id=lead_id)
        assert tracker_id is not None, "Tracker ID should be set"

        easypost_tracker = easypost_get_tracker(tracker_id=tracker_id, tracking_number=self.test_tracking_number)
        assert easypost_tracker.id == tracker_id
        assert easypost_tracker.tracking_code == self.test_tracking_number
        assert easypost_tracker.carrier == self.test_carrier

    def test_when_webhook_is_triggered_it_updates_the_tracker_id_in_close(self):
        """Test the integration between MailerAutomation in staging and Close CRM in prod."""
        # given a test lead in Close CRM with a dummy tracker id
        lead_id = self.close_crm_create_test_lead_with_tracking_info()
        dummy_tracker_id = "dummy_tracker_id"
        close_crm_set_tracker_id(lead_id, dummy_tracker_id)
        assert self.close_crm_get_tracker_id(lead_id) == dummy_tracker_id
        time.sleep(2)

        # when the MailerAutomation webhook is triggered
        response = self.mailerautomation_call_create_tracker_endpoint(lead_id)
        assert response.status_code == 202
        assert "celery_task_id" not in response.json()
        time.sleep(2)

        # then MailerAutomation shall create a Tracker in EasyPost and set the tracker id in Close CRM
        updated_tracker_id = self.close_crm_get_tracker_id(lead_id)
        assert updated_tracker_id != dummy_tracker_id
    
    def close_crm_create_test_lead_with_tracking_info(self, tracking_number : str | None = None, carrier: str | None = None) -> str:
        tracking_number = tracking_number or self.test_tracking_number
        carrier = carrier or self.test_carrier

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

    def mailerautomation_call_create_tracker_endpoint(self, lead_id: str) -> requests.Response:
        webhook_payload = {
            "event": {
                "data": {
                    "id": lead_id,
                }
            }
        }
        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=webhook_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )
        response.raise_for_status()
        return response
    
    def close_crm_get_tracker_id(self, lead_id: str) -> str:
        updated_lead = self.close_api.get_lead(lead_id)
        return updated_lead.get(
            "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
        )

    def test_async_error_handling_missing_lead_id(self):
        invalid_payload = {"event": {"data": {}}}

        response = requests.post(
            f"{self.base_url}/easypost/create_tracker",
            json=invalid_payload,
            headers={"Content-Type": "application/json"},
            timeout=self.IMMEDIATE_RESPONSE_TIMEOUT,
        )

        assert response.status_code == 202

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

    def wait_for_tracker_id_from_close(self, lead_id: str):
        tracker_id = None
        start_time = time.time()
        while (time.time() - start_time) < self.BACKGROUND_PROCESSING_TIMEOUT:
            updated_lead = self.close_api.get_lead(lead_id)
            tracker_id = updated_lead.get(
                "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
            )
            if tracker_id:
                return tracker_id
            time.sleep(2)


def close_crm_set_tracker_id(lead_id: str, tracker_id: str):
    lead_update_data = {
        "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh": tracker_id,
    }

    respones = make_close_request(
        "put",
        f"https://api.close.com/api/v1/lead/{lead_id}",
        json=lead_update_data)

    respones.raise_for_status()


def easypost_get_tracker(tracker_id: str, tracking_number: str) -> Tracker:
    easypost_client = get_easypost_client(tracking_number=tracking_number)
    easypost_tracker = easypost_client.tracker.retrieve(tracker_id)
    return easypost_tracker
