import json
from datetime import datetime
from typing import Any

import structlog
from pydantic import BaseModel, Field
from temporalio import activity

from close_utils import make_close_request
from utils.email import send_email
from utils.easypost import get_easypost_client

logger = structlog.get_logger(__name__)


class CreateTrackerActivityInput(BaseModel):
    lead_id: str = Field(..., description="Close lead identifier.")


class CreateTrackerActivityResult(BaseModel):
    tracker_id: str = Field(..., description="EasyPost tracker ID.")


class UpdateCloseLeadActivityInput(BaseModel):
    lead_id: str = Field(..., description="Close lead identifier.")
    tracker_id: str = Field(..., description="EasyPost tracker identifier.")


@activity.defn
def create_tracker_activity(
    input: CreateTrackerActivityInput,
) -> CreateTrackerActivityResult:
    """Creates a tracker on EasyPost for the provided lead."""
    response = make_close_request(
        "get",
        f"https://api.close.com/api/v1/lead/{input.lead_id}",
    )

    if response.status_code != 200:
        raise ValueError(f"Failed to fetch lead data: {response.text}")

    lead_data = response.json()

    tracking_number = lead_data.get(
        "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
    )
    carrier_field = lead_data.get(
        "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l"
    )

    if not tracking_number or not carrier_field:
        _send_error_email_missing_data(lead_id=input.lead_id, workflow_id=activity.info().workflow_id, lead_data=lead_data)
        raise ValueError("Lead doesn't have tracking number or carrier")

    carrier = carrier_field[0] if isinstance(carrier_field, list) else carrier_field

    try:
        client = get_easypost_client(tracking_number)
        tracker = client.tracker.create(tracking_code=tracking_number, carrier=carrier)
    except Exception as exc:  # pragma: no cover - defensive
        _send_error_email_create_tracker_failed(workflow_id=activity.info().workflow_id, 
                                                lead_data=lead_data, 
                                                tracking_number=tracking_number, 
                                                carrier=carrier, 
                                                error=exc)
        raise ValueError(f"Failed to create tracker for lead {lead_data['id']} with tracking number {tracking_number} and carrier {carrier} : {exc}")

    return CreateTrackerActivityResult(tracker_id=tracker.id)


def _send_error_email_missing_data(lead_id: str, workflow_id: str, lead_data: dict[str, Any]) -> None:
    detailed_error_message = f"""
        <h2>EasyPost Tracker Missing Data</h2>
        <p><strong>Error:</strong> Lead doesn't have tracking number or carrier</p>
        <p><strong>Lead ID:</strong> {lead_id}</p>
        <p><strong>Route:</strong> /easypost/create_tracker</p>
        <p><strong>Workflow ID:</strong> {workflow_id}</p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Lead Data:</h3>
        <pre>{json.dumps(lead_data, indent=2, default=str)}</pre>
        """
    send_email(subject="EasyPost Tracker Missing Data", body=detailed_error_message)


def _send_error_email_create_tracker_failed(workflow_id: str, lead_data: dict[str, Any], tracking_number: str, carrier: str, error: Exception) -> None:
    detailed_error_message = f"""
        <h2>EasyPost Tracker Creation Failed</h2>
        <p><strong>Lead ID:</strong> {lead_data['id']}</p>
        <p><strong>Tracking Number:</strong> {tracking_number}</p>
        <p><strong>Carrier:</strong> {carrier}</p>
        <p><strong>Route:</strong> /easypost/create_tracker</p>
        <p><strong>Workflow ID:</strong> {workflow_id}</p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Lead Data:</h3>
        <pre>{json.dumps(lead_data, indent=2, default=str)}</pre>

        <h3>Error:</h3>
        <pre>{str(error)}</pre>
        """
    send_email(subject="EasyPost Tracker Creation Failed", body=detailed_error_message)


@activity.defn
def update_close_lead_activity(input: UpdateCloseLeadActivityInput) -> None:

    def verify_delivery_information_updated(response_data, lead_update_data):
        for key, value in lead_update_data.items():
            if key not in response_data or response_data[key] != value:
                return False
        return True

    custom_field_ids = {
        "easypost_tracker_id": {
            "type": "text",
            "value": "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh",
        }
    }

    lead_update_data = {
        custom_field_ids["easypost_tracker_id"]["value"]: input.tracker_id,
    }

    response = make_close_request(
        "put",
        f"https://api.close.com/api/v1/lead/{input.lead_id}",
        json=lead_update_data,
    )

    response_data = response.json()
    data_updated = verify_delivery_information_updated(response_data, lead_update_data)

    if not data_updated:
        error_message = f"EasyPost tracker ID update failed for lead {input.lead_id}."
        send_email(subject="EasyPost tracker ID update failed", body=error_message)
        raise ValueError(error_message)
