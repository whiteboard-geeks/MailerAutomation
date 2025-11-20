from __future__ import annotations

from datetime import datetime
from enum import Enum
import json
from typing import Any

from pydantic import BaseModel, Field
from temporalio import activity

from close_utils import get_lead_by_id, load_query, search_close_leads, update_delivery_information_for_lead
from config import CLOSE_CRM_UI_LEAD_BASE_URL, MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL, TEMPORAL_WORKFLOW_UI_BASE_URL
from utils.easypost import create_package_delivered_custom_activity_in_close
from utils.email import send_email


class UpdateDeliveryInfoInput(BaseModel):
    tracking_code: str = Field(..., description="Tracking code of the package.")
    last_tracking_detail: TrackingDetail = Field(
        ..., description="Last tracking detail of the package."
    )


class UpdateDeliveryInfoResult(BaseModel):
    lead_id: str = Field(..., description="Close lead identifier.")


class TrackingDetail(BaseModel):
    tracking_location: TrackingLocation = Field(
        ..., description="Tracking location of the package."
    )
    datetime: str = Field(..., description="Datetime of the tracking detail.")

    @classmethod
    def new(cls, city: str | None, state: str | None, datetime: str):
        return cls(
            tracking_location=TrackingLocation(city=city, state=state),
            datetime=datetime,
        )


class TrackingLocation(BaseModel):
    city: str | None = Field(..., description="City of the tracking location.")
    state: str | None = Field(..., description="State of the tracking location.")


class CreatePackageDeliveredCustomInput(BaseModel):
    lead_id: str = Field(..., description="Close lead identifier.")
    last_tracking_detail: TrackingDetail = Field(
        ..., description="Last tracking detail of the package."
    )


class CreatePackageDeliveredCustomResult(BaseModel):
    class Status(str, Enum):
        SUCCESS = "success"
        SKIPPED = "skipped"

    status: Status = Field(..., description="Status of the activity creation.")


@activity.defn
def update_delivery_info_for_lead_activity(input: UpdateDeliveryInfoInput) -> UpdateDeliveryInfoResult:
    close_query_to_find_leads_with_tracking_number = load_query(
        "lead_by_tracking_number.json"
    )
    close_query_to_find_leads_with_tracking_number["query"]["queries"][1][
        "queries"
    ][0]["queries"][0]["condition"]["value"] = input.tracking_code

    try:
        close_leads : list[dict] = search_close_leads(close_query_to_find_leads_with_tracking_number)
    except Exception as e:
        _send_error_email_search_close_leads_failed(workflow_id=activity.info().workflow_id,
                                                    tracking_code=input.tracking_code,
                                                    error=e)
        raise ValueError(f"Failed to search Close leads: {e}") from e

    if len(close_leads) == 0:
        _send_error_email_no_leads_found(workflow_id=activity.info().workflow_id,
                                         tracking_code=input.tracking_code)
        raise ValueError(f"No leads found with tracking number {input.tracking_code}")

    if len(close_leads) > 1:
        valid_leads : list[dict] = []
        for lead in close_leads:
            lead_id = lead["id"]
            valid_lead = get_lead_by_id(lead_id)
            if valid_lead:
                valid_leads.append(lead)
        
        if len(valid_leads) == 1:
            close_leads = valid_leads
        elif len(valid_leads) > 1:
            _send_error_email_multiple_leads_found(workflow_id=activity.info().workflow_id,
                                                   tracking_code=input.tracking_code,
                                                   leads=valid_leads)
            raise ValueError(f"Multiple valid leads found with tracking number {input.tracking_code}: {valid_leads}")
        else:
            _send_error_email_no_valid_leads_found(workflow_id=activity.info().workflow_id,
                                                   tracking_code=input.tracking_code)
            raise ValueError(f"No valid leads found with tracking number {input.tracking_code}")
    else:
        valid_leads : list[dict] = []
        lead_id = close_leads[0]["id"]
        valid_lead = get_lead_by_id(lead_id)
        if valid_lead:
            valid_leads.append(valid_lead)
        else:
            _send_error_email_lead_not_found(workflow_id=activity.info().workflow_id,
                                             tracking_code=input.tracking_code,
                                             lead_id=lead_id)
            raise ValueError(f"Lead {lead_id} is not a valid lead")
    
    if not valid_leads:
        _send_error_email_no_valid_leads_found(workflow_id=activity.info().workflow_id,
                                               tracking_code=input.tracking_code)
        raise ValueError(f"No valid leads found with tracking number {input.tracking_code}")
    
    lead_id : str = valid_leads[0]["id"]

    delivery_information = _parse_delivery_information(input.last_tracking_detail)

    try:
        update_delivery_information_for_lead(lead_id, delivery_information)
    except Exception as e:
        _send_error_email_lead_update_failed(workflow_id=activity.info().workflow_id,
                                             lead_id=lead_id,
                                             tracking_code=input.tracking_code,
                                             delivery_information=delivery_information,
                                             error=e)
        raise ValueError(f"Failed to update lead {lead_id}: {e}") from e
    
    return UpdateDeliveryInfoResult(lead_id=lead_id)


def _send_error_email_search_close_leads_failed(workflow_id: str, tracking_code: str, error: Exception) -> None:
    detailed_error_message = f"""
        <h2>Update Delivery Status: Search for Close Leads Failed</h2>
        <p><strong>Error:</strong> Failed to search for leads on Close with tracking number {tracking_code}</p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow ID:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Error:</h3>
        <pre>{str(error)}</pre>
        """
    send_email(subject="Update Delivery Status: Search for Close Leads Failed",
               body=detailed_error_message)


def _send_error_email_no_leads_found(workflow_id: str, tracking_code: str) -> None:
    detailed_error_message = f"""
        <h2>Update Delivery Status: No Leads Found</h2>
        <p><strong>Error:</strong> No leads found on Close with tracking number {tracking_code}</p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow ID:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(subject="Update Delivery Status: No Leads Found",
               body=detailed_error_message)


def _send_error_email_multiple_leads_found(workflow_id: str, tracking_code: str, leads: list[dict]) -> None:
    detailed_error_message = f"""
        <h2>Update Delivery Status: Multiple Leads Found</h2>
        <p><strong>Error:</strong> Multiple valid leads found with tracking number {tracking_code}</p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow ID:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Leads:</h3>
        <pre>{json.dumps(leads, indent=2, default=str)}</pre>
        """
    send_email(subject="Update Delivery Status: Multiple Leads Found",
               body=detailed_error_message)


def _send_error_email_no_valid_leads_found(workflow_id: str, tracking_code: str) -> None:
    detailed_error_message = f"""
        <h2>Update Delivery Status: No Valid Leads Found</h2>
        <p><strong>Error:</strong> No valid leads found with tracking number {tracking_code}</p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow ID:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(subject="Update Delivery Status: No Valid Leads Found",
               body=detailed_error_message)


def _send_error_email_lead_not_found(workflow_id: str, tracking_code: str, lead_id: str) -> None:
    detailed_error_message = f"""
        <h2>Update Delivery Status: Lead Not Found</h2>
        <p><strong>Error:</strong> Lead {lead_id} is not a valid lead</p>
        <p><strong>Tracking Code:</strong> {tracking_code}</p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow ID:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(subject="Update Delivery Status: Lead Not Found",
               body=detailed_error_message)


def _send_error_email_lead_update_failed(
    workflow_id: str,
    lead_id: str,
    tracking_code: str,
    delivery_information: dict[str, Any],
    error: Exception
) -> None:
    detailed_error_message = f"""
        <h2>Update Delivery Status: Lead Update Failed</h2>
        <p><strong>Error:</strong> Failed to update lead <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}">{lead_id}</a></p>
        <p><strong>Tracking Code:</strong> {tracking_code}</p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow ID:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Delivery Information that Failed to Update on Close:</h3>
        <pre>{json.dumps(delivery_information, indent=2, default=str)}</pre>

        <h3>Error:</h3>
        <pre>{str(error)}</pre>
        """
    send_email(subject="Update Delivery Status: Lead Update Failed",
               body=detailed_error_message)


@activity.defn
def create_package_delivered_custom_activity_in_close_activity(input: CreatePackageDeliveredCustomInput) -> CreatePackageDeliveredCustomResult:
    delivery_information = _parse_delivery_information(input.last_tracking_detail)

    try:
        resp = create_package_delivered_custom_activity_in_close(input.lead_id, delivery_information)
    except Exception as e:
        _send_error_email_creation_of_custom_activity_failed(workflow_id=activity.info().workflow_id,
                                                             lead_id= input.lead_id, 
                                                             delivery_information=delivery_information,
                                                             error=e)
        raise ValueError(f"Failed to create custom activity for lead {input.lead_id}: {e}") from e
    
    if resp.get("status") == "skipped" and resp.get("reason") == "duplicate_activity_exists":
        return CreatePackageDeliveredCustomResult(status=CreatePackageDeliveredCustomResult.Status.SKIPPED)
    else:
        return CreatePackageDeliveredCustomResult(status=CreatePackageDeliveredCustomResult.Status.SUCCESS)


def _parse_delivery_information(tracking_detail: TrackingDetail) -> dict[str, Any]:
    """Parse delivery information from tracking data."""
    delivery_information = {}
    delivery_information["delivery_city"] = tracking_detail.tracking_location.city.title()
    delivery_information["delivery_state"] = tracking_detail.tracking_location.state.upper()

    delivery_datetime = datetime.strptime(tracking_detail.datetime, "%Y-%m-%dT%H:%M:%SZ")

    delivery_information["delivery_date"] = delivery_datetime.date()
    delivery_information["delivery_date_readable"] = delivery_datetime.strftime(
        "%a %-m/%-d"
    )
    delivery_information["date_and_location_of_mailer_delivered"] = (
        f"{delivery_information['delivery_date_readable']} to {delivery_information['delivery_city']}, {delivery_information['delivery_state']}"
    )
    delivery_information["location_delivered"] = (
        f"{delivery_information['delivery_city']}, {delivery_information['delivery_state']}"
    )

    return delivery_information


def _send_error_email_creation_of_custom_activity_failed(
    workflow_id: str,
    lead_id: str,
    delivery_information: dict[str, Any],
    error: Exception
) -> None:
    detailed_error_message = f"""
        <h2>Update Delivery Status: Creation of Custom Activity on Close Failed</h2>
        <p><strong>Error:</strong> Failed to create custom activity for lead <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}">{lead_id}</a></p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow ID:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Delivery Information that Failed to Create Custom Activity on Close:</h3>
        <pre>{json.dumps(delivery_information, indent=2, default=str)}</pre>

        <h3>Error:</h3>
        <pre>{str(error)}</pre>
        """
    send_email(subject="Update Delivery Status: Creation of Custom Activity Failed",
               body=detailed_error_message)
