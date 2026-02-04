from __future__ import annotations
from datetime import datetime

from temporalio import activity

from pydantic import BaseModel, Field
from close_utils import get_lead_by_id
from config import (
    CLOSE_CRM_UI_LEAD_BASE_URL,
    MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL,
    TEMPORAL_WORKFLOW_UI_BASE_URL,
    TEST_CAMPAIGN_NAME,
)
from temporal.shared import is_last_attempt
from utils.email import send_email
from utils.instantly import add_to_instantly_campaign, campaign_exists, split_name


# Ugly: copied from blueprints.instantly
BARBARA_USER_ID = "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as"


class WebhookAddLeadPayloadValidated(BaseModel):
    event: WebhookAddLeadPayloadEvent = Field(..., description="Event data")


class WebhookAddLeadPayloadEvent(BaseModel):
    action: str = Field(..., description="Action performed")
    object_type: str = Field(..., description="Type of object")
    data: AddLeadPayloadData = Field(..., description="Data of the object")


class AddLeadPayloadData(BaseModel):
    id: str = Field(..., description="ID of the lead")
    text: str = Field(..., description="Task name")
    lead_id: str = Field(..., description="ID of the lead")


class AddLeadToInstantlyCampaignArgs(BaseModel):
    lead_id: str
    campaign_name: str
    task_text: str


class LeadDetails(BaseModel):
    email: str
    first_name: str
    last_name: str
    company_name: str
    date_location: str


class EmailNotFoundError(Exception):
    pass


@activity.defn
def add_lead_to_instantly_campaign(args: AddLeadToInstantlyCampaignArgs) -> None:
    campaign_check = campaign_exists(args.campaign_name)

    if not campaign_check.get("exists"):
        if args.campaign_name == TEST_CAMPAIGN_NAME:
            activity.logger.info(
                f"Test campaign {TEST_CAMPAIGN_NAME} does not exist in Instantly. This is expected in non-production environments. Skipping sending error email."
            )
        else:
            if is_last_attempt(activity.info()):
                _send_error_email_campaign_not_found(
                    campaign_name=args.campaign_name,
                    lead_id=args.lead_id,
                    task_text=args.task_text,
                    workflow_id=activity.info().workflow_id,
                )
        raise ValueError(f"Campaign '{args.campaign_name}' does not exist in Instantly")

    campaign_id = campaign_check.get("campaign_id")
    try:
        lead_details = _get_lead_details_from_close(lead_id=args.lead_id)
    except EmailNotFoundError:
        if is_last_attempt(activity.info()):
            _send_error_email_lead_email_not_found(
                workflow_id=activity.info().workflow_id, lead_id=args.lead_id
            )
        raise ValueError(f"No email found for lead ID: {args.lead_id}")

    if not lead_details:
        if is_last_attempt(activity.info()):
            _send_error_email_no_lead_details_found(
                workflow_id=activity.info().workflow_id, lead_id=args.lead_id
            )
        raise ValueError(f"Could not retrieve lead details for lead ID: {args.lead_id}")

    instantly_result = add_to_instantly_campaign(
        campaign_id=campaign_id,
        email=lead_details.email,
        first_name=lead_details.first_name,
        last_name=lead_details.last_name,
        company_name=lead_details.company_name,
        date_location=lead_details.date_location,
    )

    if instantly_result.get("status") == "error":
        if is_last_attempt(activity.info()):
            error_message = instantly_result.get("message") or ""
            _send_error_email_instantly_api_error(
                workflow_id=activity.info().workflow_id,
                lead_id=args.lead_id,
                campaign_name=args.campaign_name,
                error_message=error_message,
            )
        raise ValueError(
            f"Failed to add lead to Instantly: {instantly_result.get('message')}"
        )


def _send_error_email_campaign_not_found(
    campaign_name: str, lead_id: str, task_text: str, workflow_id: str
) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: Campaign Not Found in Instantly</h2>
        <p><strong>Error:</strong> Campaign '{campaign_name}' does not exist in Instantly</p>
        <p><strong>Lead ID:</strong> <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Task Text (contains campaign name):</h3>
        <pre>{task_text}</pre>
        """
    send_email(
        subject="Add Lead Workflow: Campaign Not Found in Instantly",
        body=detailed_error_message,
    )


def _send_error_email_lead_email_not_found(workflow_id: str, lead_id: str) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: No Email Found for Lead in Close</h2>
        <p><strong>Error:</strong> No email found for lead ID: <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(
        subject="Add Lead Workflow: No Email Found for Lead in Close",
        body=detailed_error_message,
    )


def _send_error_email_no_lead_details_found(workflow_id: str, lead_id: str) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: No Lead Details Found for Lead in Close</h2>
        <p><strong>Error:</strong> No lead details found for lead ID: <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(
        subject="Add Lead Workflow: No Lead Details Found for Lead in Close",
        body=detailed_error_message,
    )


def _send_error_email_instantly_api_error(
    workflow_id: str, lead_id: str, campaign_name: str, error_message: str
) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: Error Adding Lead to Instantly</h2>
        <p><strong>Lead ID:</strong> <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Campaign Name:</strong> {campaign_name}</p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>

        <h3>Error Message from Instantly API:</h3>
        <pre>{error_message}</pre>
        """
    send_email(
        subject="Add Lead Workflow: Error Adding Lead to Instantly",
        body=detailed_error_message,
    )


def _get_lead_details_from_close(lead_id: str) -> LeadDetails | None:
    lead_details = get_lead_by_id(lead_id)
    if not lead_details:
        return None

    full_name = lead_details.get("contacts", [{}])[0].get("name", "")
    first_name, last_name = split_name(full_name)

    # Get contact email
    email = None
    contacts = lead_details.get("contacts", [])
    for contact in contacts:
        emails = contact.get("emails", [])
        if emails:
            email = emails[0].get("email")
            break

    if not email:
        raise EmailNotFoundError(f"No email found for lead ID: {lead_id}")

    company_name = lead_details.get(
        "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o", ""
    )
    date_location = lead_details.get(
        "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9", ""
    )

    return LeadDetails(
        email=email,
        first_name=first_name,
        last_name=last_name,
        company_name=company_name,
        date_location=date_location,
    )
