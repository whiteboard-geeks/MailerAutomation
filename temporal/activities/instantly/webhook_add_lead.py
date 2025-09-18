from temporalio import activity

from pydantic import BaseModel, Field
from close_utils import get_lead_by_id
from utils.email import send_email
from utils.instantly import add_to_instantly_campaign, campaign_exists, split_name


# Ugly: copied from blueprints.instantly
BARBARA_USER_ID = "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as"


class AddLeadToInstantlyCampaignArgs(BaseModel):
    lead_id: str
    campaign_name: str
    task_text: str


class AddLeadPayloadData(BaseModel):
    id: str = Field(..., description="ID of the lead")
    text: str = Field(..., description="Task name")
    lead_id: str = Field(..., description="ID of the lead")


class WebhookAddLeadPayloadEvent(BaseModel):
    action: str = Field(..., description="Action performed")
    object_type: str = Field(..., description="Type of object")
    data: AddLeadPayloadData = Field(..., description="Data of the object")


class WebhookAddLeadPayloadValidated(BaseModel):
    event: WebhookAddLeadPayloadEvent = Field(..., description="Event data")


class LeadDetails(BaseModel):
    email: str
    first_name: str
    last_name: str
    company_name: str
    date_location: str


class EmailNotFoundError(Exception):
    pass


@activity.defn
def add_lead_to_instantly_campaign(args: AddLeadToInstantlyCampaignArgs):
    campaign_check = campaign_exists(args.campaign_name)

    if not campaign_check.get("exists"):
        _send_error_email_campaign_not_found(campaign_name=args.campaign_name,
                                             lead_id=args.lead_id,
                                             task_text=args.task_text,
                                             workflow_id=activity.info().workflow_id)
        raise ValueError(f"Campaign '{args.campaign_name}' does not exist in Instantly")

    campaign_id = campaign_check.get("campaign_id")
    try:
        lead_details = _get_lead_details_from_close(lead_id=args.lead_id)
    except EmailNotFoundError:
        _send_error_email_lead_email_not_found(lead_id=args.lead_id)
        raise ValueError(f"No email found for lead ID: {args.lead_id}")

    if not lead_details:
        _send_error_email_lead_not_found(lead_id=args.lead_id)
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
        _send_error_email_instantly_api_error(instantly_result_message=instantly_result.get("message"))
        raise ValueError(f"Failed to add lead to Instantly: {instantly_result.get('message')}")


def _send_error_email_campaign_not_found(campaign_name: str, lead_id: str, task_text: str, workflow_id: str):
    email_subject = f"Instantly Campaign Not Found: {campaign_name}"
    close_lead_url = f"https://app.close.com/lead/{lead_id}/"
    error_msg = f"Campaign '{campaign_name}' does not exist in Instantly"
    email_body = f"""
Error: Campaign not found in Instantly (Async Processing)

Lead ID: {lead_id}
Lead URL: {close_lead_url}
Task Text: {task_text}
Campaign Name (extracted): {campaign_name}
Workflow ID: {workflow_id}

The campaign name could not be found in Instantly. Please verify the campaign exists or check the task text format.

Error details: {error_msg}
            """
    send_email(subject=email_subject, body=email_body)


def _send_error_email_lead_not_found(lead_id: str):
    error_msg = f"Could not retrieve lead details for lead ID: {lead_id}"
    send_email(subject="Close Lead Details Error (Async)", body=error_msg)


def _send_error_email_lead_email_not_found(lead_id: str):
    error_msg = f"No email found for lead ID: {lead_id}"
    send_email(subject="Close Lead Email Error (Async)", body=error_msg)


def _send_error_email_instantly_api_error(instantly_result_message: str | None):
    error_msg = (f"Failed to add lead to Instantly: {instantly_result_message}")
    send_email(subject="Instantly API Error (Async)", body=error_msg)


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

    company_name = lead_details.get("custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o", "")
    date_location = lead_details.get("custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9", "")

    return LeadDetails(
        email=email,
        first_name=first_name,
        last_name=last_name,
        company_name=company_name,
        date_location=date_location,
    )
