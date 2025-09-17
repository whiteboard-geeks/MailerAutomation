from pydantic import BaseModel
from temporalio import activity

from close_utils import create_email_search_query, get_lead_by_id, make_close_request, search_close_leads
from utils.email import send_email
from utils.instantly import add_to_instantly_campaign, campaign_exists, split_name

# Ugly: copied from blueprints.instantly
BARBARA_USER_ID = "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as"


class CompleteLeadTaskByEmailArgs(BaseModel):
    lead_email: str
    campaign_name: str


class CompleteLeadTaskByEmailResult(BaseModel):
    lead_id: str


class AddEmailActivityToLeadArgs(BaseModel):
    lead_id: str
    lead_email: str
    timestamp: str
    email_subject: str
    email_account: str
    email_html: str


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
def complete_lead_task_by_email(args: CompleteLeadTaskByEmailArgs) -> CompleteLeadTaskByEmailResult:
    """Mark task as complete in Close CRM.
    
    Args:
        args (CompleteLeadTaskByEmailArgs): Email address of lead

    Returns:
        CompleteLeadTaskByEmailResult: Lead ID
    """
    query = create_email_search_query(args.lead_email)
    leads = search_close_leads(query)

    if len(leads) != 1:
        raise ValueError(f"Expected 1 lead, got {len(leads)}")

    lead_id = leads[0]["id"]
    activity.logger.info("lead_id = %s", lead_id)

    # Get all tasks for the lead
    tasks_url = f"https://api.close.com/api/v1/task/?lead_id={lead_id}"
    tasks_response = make_close_request("get", tasks_url)
    tasks = tasks_response.json().get("data", [])

    activity.logger.info("close_crm_task_count = %d", len(tasks))

    # Find the matching task
    matching_task = None
    for task in tasks:
        if args.campaign_name in task.get("text", "") and not task.get("is_complete"):
            matching_task = task
            break
    
    if not matching_task:
        raise ValueError(f"Could not find task for campaign {args.campaign_name}")

    # Mark the task as complete
    close_task_id = matching_task["id"]
    activity.logger.info("close_crm_task_id = %s", close_task_id)
    complete_url = f"https://api.close.com/api/v1/task/{close_task_id}/"
    complete_data = {"is_complete": True}
    make_close_request("put", complete_url, json=complete_data)

    return CompleteLeadTaskByEmailResult(lead_id=lead_id)


@activity.defn
def add_email_activity_to_lead(args: AddEmailActivityToLeadArgs):
    lead_details = get_lead_by_id(args.lead_id)
    if not lead_details:
        raise ValueError(f"Could not retrieve lead details for lead ID: {args.lead_id}")

    contact = None
    for c in lead_details.get("contacts", []):
        for email in c.get("emails", []):
            if email.get("email") == args.lead_email:
                contact = c
                break
        if contact:
            break
    
    if not contact:
        raise ValueError(f"No contact found with email: {args.lead_email}")

    # Create email activity in Close
    email_data = {
        "contact_id": contact["id"],
        "user_id": BARBARA_USER_ID,
        "lead_id": args.lead_id,
        "direction": "outgoing",
        "created_by": BARBARA_USER_ID,
        "created_by_name": "Barbara Pigg",  # Hardcoded since we know it's Barbara
        "date_created": args.timestamp
        .replace("Z", "+00:00")
        .replace("T", "T"),
        "subject": args.email_subject,
        "sender": args.email_account,
        "to": [args.lead_email],
        "bcc": [],
        "cc": [],
        "status": "sent",
        "body_text": "",  # We don't have plain text version
        "body_html": args.email_html,
        "attachments": [],
        "template_id": None,
    }

    email_url = "https://api.close.com/api/v1/activity/email/"
    make_close_request("post", email_url, json=email_data)


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
