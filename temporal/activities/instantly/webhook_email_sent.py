from pydantic import BaseModel, Field
from temporalio import activity
from close_utils import create_email_search_query, get_lead_by_id, make_close_request, search_close_leads
from temporal.activities.instantly.webhook_add_lead import BARBARA_USER_ID


class CompleteLeadTaskByEmailArgs(BaseModel):
    lead_email: str
    campaign_name: str


class CompleteLeadTaskByEmailResult(BaseModel):
    lead_id: str




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


class AddEmailActivityToLeadArgs(BaseModel):
    lead_id: str
    lead_email: str
    timestamp: str
    email_subject: str
    email_account: str
    email_html: str


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


class WebhookEmailSentPaylodValidated(BaseModel):
    event_type: str = Field(..., description="Type of event")
    campaign_name: str = Field(..., description="Name of the campaign")
    lead_email: str = Field(..., description="Email of the lead")
    email_subject: str = Field(..., description="Subject of the email")
    email_html: str = Field(..., description="HTML content of the email")
    timestamp: str = Field(..., description="Timestamp of the event")
    email_account: str = Field(..., description="Email account used to send the email")


