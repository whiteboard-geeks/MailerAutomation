from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from temporalio import activity

from close_utils import (
    create_email_search_query,
    get_lead_by_id,
    get_sequence_subscriptions,
    make_close_request,
    pause_sequence_subscription,
    search_close_leads,
)
from config import (
    CLOSE_CRM_UI_LEAD_BASE_URL,
    MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL,
    TEMPORAL_WORKFLOW_UI_BASE_URL,
)
from temporal.activities.instantly.webhook_add_lead import BARBARA_USER_ID
from temporal.shared import is_last_attempt
from utils.email import send_email
from utils.instantly_reply_received import determine_notification_recipients


class WebhookReplyReceivedPayloadValidated(BaseModel):
    event_type: str = Field(..., description="Type of Instantly webhook event")
    lead_email: str = Field(..., description="Lead email address")
    campaign_name: str = Field(..., description="Instantly campaign name")
    reply_subject: str = Field(..., description="Reply subject")
    reply_text: Optional[str] = Field(None, description="Plain text reply body")
    reply_html: Optional[str] = Field(None, description="HTML reply body")
    timestamp: str = Field(..., description="Reply timestamp")
    email_account: str = Field(..., description="Instantly sending account")


class AddEmailActivityToLeadArgs(BaseModel):
    payload: WebhookReplyReceivedPayloadValidated


class AddEmailActivityToLeadResult(BaseModel):
    lead_id: str
    lead_email: str
    lead_name: str
    lead_details: Dict[str, Any]
    email_activity_id: str


class PauseSequenceSubscriptionsArgs(BaseModel):
    lead_id: str
    lead_email: str


class PauseSequenceSubscriptionsResult(BaseModel):
    paused_subscriptions: List[Dict[str, Any]]


class SendNotificationEmailArgs(BaseModel):
    lead_id: str
    lead_email: str
    lead_name: str
    campaign_name: str
    reply_subject: str
    reply_text: Optional[str]
    reply_html: Optional[str]
    env_type: str
    paused_subscriptions: List[Dict[str, Any]]
    lead_details: Dict[str, Any]
    email_activity_id: str


class SendNotificationEmailResult(BaseModel):
    notification_status: str
    custom_recipients_used: bool


@activity.defn(name="reply_received_add_email_activity_to_lead")
def add_email_activity_to_lead(
    args: AddEmailActivityToLeadArgs,
) -> AddEmailActivityToLeadResult:
    payload = args.payload

    query = create_email_search_query(payload.lead_email)
    leads = search_close_leads(query)

    if not leads:
        if is_last_attempt(activity.info()):
            _send_error_email_no_lead_found(
                workflow_id=activity.info().workflow_id, lead_email=payload.lead_email
            )
        raise ValueError(f"No lead found with email: {payload.lead_email}")

    if len(leads) > 1:
        if is_last_attempt(activity.info()):
            _send_error_email_multiple_leads_found(
                workflow_id=activity.info().workflow_id,
                lead_email=payload.lead_email,
                leads=leads,
            )
        raise ValueError(f"Multiple leads found with email: {payload.lead_email}")

    lead = leads[0]
    lead_id = lead["id"]
    activity.logger.info("lead_id = %s lead_email = %s", lead_id, payload.lead_email)

    lead_details = get_lead_by_id(lead_id)
    if not lead_details:
        if is_last_attempt(activity.info()):
            _send_error_email_no_lead_details_found(
                workflow_id=activity.info().workflow_id, lead_id=lead_id
            )
        raise ValueError(f"Could not retrieve lead details for lead ID: {lead_id}")

    contact = None
    target_email = (payload.lead_email or "").strip().lower()
    for contact_candidate in lead_details.get("contacts", []):
        for email_entry in contact_candidate.get("emails", []):
            email_value = (
                (email_entry.get("email") or email_entry.get("address") or "")
                .strip()
                .lower()
            )
            if email_value == target_email and target_email:
                contact = contact_candidate
                break
        if contact:
            break

    if not contact:
        contact_debug = [
            {
                "contact_id": c.get("id"),
                "emails": [
                    (e.get("email") or e.get("address") or "").strip()
                    for e in c.get("emails", [])
                ],
            }
            for c in lead_details.get("contacts", [])
        ]
        activity.logger.error(
            "contact_lookup_failed lead_id=%s lead_email=%s contacts=%s",
            lead_id,
            payload.lead_email,
            contact_debug,
        )
        if is_last_attempt(activity.info()):
            _send_error_email_no_contact_found(
                workflow_id=activity.info().workflow_id,
                lead_id=lead_id,
                lead_email=payload.lead_email,
            )
        raise ValueError(f"No contact found with email: {payload.lead_email}")

    email_data = {
        "contact_id": contact["id"],
        "user_id": BARBARA_USER_ID,
        "lead_id": lead_id,
        "direction": "incoming",
        "created_by": None,
        "date_created": payload.timestamp.replace("Z", "+00:00").replace("T", "T"),
        "subject": payload.reply_subject,
        "sender": payload.lead_email,
        "to": [payload.email_account],
        "bcc": [],
        "cc": [],
        "status": "inbox",
        "body_text": payload.reply_text or "",
        "body_html": payload.reply_html or "",
        "attachments": [],
        "template_id": None,
    }

    email_url = "https://api.close.com/api/v1/activity/email/"
    email_response = make_close_request("post", email_url, json=email_data)
    email_activity_id = email_response.json().get("id")

    lead_name = lead_details.get("name", "Unknown")

    return AddEmailActivityToLeadResult(
        lead_id=lead_id,
        lead_email=payload.lead_email,
        lead_name=lead_name,
        lead_details=lead_details,
        email_activity_id=email_activity_id,
    )


def _send_error_email_no_lead_found(workflow_id: str, lead_email: str) -> None:
    detailed_error_message = f"""
        <h2>Reply Received Workflow: No Lead Found for Email</h2>
        <p><strong>Error:</strong> No lead found for email {lead_email}</p>
        <p><strong>Route:</strong> /instantly/reply_received</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(
        subject="Reply Received Workflow: No Lead Found for Email",
        body=detailed_error_message,
    )


def _send_error_email_multiple_leads_found(
    workflow_id: str, lead_email: str, leads: list[dict]
) -> None:
    detailed_error_message = f"""
        <h2>Reply Received Workflow: Multiple Leads Found for Email</h2>
        <p><strong>Error:</strong> Multiple leads found for email {lead_email}</p>
        <p><strong>Route:</strong> /instantly/reply_received</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>

        <h3>Leads Found:</h3>
        <pre>{json.dumps(leads, indent=2, default=str)}</pre>
        """
    send_email(
        subject="Reply Received Workflow: Multiple Leads Found for Email",
        body=detailed_error_message,
    )


def _send_error_email_no_lead_details_found(workflow_id: str, lead_id: str) -> None:
    detailed_error_message = f"""
        <h2>Reply Received Workflow: No Lead Details Found for Lead in Close</h2>
        <p><strong>Error:</strong> No lead details found for lead ID: <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/reply_received</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(
        subject="Reply Received Workflow: No Lead Details Found for Lead in Close",
        body=detailed_error_message,
    )


def _send_error_email_no_contact_found(
    workflow_id: str, lead_id: str, lead_email: str
) -> None:
    detailed_error_message = f"""
        <h2>Reply Received Workflow: No Contact Found for Lead in Close</h2>
        <p><strong>Error:</strong> No contact found for lead ID: <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a> with email {lead_email}</p>
        <p><strong>Route:</strong> /instantly/reply_received</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(
        subject="Reply Received Workflow: No Contact Found for Lead in Close",
        body=detailed_error_message,
    )


@activity.defn(name="reply_received_pause_sequence_subscriptions")
def pause_sequence_subscriptions(
    args: PauseSequenceSubscriptionsArgs,
) -> PauseSequenceSubscriptionsResult:
    subscriptions = get_sequence_subscriptions(lead_id=args.lead_id)
    paused: List[Dict[str, Any]] = []

    for subscription in subscriptions:
        if subscription.get("status") != "active":
            continue

        subscription_id = subscription.get("id")
        result = pause_sequence_subscription(
            subscription_id,
            status_reason="replied",
        )

        if not result:
            continue

        paused.append(
            {
                "subscription_id": subscription_id,
                "sequence_id": subscription.get("sequence_id"),
                "sequence_name": subscription.get("sequence_name", "Unknown"),
            }
        )
        activity.logger.info(
            "sequence_paused subscription_id=%s lead_id=%s lead_email=%s",
            subscription_id,
            args.lead_id,
            args.lead_email,
        )

    return PauseSequenceSubscriptionsResult(paused_subscriptions=paused)


@activity.defn(name="reply_received_send_notification_email")
def send_notification_email(
    args: SendNotificationEmailArgs,
) -> SendNotificationEmailResult:
    env_type = args.env_type
    reply_html = args.reply_html
    reply_text = args.reply_text

    custom_recipients, consultant_error = determine_notification_recipients(
        args.lead_details, env_type
    )

    if consultant_error:
        raise ValueError(consultant_error)

    timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    notification_html = f"""
        <h2>Instantly Email Reply Received</h2>
        <p>A reply has been received from an Instantly email campaign.</p>
        
        <h3>Details:</h3>
        <ul>
            <li><strong>Lead:</strong> {args.lead_name}</li>
            <li><strong>Lead Email:</strong> {args.lead_email}</li>
            <li><strong>Campaign:</strong> {args.campaign_name}</li>
            <li><strong>Subject:</strong> {args.reply_subject}</li>
            <li><strong>Environment:</strong> {env_type}</li>
            <li><strong>Time:</strong> {timestamp_now}</li>
        </ul>
        
        <h3>Reply Content:</h3>
        <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; background-color: #f9f9f9;">
            {reply_html or reply_text or "No content available"}
        </div>
    """

    if args.paused_subscriptions:
        notification_html += """
            <h3>Sequences Paused:</h3>
            <ul>
        """
        for sub in args.paused_subscriptions:
            notification_html += (
                f"<li>{sub.get('sequence_name', 'Unknown Sequence')} "
                f"(ID: {sub.get('sequence_id')})</li>"
            )
        notification_html += "</ul>"

    notification_html += (
        f'<p><a href="https://app.close.com/lead/{args.lead_id}/" '
        f'style="padding: 10px 15px; background-color: #4CAF50; color: white; '
        f'text-decoration: none; border-radius: 4px; display: inline-block; margin-top: 10px;">'
        f"View Lead in Close</a></p>"
    )

    text_content = (
        "Instantly Reply Received\n\n"
        f"Lead: {args.lead_name}\n"
        f"Email: {args.lead_email}\n"
        f"Campaign: {args.campaign_name}\n"
        f"Subject: {args.reply_subject}\n"
        f"Environment: {env_type}\n"
        f"Time: {timestamp_now}"
    )

    if args.paused_subscriptions:
        text_content += "\n\nSequences Paused:"
        for sub in args.paused_subscriptions:
            text_content += (
                f"\n- {sub.get('sequence_name', 'Unknown Sequence')} "
                f"(ID: {sub.get('sequence_id')})"
            )

    email_kwargs: Dict[str, Any] = {
        "subject": f"Instantly Reply: {args.reply_subject} from {args.lead_name}",
        "body": notification_html,
        "text_content": text_content,
    }

    if custom_recipients:
        email_kwargs["recipients"] = custom_recipients
        activity.logger.info(
            "using_custom_recipients lead_id=%s recipients=%s",
            args.lead_id,
            custom_recipients,
        )

    notification_status = "unknown"
    try:
        notification_result = send_email(**email_kwargs)
        notification_status = notification_result.get("status", "unknown")
        activity.logger.info(
            "notification_email_sent status=%s message_id=%s",
            notification_status,
            notification_result.get("message_id"),
        )
    except Exception as email_error:  # pragma: no cover - defensive logging
        activity.logger.error(
            "gmail_notification_failed error=%s",
            str(email_error),
        )
        notification_status = "error"

    return SendNotificationEmailResult(
        notification_status=notification_status,
        custom_recipients_used=bool(custom_recipients),
    )


__all__ = [
    "AddEmailActivityToLeadArgs",
    "AddEmailActivityToLeadResult",
    "PauseSequenceSubscriptionsArgs",
    "PauseSequenceSubscriptionsResult",
    "SendNotificationEmailArgs",
    "SendNotificationEmailResult",
    "WebhookReplyReceivedPayloadValidated",
    "add_email_activity_to_lead",
    "pause_sequence_subscriptions",
    "send_notification_email",
]
