"""Shared notification activity for workflow-level failure reporting.

Most error emails are sent from inside the activity that failed, guarded by
``is_last_attempt``. That path cannot cover start-to-close timeouts: Temporal tears the
activity down mid-execution, so nothing in the activity body runs.

The workflow therefore has to report those itself -- but a bare ``send_email`` call in
workflow code re-executes on every replay, which would fan out duplicate emails every
time a parked workflow is resumed by signal. Routing it through an activity records the
send in workflow history, so it happens exactly once.
"""

from datetime import datetime

from pydantic import BaseModel, Field
from temporalio import activity

from config import (
    CLOSE_CRM_UI_LEAD_BASE_URL,
    MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL,
    TEMPORAL_WORKFLOW_UI_BASE_URL,
)
from utils.email import send_email


class ActivityTimeoutAlertArgs(BaseModel):
    workflow_id: str = Field(..., description="Temporal workflow id")
    workflow_type: str = Field(..., description="Temporal workflow type name")
    activity_name: str = Field(..., description="Activity that timed out")
    route: str = Field(..., description="Webhook route that started the workflow")
    timeout_seconds: int = Field(..., description="start_to_close timeout in seconds")
    max_attempts: int = Field(..., description="Retry attempts that were exhausted")
    lead_email: str | None = Field(None, description="Lead email, when known")
    campaign_name: str | None = Field(None, description="Campaign name, when known")
    lead_id: str | None = Field(None, description="Close lead id, when known")


@activity.defn
def send_activity_timeout_alert(args: ActivityTimeoutAlertArgs) -> None:
    """Email the sales/ops team that an activity exhausted its retries by timing out."""
    lead_line = ""
    if args.lead_id:
        lead_line = (
            f'<p><strong>Lead:</strong> <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/'
            f'{args.lead_id}/">{args.lead_id}</a></p>'
        )

    detailed_error_message = f"""
        <h2>{args.workflow_type}: Activity Timed Out</h2>
        <p><strong>Error:</strong> Activity <code>{args.activity_name}</code> exhausted
           {args.max_attempts} attempts, each timing out after
           {args.timeout_seconds}s (start-to-close).</p>
        <p><strong>Impact:</strong> The workflow is paused and waiting for a
           <code>data_issue_fixed</code> signal. Whatever this activity does (e.g.
           completing the Close task) has NOT reliably happened.</p>
        <p><strong>Lead Email:</strong> {args.lead_email or "unknown"}</p>
        <p><strong>Campaign:</strong> {args.campaign_name or "unknown"}</p>
        {lead_line}
        <p><strong>Route:</strong> {args.route}</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{args.workflow_id}">{args.workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(
        subject=f"{args.workflow_type}: Activity Timed Out ({args.activity_name})",
        body=detailed_error_message,
    )
