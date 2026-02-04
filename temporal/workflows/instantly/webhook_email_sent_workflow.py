from datetime import timedelta
import json
from typing import Any

from pydantic import BaseModel, Field
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from config import (
    MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL, 
    TEMPORAL_WORKFLOW_UI_BASE_URL, 
    TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS,
)
from temporal.shared import WAITING_FOR_RESUME_KEY_STR
from utils.email import send_email

with workflow.unsafe.imports_passed_through():
    from temporal.activities.instantly.webhook_email_sent import (
        AddEmailActivityToLeadArgs,
        CompleteLeadTaskByEmailArgs,
        CompleteLeadTaskByEmailResult,
        WebhookEmailSentPaylodValidated,
        add_email_activity_to_lead, 
        complete_lead_task_by_email)


class WebhookEmailSentPayload(BaseModel):
    json_payload: dict = Field(..., description="JSON payload of the request")


@workflow.defn
class WebhookEmailSentWorkflow:
    def __init__(self) -> None:
        self._data_issue_fixed: bool = True
        self._activity_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            maximum_attempts=TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS,
        )

    @workflow.signal
    def data_issue_fixed(self) -> None:
        """Signals that data have been manually fixed and that the workflow should retry."""
        self._data_issue_fixed = True

    @workflow.run
    async def run(self, input: WebhookEmailSentPayload) -> None:
        input_validated = self._validate_input(input)

        complete_lead_task_result = await self._complete_lead_task_by_email(input_validated)

        add_email_activity_to_lead_args = AddEmailActivityToLeadArgs(
            lead_id=complete_lead_task_result.lead_id,
            lead_email=input_validated.lead_email,
            timestamp=input_validated.timestamp,
            email_subject=input_validated.email_subject,
            email_account=input_validated.email_account,
            email_html=input_validated.email_html,
        )

        await self._add_email_activity_to_lead(add_email_activity_to_lead_args)

    async def _complete_lead_task_by_email(self, input: WebhookEmailSentPaylodValidated) -> CompleteLeadTaskByEmailResult:
        while True:
            try:
                return await workflow.execute_activity(
                    complete_lead_task_by_email,
                    CompleteLeadTaskByEmailArgs(lead_email=input.lead_email, campaign_name=input.campaign_name),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=self._activity_retry_policy,
                )
            except Exception:
                await self._wait_for_signal_data_issue_fixed()

    async def _add_email_activity_to_lead(self, input: AddEmailActivityToLeadArgs) -> None:
        while True:
            try:
                await workflow.execute_activity(
                    add_email_activity_to_lead,
                    input,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=self._activity_retry_policy,
                )
                return
            except Exception:
                await self._wait_for_signal_data_issue_fixed()

    async def _wait_for_signal_data_issue_fixed(self) -> None:
        self._data_issue_fixed = False
        workflow.upsert_search_attributes({WAITING_FOR_RESUME_KEY_STR: [True]})
        await workflow.wait_condition(lambda: self._data_issue_fixed)
        workflow.upsert_search_attributes({WAITING_FOR_RESUME_KEY_STR: [False]})

    @staticmethod
    def _validate_input(input: WebhookEmailSentPayload) -> WebhookEmailSentPaylodValidated:
        try:
            input_validated = WebhookEmailSentPaylodValidated(
                event_type=input.json_payload["event_type"],
                campaign_name=input.json_payload["campaign_name"],
                lead_email=input.json_payload["lead_email"],
                email_subject=input.json_payload["email_subject"],
                email_html=input.json_payload["email_html"],
                timestamp=input.json_payload["timestamp"],
                email_account=input.json_payload["email_account"],
            )

        except Exception as e:
            _send_error_email_validation_error(workflow_id=workflow.info().workflow_id, 
                                               json_payload=input.json_payload)
            raise ApplicationError(f"Invalid payload for email sent webhook: {e}") from e

        if input_validated.event_type != "email_sent":
            _send_error_email_event_type_not_email_sent(workflow_id=workflow.info().workflow_id,
                                                       event_type=input_validated.event_type)
            raise ApplicationError(f"Expected email_sent event, got {input_validated.event_type}")

        return input_validated


def _send_error_email_validation_error(workflow_id: str, json_payload: dict[str, Any]) -> None:
    detailed_error_message = f"""
        <h2>Email Sent Workflow: Payload Validation Error</h2>
        <p><strong>Error:</strong> Payload validation failed</p>
        <p><strong>Route:</strong> /instantly/email_sent</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {workflow.now().isoformat()}</p>
        
        <h3>JSON Payload:</h3>
        <pre>{json.dumps(json_payload, indent=2, default=str)}</pre>
        """
    send_email(subject="Email Sent Workflow: Payload Validation Error",
               body=detailed_error_message)


def _send_error_email_event_type_not_email_sent(workflow_id: str, event_type: str) -> None:
    detailed_error_message = f"""
        <h2>Email Sent Workflow: event_type!="email_sent" in payload received from Instantly</h2>
        <p><strong>Error:</strong> Expected event_type="email_sent", got "{event_type}"</p>
        <p><strong>Route:</strong> /instantly/email_sent</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {workflow.now().isoformat()}</p>
        """
    send_email(subject="Email Sent Workflow: Event Type Not Email Sent",
               body=detailed_error_message)
