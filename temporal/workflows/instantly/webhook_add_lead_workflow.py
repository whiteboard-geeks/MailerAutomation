from datetime import datetime, timedelta
import json
from typing import Any

from pydantic import BaseModel, Field
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from config import CLOSE_CRM_UI_LEAD_BASE_URL, MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL, TEMPORAL_WORKFLOW_UI_BASE_URL
from temporal.shared import WAITING_FOR_RESUME_KEY_STR
from utils.email import send_email

with workflow.unsafe.imports_passed_through():
    from temporal.activities.instantly.webhook_add_lead import (
        AddLeadToInstantlyCampaignArgs,
        WebhookAddLeadPayloadValidated,
        add_lead_to_instantly_campaign)
    from utils.instantly import get_instantly_campaign_name


class WebhookAddLeadPayload(BaseModel):
    json_payload: dict = Field(..., description="JSON payload of the request")


@workflow.defn
class WebhookAddLeadWorkflow:
    def __init__(self) -> None:
        self._data_issue_fixed: bool = True
        self._activity_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            maximum_attempts=2,
        )

    @workflow.signal
    def data_issue_fixed(self) -> None:
        """Signals that data have been manually fixed and that the workflow should retry."""
        self._data_issue_fixed = True

    @workflow.run
    async def run(self, input: WebhookAddLeadPayload) -> None:
        input_validated = self._validate_input(input)

        campaign_name = get_instantly_campaign_name(input_validated.event.data.text)
        if not campaign_name:
            _send_error_email_campaign_name_not_found(workflow_id=workflow.info().workflow_id,
                                                      lead_id=input_validated.event.data.lead_id,
                                                      task_text=input_validated.event.data.text)
            raise ApplicationError(f"Could not extract campaign name from task: {input_validated.event.data.text}")

        add_lead_to_instantly_campaign_args = AddLeadToInstantlyCampaignArgs(
            lead_id=input_validated.event.data.lead_id,
            campaign_name=campaign_name,
            task_text=input_validated.event.data.text,
        )

        await self._add_lead_to_instantly_campaign(add_lead_to_instantly_campaign_args)

    @staticmethod
    def _validate_input(input: WebhookAddLeadPayload) -> WebhookAddLeadPayloadValidated:
        try:
            input_validated = WebhookAddLeadPayloadValidated(**input.json_payload)
        except Exception as e:
            _send_error_email_validation_error(workflow_id=workflow.info().workflow_id, 
                                               json_payload=input.json_payload)
            raise ApplicationError(f"Invalid payload for add lead webhook: {e}") from e

        if input_validated.event.action != "created":
            _send_error_email_action_not_created(workflow_id=workflow.info().workflow_id,
                                                 lead_id=input_validated.event.data.lead_id, 
                                                 action=input_validated.event.action)
            raise ApplicationError(f"Expected created action, got {input_validated.event.action}")

        if input_validated.event.object_type != "task.lead":
            _send_error_email_object_type_not_task_lead(workflow_id=workflow.info().workflow_id,
                                                        lead_id=input_validated.event.data.lead_id, 
                                                        object_type=input_validated.event.object_type)
            raise ApplicationError(f"Expected task.lead object, got {input_validated.event.object_type}")

        if not input_validated.event.data.text.lower().startswith("instantly:"):
            _send_error_email_task_does_not_start_with_instantly(workflow_id=workflow.info().workflow_id,
                                                                 lead_id=input_validated.event.data.lead_id, 
                                                                 task_text=input_validated.event.data.text)
            raise ApplicationError(f"Expected task to start with Instantly:, got {input_validated.event.data.text}")

        return input_validated

    async def _add_lead_to_instantly_campaign(self, input: AddLeadToInstantlyCampaignArgs) -> None:
        while True:
            try:
                await workflow.execute_activity(
                    add_lead_to_instantly_campaign,
                    input,
                    start_to_close_timeout=timedelta(seconds=60),
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


def _send_error_email_validation_error(workflow_id: str, json_payload: dict[str, Any]) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: Payload Validation Error</h2>
        <p><strong>Error:</strong> Payload validation failed</p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>JSON Payload:</h3>
        <pre>{json.dumps(json_payload, indent=2, default=str)}</pre>
        """
    send_email(subject="Add Lead Workflow: Payload Validation Error",
               body=detailed_error_message)


def _send_error_email_campaign_name_not_found(workflow_id: str, lead_id: str, task_text: str) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: Campaign Name Not Found</h2>
        <p><strong>Error:</strong> Campaign name not found in task text</p>
        <p><strong>Lead ID:</strong> <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Task Text:</h3>
        <pre>{task_text}</pre>
        """
    send_email(subject="Add Lead Workflow: Campaign Name Not Found",
               body=detailed_error_message)


def _send_error_email_action_not_created(workflow_id: str, lead_id: str, action: str) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: action!="created" in payload received from Close</h2>
        <p><strong>Error:</strong> Expected action="created", got "{action}"</p>
        <p><strong>Lead ID:</strong> <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(subject="Add Lead Workflow: Action Not Created",
               body=detailed_error_message)


def _send_error_email_object_type_not_task_lead(workflow_id: str, lead_id: str, object_type: str) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: object_type!="task.lead" in payload received from Close</h2>
        <p><strong>Error:</strong> Expected object_type="task.lead", got "{object_type}"</p>
        <p><strong>Lead ID:</strong> <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        """
    send_email(subject="Add Lead Workflow: Object Type Not Task Lead",
               body=detailed_error_message)


def _send_error_email_task_does_not_start_with_instantly(workflow_id: str, lead_id: str, task_text: str) -> None:
    detailed_error_message = f"""
        <h2>Add Lead Workflow: Task Does Not Start With "Instantly:"</h2>
        <p><strong>Error:</strong> Expected task to start with "Instantly:", got "{task_text}"</p>
        <p><strong>Lead ID:</strong> <a href="{CLOSE_CRM_UI_LEAD_BASE_URL}/{lead_id}/">{lead_id}</a></p>
        <p><strong>Route:</strong> /instantly/add_lead</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Task Text:</h3>
        <pre>{task_text}</pre>
        """
    send_email(subject="Add Lead Workflow: Task Does Not Start With Instantly:",
               body=detailed_error_message)
