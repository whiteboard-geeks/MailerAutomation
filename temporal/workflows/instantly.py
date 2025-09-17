from datetime import timedelta
from pydantic import BaseModel, Field
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from temporal.activities.instantly import (
        AddEmailActivityToLeadArgs, 
        AddLeadToInstantlyCampaignArgs, 
        CompleteLeadTaskByEmailArgs, 
        CompleteLeadTaskByEmailResult, 
        add_email_activity_to_lead, 
        add_lead_to_instantly_campaign, 
        complete_lead_task_by_email)
    from utils.instantly import get_instantly_campaign_name

from temporal.shared import WAITING_FOR_RESUME_KEY_STR


class WebhookEmailSentPaylodValidated(BaseModel):
    event_type: str = Field(..., description="Type of event")
    campaign_name: str = Field(..., description="Name of the campaign")
    lead_email: str = Field(..., description="Email of the lead")
    email_subject: str = Field(..., description="Subject of the email")
    email_html: str = Field(..., description="HTML content of the email")
    timestamp: str = Field(..., description="Timestamp of the event")
    email_account: str = Field(..., description="Email account used to send the email")


class WebhookEmailSentPayload(BaseModel):
    json_payload: dict = Field(..., description="JSON payload of the request")


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


class WebhookAddLeadPayload(BaseModel):
    json_payload: dict = Field(..., description="JSON payload of the request")


@workflow.defn
class WebhookEmailSentWorkflow:
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
            raise ApplicationError(f"Invalid payload for email sent webhook: {e}") from e
        
        if input_validated.event_type != "email_sent":
            raise ApplicationError(f"Expected email_sent event, got {input_validated.event_type}")

        return input_validated


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
            raise ApplicationError(f"Invalid payload for add lead webhook: {e}") from e
        
        if input_validated.event.action != "created":
            raise ApplicationError(f"Expected created action, got {input_validated.event.action}")
        
        if input_validated.event.object_type != "task.lead":
            raise ApplicationError(f"Expected task.lead object, got {input_validated.event.object_type}")
    
        if not input_validated.event.data.text.lower().startswith("instantly:"):
            raise ApplicationError(f"Expected task to start with Instantly:, got {input_validated.event.data.text}")

        return input_validated

    async def _add_lead_to_instantly_campaign(self, input: AddLeadToInstantlyCampaignArgs) -> None:
        while True:
            try:
                await workflow.execute_activity(
                    add_lead_to_instantly_campaign,
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
