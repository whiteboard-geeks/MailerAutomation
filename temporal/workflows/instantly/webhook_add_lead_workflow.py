from datetime import timedelta

from pydantic import BaseModel, Field
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from temporal.shared import WAITING_FOR_RESUME_KEY_STR

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
