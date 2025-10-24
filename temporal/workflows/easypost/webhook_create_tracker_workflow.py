from datetime import timedelta

from pydantic import BaseModel, ConfigDict, Field
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from temporal.shared import WAITING_FOR_RESUME_KEY_STR

with workflow.unsafe.imports_passed_through():
    from temporal.activities.easypost.webhook_create_tracker import (
        CreateTrackerActivityInput,
        CreateTrackerActivityResult,
        UpdateCloseLeadActivityInput,
        create_tracker_activity,
        update_close_lead_activity,
    )


def send_email(subject, body, **kwargs):
    """Send email notification."""
    from utils.email import send_email as app_send_email

    return app_send_email(subject, body, **kwargs)


class WebhookCreateTrackerPayload(BaseModel):
    json_payload: dict = Field(..., description="Original webhook payload.")


class WebhookCreateTrackerPayloadData(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(..., description="Close lead identifier.")


class WebhookCreateTrackerPayloadEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    data: WebhookCreateTrackerPayloadData = Field(
        ..., description="Event payload containing lead data."
    )


class WebhookCreateTrackerPayloadValidated(BaseModel):
    model_config = ConfigDict(extra="allow")

    event: WebhookCreateTrackerPayloadEvent = Field(
        ..., description="Validated Close webhook event."
    )


@workflow.defn
class WebhookCreateTrackerWorkflow:
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
    async def run(self, input: WebhookCreateTrackerPayload) -> None:
        input_validated = self._validate_input(input)

        create_tracker_input = CreateTrackerActivityInput(
            lead_id=input_validated.event.data.id,
        )

        create_tracker_result = await self._create_tracker(create_tracker_input)

        update_activity_input = UpdateCloseLeadActivityInput(
            lead_id=create_tracker_input.lead_id,
            tracker_id=create_tracker_result.tracker_id,
        )

        await self._update_close_lead(update_activity_input)


    def _validate_input(
        self, input: WebhookCreateTrackerPayload
    ) -> WebhookCreateTrackerPayloadValidated:
        try:
            input_validated = WebhookCreateTrackerPayloadValidated.model_validate(input.json_payload)
        except Exception as exc:
            
            raise ApplicationError(
                f"Invalid payload for create tracker workflow: {exc}"
            ) from exc

        return input_validated

    async def _create_tracker(
        self, input: CreateTrackerActivityInput
    ) -> CreateTrackerActivityResult:
        while True:
            try:
                return await workflow.execute_activity(
                        create_tracker_activity,
                        CreateTrackerActivityInput(lead_id=input.lead_id),
                        start_to_close_timeout=timedelta(seconds=60),
                        retry_policy=self._activity_retry_policy)
            except Exception:
                await self._wait_for_signal_data_issue_fixed()

    async def _update_close_lead(
        self, activity_input: UpdateCloseLeadActivityInput
    ) -> None:
        while True:
            try:
                await workflow.execute_activity(
                    update_close_lead_activity,
                    activity_input,
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
