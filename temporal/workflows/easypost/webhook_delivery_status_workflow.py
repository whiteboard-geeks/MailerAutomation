from __future__ import annotations

from datetime import timedelta
from enum import Enum
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


with workflow.unsafe.imports_passed_through():
    from temporal.activities.easypost.webhook_delivery_status import (
        CreatePackageDeliveredCustomInput,
        CreatePackageDeliveredCustomResult,
        UpdateDeliveryInfoInput,
        UpdateDeliveryInfoResult,
        create_package_delivered_custom_activity_in_close_activity,
        update_delivery_info_for_lead_activity,
        TrackingDetail as TrackingDetailActivity,
    )
    from utils.email import send_email


class WebhookDeliveryStatusPayload(BaseModel):
    json_payload: dict = Field(..., description="JSON payload of the request")


class WebhookDeliveryStatusPayloadValidated(BaseModel):
    result: Result = Field(..., description="Validated webhook payload")


class Result(BaseModel):
    tracking_code: str = Field(..., description="Tracking code of the package.")
    tracking_details: list[TrackingDetail] = Field(
        ..., description="Tracking details of the package."
    )
    status: str = Field(..., description="Status of the package.")


class WebhookDeliveryStatusResult(BaseModel):
    status: Status


class TrackingDetail(BaseModel):
    tracking_location: TrackingLocation = Field(
        ..., description="Tracking location of the package."
    )
    message: str | None = Field(..., description="Message of the tracking detail.")
    datetime: str = Field(..., description="Datetime of the tracking detail.")


class TrackingLocation(BaseModel):
    city: str | None = Field(..., description="City of the tracking location.")
    state: str | None = Field(..., description="State of the tracking location.")


class Status(str, Enum):
    SUCCESS = "success"
    NO_OP_RETURNED_TO_SENDER = "no_op_returned_to_sender"
    NO_OP_DUPLICATE_ACTIVITY = "no_op_duplicate_activity"


@workflow.defn
class WebhookDeliveryStatusWorkflow:
    def __init__(self) -> None:
        self._data_issue_fixed: bool = True
        self._activity_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            maximum_attempts=TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS,
        )

    @workflow.run
    async def run(
        self, input: WebhookDeliveryStatusPayload
    ) -> WebhookDeliveryStatusResult:
        input_validated = self._validate_input(input)

        last_tracking_detail = input_validated.result.tracking_details[-1]

        if last_tracking_detail.message == "Delivered, To Original Sender":
            return WebhookDeliveryStatusResult(status=Status.NO_OP_RETURNED_TO_SENDER)

        update_delivery_info_input = UpdateDeliveryInfoInput(
            tracking_code=input_validated.result.tracking_code,
            last_tracking_detail=TrackingDetailActivity.new(
                city=last_tracking_detail.tracking_location.city,
                state=last_tracking_detail.tracking_location.state,
                datetime=last_tracking_detail.datetime,
            ),
        )

        update_delivery_info_result = await self._update_delivery_info_for_lead(
            update_delivery_info_input
        )

        create_package_delivered_custom_input = CreatePackageDeliveredCustomInput(
            lead_id=update_delivery_info_result.lead_id,
            last_tracking_detail=update_delivery_info_input.last_tracking_detail,
        )

        create_package_delivered_custom_result = (
            await self._create_package_delivered_custom_activity(
                create_package_delivered_custom_input
            )
        )

        status = Status.SUCCESS

        if (
            create_package_delivered_custom_result.status
            == CreatePackageDeliveredCustomResult.Status.SKIPPED
        ):
            status = Status.NO_OP_DUPLICATE_ACTIVITY

        return WebhookDeliveryStatusResult(status=status)

    def _validate_input(
        self, input: WebhookDeliveryStatusPayload
    ) -> WebhookDeliveryStatusPayloadValidated:
        try:
            input_validated = WebhookDeliveryStatusPayloadValidated.model_validate(
                input.json_payload
            )
        except Exception as exc:
            _send_error_email_validation_error(
                workflow_id=workflow.info().workflow_id, json_payload=input.json_payload
            )
            raise ApplicationError(
                f"Invalid payload for delivery status workflow: {exc}"
            ) from exc

        return input_validated

    async def _update_delivery_info_for_lead(
        self, input: UpdateDeliveryInfoInput
    ) -> UpdateDeliveryInfoResult:
        while True:
            try:
                return await workflow.execute_activity(
                    update_delivery_info_for_lead_activity,
                    input,
                    start_to_close_timeout=timedelta(seconds=60),
                    retry_policy=self._activity_retry_policy,
                )
            except Exception:
                await self._wait_for_signal_data_issue_fixed()

    async def _create_package_delivered_custom_activity(
        self, input: CreatePackageDeliveredCustomInput
    ) -> CreatePackageDeliveredCustomResult:
        while True:
            try:
                return await workflow.execute_activity(
                    create_package_delivered_custom_activity_in_close_activity,
                    input,
                    start_to_close_timeout=timedelta(seconds=60),
                    retry_policy=self._activity_retry_policy,
                )
            except Exception:
                await self._wait_for_signal_data_issue_fixed()

    async def _wait_for_signal_data_issue_fixed(self) -> None:
        self._data_issue_fixed = False
        workflow.upsert_search_attributes({WAITING_FOR_RESUME_KEY_STR: [True]})
        await workflow.wait_condition(lambda: self._data_issue_fixed)
        workflow.upsert_search_attributes({WAITING_FOR_RESUME_KEY_STR: [False]})


def _send_error_email_validation_error(
    workflow_id: str, json_payload: dict[str, Any]
) -> None:
    detailed_error_message = f"""
        <h2>Validation Error in EasyPost Delivery Status Workflow</h2>
        <p><strong>Error:</strong> Payload validation failed</p>
        <p><strong>Route:</strong> /easypost/delivery_status</p>
        <p><strong>Workflow Run:</strong> <a href="{TEMPORAL_WORKFLOW_UI_BASE_URL}/{workflow_id}">{workflow_id}</a></p>
        <p><strong>Temporal Playbook:</strong> <a href="{MAILER_AUTOMATION_TEMPORAL_PLAYBOOK_URL}">Mailer Automation Temporal Playbook</a></p>
        <p><strong>Time:</strong> {workflow.now().isoformat()}</p>
        
        <h3>JSON Payload:</h3>
        <pre>{json.dumps(json_payload, indent=2, default=str)}</pre>
        """
    send_email(
        subject="Validation Error in EasyPost Delivery Status Workflow",
        body=detailed_error_message,
    )
