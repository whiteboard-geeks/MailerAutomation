from __future__ import annotations

from datetime import timedelta
from enum import Enum

from pydantic import BaseModel, Field
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

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
    NO_OP_DELIVERY_STATUS_NOT_DELIVERED = "no_op_delivery_status_not_delivered"
    NO_OP_RETURNED_TO_SENDER = "no_op_returned_to_sender"
    NO_OP_DUPLICATE_ACTIVITY = "no_op_duplicate_activity"


@workflow.defn
class WebhookDeliveryStatusWorkflow:
    def __init__(self) -> None:
        self._data_issue_fixed: bool = True
        self._activity_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            maximum_attempts=2,
        )

    @workflow.run
    async def run(self, input: WebhookDeliveryStatusPayload) -> WebhookDeliveryStatusResult:
        input_validated = self._validate_input(input)

        if input_validated.result.status != "delivered":
            return WebhookDeliveryStatusResult(
                status=Status.NO_OP_DELIVERY_STATUS_NOT_DELIVERED
            )

        last_tracking_detail = input_validated.result.tracking_details[-1]

        if last_tracking_detail.message == "Delivered, To Original Sender":
            return WebhookDeliveryStatusResult(
                status=Status.NO_OP_RETURNED_TO_SENDER
            )

        update_delivery_info_input = UpdateDeliveryInfoInput(
            tracking_code=input_validated.result.tracking_code,
            last_tracking_detail=TrackingDetailActivity.new(
                city=last_tracking_detail.tracking_location.city,
                state=last_tracking_detail.tracking_location.state,
                datetime=last_tracking_detail.datetime,
            ),
        )

        update_delivery_info_result = await self._update_delivery_info_for_lead(
            update_delivery_info_input)

        create_package_delivered_custom_input = CreatePackageDeliveredCustomInput(
            lead_id=update_delivery_info_result.lead_id,
            last_tracking_detail=update_delivery_info_input.last_tracking_detail,
        )

        create_package_delivered_custom_result = await self._create_package_delivered_custom_activity(
            create_package_delivered_custom_input)
        
        status = Status.SUCCESS

        if create_package_delivered_custom_result.status == CreatePackageDeliveredCustomResult.Status.SKIPPED:
            status = Status.NO_OP_DUPLICATE_ACTIVITY

        return WebhookDeliveryStatusResult(status=status)

    def _validate_input(
        self, input: WebhookDeliveryStatusPayload
    ) -> WebhookDeliveryStatusPayloadValidated:
        try:
            input_validated = WebhookDeliveryStatusPayloadValidated.model_validate(input.json_payload)
        except Exception as exc:
            raise ApplicationError(
                f"Invalid payload for delivery status workflow: {exc}"
            ) from exc

        return input_validated
    
    async def _update_delivery_info_for_lead(self, input: UpdateDeliveryInfoInput) -> UpdateDeliveryInfoResult:
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

    async def _create_package_delivered_custom_activity(self, input: CreatePackageDeliveredCustomInput) -> CreatePackageDeliveredCustomResult:
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
