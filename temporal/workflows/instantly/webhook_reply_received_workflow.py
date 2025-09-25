from __future__ import annotations

import os
from datetime import timedelta

from pydantic import BaseModel, Field
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from temporal.shared import WAITING_FOR_RESUME_KEY_STR

ENV_TYPE = os.getenv("ENV_TYPE", "development")

with workflow.unsafe.imports_passed_through():
    from temporal.activities.instantly.webhook_reply_received import (
        AddEmailActivityToLeadArgs,
        AddEmailActivityToLeadResult,
        PauseSequenceSubscriptionsArgs,
        PauseSequenceSubscriptionsResult,
        SendNotificationEmailArgs,
        WebhookReplyReceivedPayloadValidated,
        add_email_activity_to_lead,
        pause_sequence_subscriptions,
        send_notification_email,
    )


class WebhookReplyReceivedPayload(BaseModel):
    json_payload: dict = Field(..., description="JSON payload of the request")


@workflow.defn
class WebhookReplyReceivedWorkflow:
    def __init__(self) -> None:
        self._data_issue_fixed: bool = True
        self._activity_retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            maximum_attempts=3,
        )

    @workflow.signal
    def data_issue_fixed(self) -> None:
        self._data_issue_fixed = True

    @workflow.run
    async def run(self, input: WebhookReplyReceivedPayload) -> None:
        input_validated = self._validate_input(input)

        add_email_result = await self._add_email_activity_to_lead(input_validated)

        pause_result = await self._pause_sequence_subscriptions(
            add_email_result.lead_id,
            input_validated.lead_email,
        )

        await self._send_notification_email(
            add_email_result,
            pause_result,
            input_validated,
        )

    async def _add_email_activity_to_lead(
        self,
        input_validated: WebhookReplyReceivedPayloadValidated,
    ) -> AddEmailActivityToLeadResult:
        while True:
            try:
                return await workflow.execute_activity(
                    add_email_activity_to_lead,
                    AddEmailActivityToLeadArgs(payload=input_validated),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=self._activity_retry_policy,
                )
            except Exception:
                await self._wait_for_signal_data_issue_fixed()

    async def _pause_sequence_subscriptions(
        self,
        lead_id: str,
        lead_email: str,
    ) -> PauseSequenceSubscriptionsResult:
        while True:
            try:
                return await workflow.execute_activity(
                    pause_sequence_subscriptions,
                    PauseSequenceSubscriptionsArgs(
                        lead_id=lead_id, lead_email=lead_email
                    ),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=self._activity_retry_policy,
                )
            except Exception:
                await self._wait_for_signal_data_issue_fixed()

    async def _send_notification_email(
        self,
        add_email_result: AddEmailActivityToLeadResult,
        pause_result: PauseSequenceSubscriptionsResult,
        input_validated: WebhookReplyReceivedPayloadValidated,
    ) -> None:
        while True:
            try:
                await workflow.execute_activity(
                    send_notification_email,
                    SendNotificationEmailArgs(
                        lead_id=add_email_result.lead_id,
                        lead_email=add_email_result.lead_email,
                        lead_name=add_email_result.lead_name,
                        campaign_name=input_validated.campaign_name,
                        reply_subject=input_validated.reply_subject,
                        reply_text=input_validated.reply_text,
                        reply_html=input_validated.reply_html,
                        env_type=ENV_TYPE,
                        paused_subscriptions=pause_result.paused_subscriptions,
                        lead_details=add_email_result.lead_details,
                        email_activity_id=add_email_result.email_activity_id,
                    ),
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
    def _validate_input(
        input: WebhookReplyReceivedPayload,
    ) -> WebhookReplyReceivedPayloadValidated:
        payload = input.json_payload
        try:
            validated = WebhookReplyReceivedPayloadValidated(
                event_type=payload["event_type"],
                lead_email=payload["lead_email"],
                campaign_name=payload["campaign_name"],
                reply_subject=payload["reply_subject"],
                reply_text=payload.get("reply_text"),
                reply_html=payload.get("reply_html"),
                timestamp=payload["timestamp"],
                email_account=payload["email_account"],
            )
        except KeyError as exc:
            raise ApplicationError(
                f"Missing required field in reply received payload: {exc}"
            ) from exc
        except Exception as exc:  # pragma: no cover - defensive guard
            raise ApplicationError(
                f"Invalid payload for reply received webhook: {exc}"
            ) from exc

        if validated.event_type != "reply_received":
            raise ApplicationError(
                f"Expected reply_received event, got {validated.event_type}"
            )

        if not (validated.reply_text or validated.reply_html):
            raise ApplicationError(
                "Either reply_text or reply_html must be provided"
            )

        return validated


__all__ = [
    "WebhookReplyReceivedPayload",
    "WebhookReplyReceivedWorkflow",
]
