"""Temporal worker for running workflows and activities."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import timedelta

import structlog
from temporalio.client import Client
from temporalio.worker import Worker

from temporal.activities.easypost.webhook_delivery_status import (
    create_package_delivered_custom_activity_in_close_activity,
    update_delivery_info_for_lead_activity,
)

from temporal.activities.notifications import send_activity_timeout_alert

from .activities.instantly import webhook_email_sent
from .activities.instantly import webhook_reply_received as reply_received_activities
from .activities.easypost import webhook_create_tracker as easypost_activities
from temporal.client_provider import get_temporal_client
from temporal.shared import TASK_QUEUE_NAME

from .workflows.instantly.webhook_add_lead_workflow import WebhookAddLeadWorkflow
from .workflows.instantly.webhook_email_sent_workflow import WebhookEmailSentWorkflow
from .workflows.instantly.webhook_reply_received_workflow import (
    WebhookReplyReceivedWorkflow,
)
from .workflows.easypost.webhook_delivery_status_workflow import (
    WebhookDeliveryStatusWorkflow,
)
from .activities.instantly.webhook_add_lead import add_lead_to_instantly_campaign
from .workflows.easypost.webhook_create_tracker_workflow import (
    WebhookCreateTrackerWorkflow,
)

WORKFLOWS = [
    WebhookEmailSentWorkflow,
    WebhookAddLeadWorkflow,
    WebhookReplyReceivedWorkflow,
    WebhookCreateTrackerWorkflow,
    WebhookDeliveryStatusWorkflow,
]

ACTIVITIES = [
    webhook_email_sent.complete_lead_task_by_email,
    webhook_email_sent.add_email_activity_to_lead,
    add_lead_to_instantly_campaign,
    reply_received_activities.add_email_activity_to_lead,
    reply_received_activities.pause_sequence_subscriptions,
    reply_received_activities.send_notification_email,
    easypost_activities.create_tracker_activity,
    easypost_activities.update_close_lead_activity,
    update_delivery_info_for_lead_activity,
    create_package_delivered_custom_activity_in_close_activity,
    send_activity_timeout_alert,
]


def _build_worker(client: Client, executor: ThreadPoolExecutor) -> Worker:
    return Worker(
        client,
        task_queue=TASK_QUEUE_NAME,
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
        graceful_shutdown_timeout=timedelta(minutes=1),
        max_concurrent_activities=10,
        max_concurrent_workflow_tasks=5,
        activity_executor=executor,
    )


async def run_worker() -> None:
    """Run the self-hosted Temporal worker."""
    logging.basicConfig(level=logging.INFO)
    logger = structlog.get_logger(__name__)

    try:
        client = await get_temporal_client()
    except Exception as exc:
        logger.exception("failed_to_connect_to_temporal_server", error=str(exc))
        raise

    with ThreadPoolExecutor(max_workers=10) as activity_executor:
        worker = _build_worker(client, activity_executor)
        logger.info("starting_temporal_worker", task_queue=TASK_QUEUE_NAME)
        await worker.run()


if __name__ == "__main__":
    asyncio.run(run_worker())
