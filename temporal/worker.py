"""Temporal worker for running workflows and activities."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import timedelta

import structlog
from temporalio.worker import Worker

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
from .activities.instantly.webhook_add_lead import add_lead_to_instantly_campaign
from .workflows.easypost.webhook_create_tracker_workflow import (
    WebhookCreateTrackerWorkflow,
)

async def run_worker() -> None:
    """Run the Temporal worker with proper configuration."""

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = structlog.get_logger(__name__)

    try:
        # Connect to Temporal server
        client = await get_temporal_client()

        logger.info("connected_to_temporal_server")
    except Exception as e:
        logger.error(f"Failed to connect to Temporal server: {e}")
        logger.info("Worker will run without Temporal connection (for testing)")
        return

    with ThreadPoolExecutor(max_workers=10) as activity_executor:
        # Create worker with all workflows and activities
        worker = Worker(
            client,
            task_queue=TASK_QUEUE_NAME,
            workflows=[
                WebhookEmailSentWorkflow,
                WebhookAddLeadWorkflow,
                WebhookReplyReceivedWorkflow,
                WebhookCreateTrackerWorkflow,
            ],
            activities=[
                webhook_email_sent.complete_lead_task_by_email,
                webhook_email_sent.add_email_activity_to_lead,
                add_lead_to_instantly_campaign,
                reply_received_activities.add_email_activity_to_lead,
                reply_received_activities.pause_sequence_subscriptions,
                reply_received_activities.send_notification_email,
                easypost_activities.create_tracker_activity,
                easypost_activities.update_close_lead_activity,
            ],
            # Graceful shutdown timeout
            graceful_shutdown_timeout=timedelta(minutes=1),
            # Activity task configuration
            max_concurrent_activities=10,
            max_concurrent_workflow_tasks=5,
            activity_executor=activity_executor,

        )

        logger.info("Starting Temporal worker...")
        try:
            await worker.run()
        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
        except Exception as e:
            logger.error(f"Worker failed: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(run_worker())
