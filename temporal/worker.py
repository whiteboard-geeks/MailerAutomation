"""Temporal worker for running workflows and activities."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import timedelta

import structlog
from temporalio.worker import Worker

from temporal.client_provider import get_temporal_client
from temporal.shared import TASK_QUEUE_NAME

from .workflows.instantly import WebhookEmailSentWorkflow
from .activities.instantly import complete_lead_task_by_email, add_email_activity_to_lead

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
            workflows=[WebhookEmailSentWorkflow],
            activities=[complete_lead_task_by_email, add_email_activity_to_lead],
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
