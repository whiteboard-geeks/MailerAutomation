"""Workflow-side helper for reporting activity timeouts.

Called from inside a workflow's ``except`` block, before it parks on the
``data_issue_fixed`` signal.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from config import (
    TEMPORAL_ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS,
    TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS,
)
from temporal.shared import is_timeout_failure

with workflow.unsafe.imports_passed_through():
    from temporal.activities.notifications import (
        ActivityTimeoutAlertArgs,
        send_activity_timeout_alert,
    )


async def alert_if_activity_timeout(
    exc: BaseException,
    *,
    activity_name: str,
    route: str,
    lead_email: str | None = None,
    campaign_name: str | None = None,
    lead_id: str | None = None,
) -> None:
    """Send a timeout alert if ``exc`` was caused by a Temporal timeout.

    Application errors are skipped: the activity already emailed on its last attempt, so
    alerting here too would double-send.
    """
    if not is_timeout_failure(exc):
        return

    try:
        await workflow.execute_activity(
            send_activity_timeout_alert,
            ActivityTimeoutAlertArgs(
                workflow_id=workflow.info().workflow_id,
                workflow_type=workflow.info().workflow_type,
                activity_name=activity_name,
                route=route,
                timeout_seconds=TEMPORAL_ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS,
                max_attempts=TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS,
                lead_email=lead_email,
                campaign_name=campaign_name,
                lead_id=lead_id,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
    except Exception:
        # Never let the alerting path change workflow control flow -- the workflow still
        # needs to park and wait for the data_issue_fixed signal.
        workflow.logger.warning(
            "activity_timeout_alert_failed", extra={"activity_name": activity_name}
        )
