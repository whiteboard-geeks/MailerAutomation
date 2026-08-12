"""Tests for activity-timeout detection and alerting.

Background: activities report their own errors by email on the last retry attempt. A
start-to-close timeout tears the activity down mid-execution, so that reporting never
runs -- which is how 24 email_sent workflows parked silently on 2026-08-10/11 while
Close tasks went uncompleted. The workflow now reports timeouts itself.
"""

import pytest
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    TimeoutError as TemporalTimeoutError,
    TimeoutType,
)

from temporal.shared import is_timeout_failure


def _timeout_error() -> TemporalTimeoutError:
    return TemporalTimeoutError(
        "activity StartToClose timeout",
        type=TimeoutType.START_TO_CLOSE,
        last_heartbeat_details=[],
    )


def test_direct_timeout_is_detected():
    assert is_timeout_failure(_timeout_error()) is True


def test_timeout_wrapped_in_activity_error_is_detected():
    """Temporal surfaces activity failures wrapped in ActivityError."""
    wrapped = ActivityError(
        "activity failed",
        scheduled_event_id=1,
        started_event_id=2,
        identity="test",
        activity_type="complete_lead_task_by_email",
        activity_id="1",
        retry_state=None,
    )
    wrapped.__cause__ = _timeout_error()
    assert is_timeout_failure(wrapped) is True


def test_application_error_is_not_a_timeout():
    """'Could not find task for campaign X' already emails from inside the activity."""
    err = ApplicationError("Could not find task for campaign BP_BC_BlindInviteEmail1")
    assert is_timeout_failure(err) is False


def test_plain_exception_is_not_a_timeout():
    assert is_timeout_failure(ValueError("boom")) is False


def test_cause_cycle_terminates():
    """A self-referential cause chain must not hang the workflow."""
    a = ApplicationError("a")
    b = ApplicationError("b")
    a.__cause__ = b
    b.__cause__ = a
    assert is_timeout_failure(a) is False


def test_timeout_constant_is_not_tighter_than_add_lead():
    """email_sent/reply_received previously sat at 10s while add_lead used 60s."""
    from config import TEMPORAL_ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS

    assert TEMPORAL_ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS >= 60


@pytest.mark.parametrize(
    "module_path",
    [
        "temporal/workflows/instantly/webhook_email_sent_workflow.py",
        "temporal/workflows/instantly/webhook_reply_received_workflow.py",
    ],
)
def test_no_hardcoded_ten_second_timeouts_remain(module_path):
    source = open(module_path).read()
    assert "timedelta(seconds=10)" not in source


def test_alert_activity_is_registered_on_worker():
    """An unregistered activity would fail at runtime, in the error path."""
    from temporal.activities.notifications import send_activity_timeout_alert
    from temporal.worker import ACTIVITIES

    assert send_activity_timeout_alert in ACTIVITIES
