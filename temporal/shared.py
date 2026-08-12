from temporalio import activity
from temporalio.common import SearchAttributeKey
from temporalio.exceptions import TimeoutError as TemporalTimeoutError

from config import TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS


TASK_QUEUE_NAME = "task_queue"

WAITING_FOR_RESUME_KEY_STR = "WaitingForResume"
WAITING_FOR_RESUME_KEY = SearchAttributeKey.for_bool(WAITING_FOR_RESUME_KEY_STR)


def is_last_attempt(info: activity.Info) -> bool:
    return info.attempt >= TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS


def is_timeout_failure(exc: BaseException) -> bool:
    """True when an activity failure was ultimately caused by a Temporal timeout.

    Activities report their own errors by email on the last attempt (see
    ``is_last_attempt``). A start-to-close timeout tears the activity down mid-run, so
    that reporting code never executes and the failure is silent. Workflows use this to
    decide whether they need to raise the alarm themselves, without double-sending for
    ordinary application errors the activity already reported.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TemporalTimeoutError):
            return True
        current = getattr(current, "cause", None)
    return False
