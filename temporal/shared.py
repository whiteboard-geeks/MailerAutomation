from temporalio import activity
from temporalio.common import SearchAttributeKey

from config import TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS


TASK_QUEUE_NAME = "task_queue"

WAITING_FOR_RESUME_KEY_STR = "WaitingForResume"
WAITING_FOR_RESUME_KEY = SearchAttributeKey.for_bool(WAITING_FOR_RESUME_KEY_STR)


def is_last_attempt(info: activity.Info) -> bool:
    return info.attempt >= TEMPORAL_WORKFLOW_ACTIVITY_MAX_ATTEMPTS
