from temporalio.common import SearchAttributeKey


TASK_QUEUE_NAME="task_queue"

WAITING_FOR_RESUME_KEY_STR = "WaitingForResume"
WAITING_FOR_RESUME_KEY = SearchAttributeKey.for_bool(WAITING_FOR_RESUME_KEY_STR)
