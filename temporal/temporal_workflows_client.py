from typing import Any

from temporalio.client import Client, WorkflowExecutionStatus


class TemporalWorkflowsClient:
    def __init__(self, client: Client) -> None:
        self._client = client

    async def get_workflow_status_and_result(self, workflow_id: str) -> tuple[WorkflowExecutionStatus | None, Any]:
        handle = self._client.get_workflow_handle(workflow_id)
        desc = await handle.describe()

        if desc.status == WorkflowExecutionStatus.COMPLETED:
            result = await handle.result()
            return desc.status, result

        return desc.status, None

    async def terminate_workflow(self, workflow_id: str) -> None:
        """Terminate the workflow if it is running."""
        handle = self._client.get_workflow_handle(workflow_id)
        desc = await handle.describe()
        if desc.status == WorkflowExecutionStatus.RUNNING:
            await handle.terminate(reason="integration test cleanup")

