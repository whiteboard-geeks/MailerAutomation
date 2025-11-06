"""List workflows for Temporal and save to JSONL file.

This script expects these environment variables to be set:

- TEMPORAL_ADDRESS
- TEMPORAL_NAMESPACE
- TEMPORAL_API_KEY

Usage:
    set -a; source .env-prod; set +a; python -m scripts.temporal_list_workflows <output_dir>

Optional flags:
    --filter-exec-status S  Filter workflows by execution status (Running or Completed).
    --take T        Only process the first T workflows.
    --workflow-type T       Filter workflows by workflow type.

It saves workflow data as JSONL to: <output_dir>/<timestamp>.jsonl

For each workflow, the following information is saved:
    - *Workflow ID
    - *Workflow Type
    - *Start Time
    - *Execution Status
    - =WaitingForResume
    - +Lead Email Address: corresponds to the field `json_payload.lead_email` of the input json object of the workflow

Fields marked with * are native to Temporal.
Fields marked with = are custom search attributes.
Fields marked with + are part of the input of the workflow.
"""

import os

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from temporalio.client import Client, WorkflowHandle, WorkflowExecutionStatus

from temporal.client_provider import get_temporal_client
from temporal.shared import WAITING_FOR_RESUME_KEY

# Environment variables
TEMPORAL_ADDRESS = os.environ["TEMPORAL_ADDRESS"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]
TEMPORAL_API_KEY = os.environ["TEMPORAL_API_KEY"]


async def main(output_dir: str, filter_exec_status: str | None, take: int | None, workflow_type: str | None):
    client = await get_temporal_client()

    # Create output directory structure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / f"{timestamp}.jsonl"

    query_parts = []
    if filter_exec_status:
        query_parts.append(f'ExecutionStatus = "{filter_exec_status}"')
    if workflow_type:
        query_parts.append(f'WorkflowType = "{workflow_type}"')

    if query_parts:
        query = ' AND '.join(query_parts)
        workflow_iterator = client.list_workflows(query)
    else:
        workflow_iterator = client.list_workflows()

    with open(output_file, 'w') as f:
        count = 0
        async for wf in workflow_iterator:
            if take is not None and count >= take:
                break
            workflow_id = wf.id
            workflow_type = wf.workflow_type
            workflow_handle = client.get_workflow_handle(workflow_id, run_id=wf.run_id)
            start_time = str(wf.start_time)
            status = str(wf.status)
            waiting_for_resume = wf.typed_search_attributes.get(WAITING_FOR_RESUME_KEY)

            json_payload = await fetch_json_payload_from_history(client, workflow_handle)
            workflow_result = await get_workflow_result(workflow_handle, wf.status)

            # Create JSON object for this workflow
            workflow_data = {
                "workflow_id": workflow_id,
                "workflow_type": workflow_type,
                "start_time": start_time,
                "status": status,
                "waiting_for_resume": waiting_for_resume,
                "pl": json_payload,
                "result": workflow_result
            }

            # Write as JSONL (one JSON object per line)
            f.write(json.dumps(workflow_data) + '\n')
            count += 1


async def fetch_json_payload_from_history(client: Client, handle: WorkflowHandle) -> dict:
    input_obj = await get_workflow_input(client, handle)
    if not input_obj:
        return {}
    return input_obj[0]["json_payload"]


async def get_workflow_input(client: Client, handle: WorkflowHandle):
    # Fetch the first event in the workflow history
    async for event in handle.fetch_history_events(page_size=1):
        # The first event should be WorkflowExecutionStarted
        attrs = getattr(event, "workflow_execution_started_event_attributes", None)
        if attrs and attrs.input and attrs.input.payloads:
            # Use the client's data converter to decode the payload
            payload = attrs.input.payloads[0]
            # This returns a Python object (dict) if the input was a JSON object
            return await client.data_converter.decode([payload])
        break
    return None


async def get_workflow_result(handle: WorkflowHandle, exec_status: WorkflowExecutionStatus | None) -> dict | None:
    if exec_status != WorkflowExecutionStatus.COMPLETED:
        return None
    try:
        res = await handle.result()
        return res
    except Exception:
        print(f"Error getting result for workflow {handle.id}")
        return None



def parse_args():
    parser = argparse.ArgumentParser(description="Export Temporal workflows to JSONL.")
    parser.add_argument(
        "output_dir",
        help="Directory to save the output JSONL file.",
    )
    parser.add_argument(
        "--filter-exec-status",
        choices=["Running", "Completed"],
        help="Filter workflows by execution status.",
    )
    parser.add_argument(
        "--take",
        type=int,
        help="Only process the first T workflows.",
    )
    parser.add_argument(
        "--workflow-type",
        help="Filter workflows by workflow type.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(output_dir=args.output_dir, filter_exec_status=args.filter_exec_status, take=args.take, workflow_type=args.workflow_type))
