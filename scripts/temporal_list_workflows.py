"""List workflows for Temporal and save to JSONL file.

This script expects these environment variables to be set:

- TEMPORAL_ADDRESS
- TEMPORAL_NAMESPACE
- TEMPORAL_API_KEY

Usage:
    set -a; source .env-prod; set +a; python -m scripts.temporal_list_workflows

Optional flags:
    --only-running  Only export workflows whose execution status is Running.

It saves workflow data as JSONL to: workflow-runs/<namespace>/<timestamp>.jsonl

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

from temporal.client_provider import get_temporal_client
from temporal.shared import WAITING_FOR_RESUME_KEY

# Environment variables
TEMPORAL_ADDRESS = os.environ["TEMPORAL_ADDRESS"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]
TEMPORAL_API_KEY = os.environ["TEMPORAL_API_KEY"]


async def main(only_running: bool):
    client = await get_temporal_client()

    # Create output directory structure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("workflow-runs") / TEMPORAL_NAMESPACE
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"{timestamp}.jsonl"

    workflow_iterator = (
        client.list_workflows('ExecutionStatus = "Running"')
        if only_running
        else client.list_workflows()
    )

    with open(output_file, 'w') as f:
        async for wf in workflow_iterator:
            workflow_id = wf.id
            workflow_type = wf.workflow_type
            start_time = str(wf.start_time)
            status = str(wf.status)
            waiting_for_resume = wf.typed_search_attributes.get(WAITING_FOR_RESUME_KEY)

            json_payload = await fetch_json_payload_from_history(client, workflow_id, wf.run_id)

            # Create JSON object for this workflow
            workflow_data = {
                "workflow_id": workflow_id,
                "workflow_type": workflow_type,
                "start_time": start_time,
                "status": status,
                "waiting_for_resume": waiting_for_resume,
                "pl": json_payload
            }
            
            # Write as JSONL (one JSON object per line)
            f.write(json.dumps(workflow_data) + '\n')


async def fetch_json_payload_from_history(client, workflow_id, run_id) -> dict:
    input_obj = await get_workflow_input(client, workflow_id, run_id)
    if not input_obj:
        return {}
    return input_obj[0]["json_payload"]


async def get_workflow_input(client, workflow_id, run_id):
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
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


def parse_args():
    parser = argparse.ArgumentParser(description="Export Temporal workflows to JSONL.")
    parser.add_argument(
        "--only-running",
        action="store_true",
        help="Only export workflows that are currently running.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(only_running=args.only_running))
