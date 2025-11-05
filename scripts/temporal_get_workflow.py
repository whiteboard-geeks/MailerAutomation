"""Print information about a Temporal workflow run to stdout.

Usage:
    set -a; source .env-staging; set +a; python -m scripts.temporal_get_workflow <workflow_id>
"""

import asyncio

import argparse

from temporal.client_provider import get_temporal_client
from temporal.temporal_workflows_client import TemporalWorkflowsClient


async def main(workflow_id: str):
    client = await get_temporal_client()
    temporal_workflows_client = TemporalWorkflowsClient(client)
    status, result = await temporal_workflows_client.get_workflow_status_and_result(workflow_id)

    if status:
        status = status.name

    print(f"status: {status}")
    print(f"result: {result}")


def parse_args():
    parser = argparse.ArgumentParser(description="Print information about a Temporal workflow run to stdout.")
    parser.add_argument("workflow_id", help="ID of the workflow to print")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.workflow_id))
