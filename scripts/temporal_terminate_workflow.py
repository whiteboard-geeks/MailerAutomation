"""Terminate a single Temporal Workflow run.

Usage:
    set -a; source .env-staging; set +a; python -m scripts.temporal_terminate_workflow <workflow_id>
"""

import asyncio

import argparse

from temporal.client_provider import get_temporal_client
from temporal.temporal_workflows_client import TemporalWorkflowsClient


async def main(workflow_id: str):
    client = await get_temporal_client()
    temporal_workflows_client = TemporalWorkflowsClient(client)
    await temporal_workflows_client.terminate_workflow(workflow_id)


def parse_args():
    parser = argparse.ArgumentParser(description="Terminate a single Temporal Workflow run.")
    parser.add_argument("workflow_id", help="ID of the workflow to terminate")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.workflow_id))
