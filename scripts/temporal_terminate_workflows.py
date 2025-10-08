"""Terminate Temporal Workflow runs given in a CSV file

Usage:
    set -a; source .env-prod-staging; set +a; python -m scripts.temporal_terminate_workflows [--dry-run] <csv_file>

The CSV file must have a header row
The CSV file must have these columns:

- todo: can be either terminate_prod, terminate_stg, noop, or dont_know
- workflow_id_to_terminate

In case the todo is terminate_prod or terminate_stg, the workflow_id_to_terminate
will be terminated in the corresponding environment.

The script requires the following environment variables to be set:
- TEMPORAL_ADDRESS_PROD
- TEMPORAL_NAMESPACE_PROD
- TEMPORAL_API_KEY_PROD
- TEMPORAL_ADDRESS_STAGING
- TEMPORAL_NAMESPACE_STAGING
- TEMPORAL_API_KEY_STAGING
"""

import sys
import asyncio
import argparse
import csv
from pathlib import Path
from scripts.client_provider import get_temporal_client, Environment


async def terminate_workflow(client, workflow_id: str, environment: str, dry_run: bool = False):
    """Terminate a single workflow"""
    if dry_run:
        print(f"[DRY RUN] Would terminate workflow {workflow_id} in {environment}")
        return
    
    try:
        handle = client.get_workflow_handle(workflow_id)
        await handle.terminate(reason="fixed manually")
        print(f"Successfully terminated workflow {workflow_id} in {environment}")
    except Exception as e:
        print(f"Error terminating workflow {workflow_id} in {environment}: {e}")


async def process_csv_file(csv_file: str, dry_run: bool = False):
    """Process the CSV file and terminate workflows as specified"""
    
    # Validate CSV file exists
    csv_path = Path(csv_file)
    if not csv_path.exists():
        print(f"Error: CSV file '{csv_file}' not found")
        sys.exit(1)
    
    # Initialize clients
    client_prod = None
    client_staging = None
    
    # Read and process CSV
    with open(csv_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # Validate required columns
        required_columns = {'todo', 'workflow_id_to_terminate'}
        fieldnames = reader.fieldnames or []
        if not required_columns.issubset(fieldnames):
            print(f"Error: CSV file must contain columns: {required_columns}")
            print(f"Found columns: {fieldnames}")
            sys.exit(1)
        
        rows_processed = 0
        rows_terminated = 0
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 since row 1 is header
            rows_processed += 1
            todo = row['todo'].strip()
            workflow_id = row['workflow_id_to_terminate'].strip()
            
            # Skip rows with empty workflow_id_to_terminate
            if not workflow_id:
                print(f"Row {row_num}: Skipping - no workflow_id_to_terminate")
                continue
            
            if todo == 'terminate_prod':
                if not dry_run and client_prod is None:
                    client_prod = await get_temporal_client(Environment.PROD)
                await terminate_workflow(client_prod, workflow_id, "PROD", dry_run)
                rows_terminated += 1
                
            elif todo == 'terminate_stg':
                if not dry_run and client_staging is None:
                    client_staging = await get_temporal_client(Environment.STAGING)
                await terminate_workflow(client_staging, workflow_id, "STAGING", dry_run)
                rows_terminated += 1
                
            elif todo == 'noop':
                print(f"Row {row_num}: No operation for workflow {workflow_id}")
                
            elif todo == 'dont_know':
                print(f"Row {row_num}: Don't know action for workflow {workflow_id} - skipping")
                
            else:
                print(f"Row {row_num}: Unknown todo value '{todo}' for workflow {workflow_id} - skipping")
    
    print(f"\nSummary:")
    print(f"Total rows processed: {rows_processed}")
    print(f"Workflows terminated: {rows_terminated}")
    if dry_run:
        print("This was a dry run - no workflows were actually terminated")


async def main():
    parser = argparse.ArgumentParser(
        description="Terminate Temporal Workflow runs given in a CSV file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
The CSV file must have a header row
The CSV file must have these columns:

- todo: can be either terminate_prod, terminate_stg, noop, or dont_know
- workflow_id_to_terminate

In case the todo is terminate_prod or terminate_stg, the workflow_id_to_terminate
will be terminated in the corresponding environment.
        """
    )
    
    parser.add_argument('csv_file', help='Path to the CSV file containing workflow termination instructions')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without actually terminating workflows')
    
    args = parser.parse_args()
    
    await process_csv_file(args.csv_file, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
