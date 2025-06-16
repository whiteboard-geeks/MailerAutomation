#!/usr/bin/env python3
"""
Cleanup script to delete pre-generated test leads from Close.

This script reads the test_leads_3000.json file and deletes all the leads
listed in it from Close. Use this when you're done with testing and want
to clean up the test data.

Usage:
    python scripts/cleanup_test_leads.py
"""

import os
import sys
import json
from datetime import datetime

# Add the parent directory to Python path so we can import from tests/utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.utils.close_api import CloseAPI
from scripts.generate_test_leads import load_test_leads


def cleanup_test_leads(filename="test_leads_3000.json"):
    """
    Delete all test leads listed in the specified file.

    Args:
        filename (str): Name of the file containing lead IDs to delete

    Returns:
        tuple: (successful_deletions, failed_deletions)
    """
    print("\n=== CLEANING UP TEST LEADS ===")

    # Load the test leads
    leads = load_test_leads(filename)
    if not leads:
        print("No test leads found to clean up.")
        return 0, 0

    print(f"Found {len(leads)} test leads to delete.")

    # Confirm before proceeding
    response = input(
        f"\nThis will PERMANENTLY DELETE {len(leads)} leads from Close. Continue? (y/N): "
    )
    if response.lower() != "y":
        print("Cleanup cancelled.")
        return 0, 0

    # Initialize Close API
    close_api = CloseAPI()

    successful_deletions = 0
    failed_deletions = []

    print("Deleting leads...")

    for i, lead in enumerate(leads):
        lead_id = lead["id"]

        try:
            result = close_api.delete_lead(lead_id)
            if result == {} or result is True:  # Successful deletion
                successful_deletions += 1
            else:
                failed_deletions.append(
                    {"id": lead_id, "error": f"Unexpected result: {result}"}
                )

        except Exception as e:
            failed_deletions.append({"id": lead_id, "error": str(e)})

        # Progress indicator every 100 deletions
        if (i + 1) % 100 == 0:
            print(
                f"  Deleted {i + 1}/{len(leads)} leads ({len(failed_deletions)} failures)"
            )

    print("\n=== CLEANUP COMPLETE ===")
    print(f"Successfully deleted: {successful_deletions} leads")
    print(f"Failed to delete: {len(failed_deletions)} leads")

    if failed_deletions:
        print(f"Failure rate: {len(failed_deletions)/len(leads)*100:.1f}%")

        # Save failure report
        scripts_dir = os.path.dirname(os.path.abspath(__file__))
        failure_file = os.path.join(scripts_dir, "cleanup_failures.json")

        try:
            with open(failure_file, "w") as f:
                json.dump(
                    {
                        "cleanup_date": datetime.now().isoformat(),
                        "total_attempted": len(leads),
                        "successful_deletions": successful_deletions,
                        "failed_deletions": failed_deletions,
                    },
                    f,
                    indent=2,
                )
            print(f"Failure details saved to: {failure_file}")
        except Exception as e:
            print(f"Could not save failure report: {e}")

    return successful_deletions, len(failed_deletions)


def delete_leads_file(filename="test_leads_3000.json"):
    """
    Delete the test leads JSON file after cleanup.

    Args:
        filename (str): Name of the file to delete
    """
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(scripts_dir, filename)

    if os.path.exists(filepath):
        try:
            os.remove(filepath)
            print(f"✓ Deleted test leads file: {filepath}")
        except Exception as e:
            print(f"✗ Could not delete test leads file: {e}")
    else:
        print(f"Test leads file not found: {filepath}")


def main():
    """Main function to cleanup test leads."""
    print("=== TEST LEADS CLEANUP UTILITY ===")
    print("This script will delete all pre-generated test leads from Close.")

    # Run the cleanup
    successful, failed = cleanup_test_leads()

    if successful > 0:
        print(f"\n✓ Successfully cleaned up {successful} test leads from Close.")

        # Ask if we should also delete the leads file
        if failed == 0:  # Only if all deletions were successful
            response = input("\nDelete the test leads file as well? (y/N): ")
            if response.lower() == "y":
                delete_leads_file()
        else:
            print(f"Keeping test leads file due to {failed} failed deletions.")
            print("Check cleanup_failures.json for details on failed deletions.")
    else:
        print("\nNo leads were deleted.")


if __name__ == "__main__":
    main()
