#!/usr/bin/env python3
"""
Standalone script to generate 3,000 test leads in Close for timeout reproduction testing.

This script creates a large batch of leads once and saves their IDs to a JSON file,
so the timeout reproduction tests can reuse them instead of creating new leads each time.

Usage:
    python scripts/generate_test_leads.py

Output:
    - Creates leads in Close
    - Saves lead IDs to scripts/test_leads_3000.json
    - Prints progress and final summary
"""

import os
import sys
import json
from datetime import datetime

# Add the parent directory to Python path so we can import from tests/utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.utils.close_api import CloseAPI


def generate_test_leads(count=3000):
    """
    Generate the specified number of test leads in Close.

    Args:
        count (int): Number of test leads to create (default: 3000)

    Returns:
        list: List of created lead data with IDs
    """
    print(f"\n=== GENERATING {count} TEST LEADS FOR TIMEOUT REPRODUCTION ===")
    print("This will take several minutes. Progress will be shown every 50 leads.")

    # Initialize Close API
    close_api = CloseAPI()

    # Generate timestamp for unique identification
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    created_leads = []
    failed_leads = []

    for i in range(count):
        # Generate unique email with timestamp and index
        email = f"lance+timeout+{timestamp}+{i}@whiteboardgeeks.com"

        try:
            lead_data = close_api.create_test_lead(
                email=email,
                first_name="TimeoutTestLead",
                last_name=str(i),
                custom_fields={
                    "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o": f"Timeout Test Company {i}",  # Company
                    "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9": f"Timeout Test Location {timestamp}",  # Date & Location
                },
                include_date_location=False,  # We're setting it manually above
            )

            # Store just the essential data we need for testing
            lead_info = {
                "id": lead_data["id"],
                "email": email,
                "name": f"TimeoutTestLead {i}",
                "created_at": lead_data.get("date_created", datetime.now().isoformat()),
            }
            created_leads.append(lead_info)

            # Progress indicator every 50 leads
            if (i + 1) % 50 == 0:
                print(
                    f"✓ Created {i + 1}/{count} test leads ({len(failed_leads)} failures)"
                )

        except Exception as e:
            print(f"✗ Failed to create lead {i}: {e}")
            failed_leads.append({"index": i, "email": email, "error": str(e)})
            # Continue with other leads even if one fails

    print("\n=== LEAD GENERATION COMPLETE ===")
    print(f"Successfully created: {len(created_leads)} leads")
    print(f"Failed to create: {len(failed_leads)} leads")

    if failed_leads:
        print(f"Failure rate: {len(failed_leads)/count*100:.1f}%")

    return created_leads, failed_leads


def save_leads_to_file(leads_data, filename="test_leads_3000.json"):
    """
    Save lead data to a JSON file for reuse.

    Args:
        leads_data (list): List of lead data dictionaries
        filename (str): Name of the file to save to
    """
    # Ensure scripts directory exists
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(scripts_dir, filename)

    # Prepare data structure for saving
    save_data = {
        "generated_at": datetime.now().isoformat(),
        "total_count": len(leads_data),
        "leads": leads_data,
        "usage_notes": [
            "This file contains lead IDs for timeout reproduction testing",
            "Each lead has: id, email, name, created_at",
            "Use load_test_leads() function to load this data in tests",
            "Do not modify this file manually",
        ],
    }

    try:
        with open(filepath, "w") as f:
            json.dump(save_data, f, indent=2)
        print(f"✓ Lead data saved to: {filepath}")
        print(f"  File size: {os.path.getsize(filepath)} bytes")
        return filepath
    except Exception as e:
        print(f"✗ Failed to save lead data: {e}")
        return None


def load_test_leads(filename="test_leads_3000.json"):
    """
    Load test leads from the JSON file.

    Args:
        filename (str): Name of the file to load from

    Returns:
        list: List of lead data dictionaries, or empty list if file not found
    """
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(scripts_dir, filename)

    if not os.path.exists(filepath):
        print(f"✗ Test leads file not found: {filepath}")
        return []

    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        leads = data.get("leads", [])
        print(f"✓ Loaded {len(leads)} test leads from: {filepath}")
        print(f"  Generated at: {data.get('generated_at', 'unknown')}")
        return leads

    except Exception as e:
        print(f"✗ Failed to load test leads: {e}")
        return []


def main():
    """Main function to generate leads and save to file."""
    print("=== CLOSE TEST LEADS GENERATOR ===")
    print(
        "This script generates 3,000 test leads in Close for timeout reproduction testing."
    )

    # Confirm before proceeding
    response = input("\nThis will create 3,000 leads in Close. Continue? (y/N): ")
    if response.lower() != "y":
        print("Operation cancelled.")
        return

    # Generate the leads
    created_leads, failed_leads = generate_test_leads(count=3000)

    if not created_leads:
        print("✗ No leads were created successfully. Exiting.")
        return

    # Save to file
    filepath = save_leads_to_file(created_leads)

    if filepath:
        print("\n=== SUCCESS ===")
        print(f"Generated {len(created_leads)} test leads and saved to file.")
        print(f"File location: {filepath}")
        print("\nTo use these leads in tests:")
        print("from scripts.generate_test_leads import load_test_leads")
        print("leads = load_test_leads()")
    else:
        print("\n=== PARTIAL SUCCESS ===")
        print(f"Generated {len(created_leads)} test leads but failed to save to file.")
        print("You may need to create the leads again.")

    # Save failed leads info if any
    if failed_leads:
        failed_filepath = (
            filepath.replace(".json", "_failures.json")
            if filepath
            else "test_leads_failures.json"
        )
        try:
            with open(failed_filepath, "w") as f:
                json.dump(
                    {
                        "failed_at": datetime.now().isoformat(),
                        "total_failures": len(failed_leads),
                        "failures": failed_leads,
                    },
                    f,
                    indent=2,
                )
            print(f"✓ Failed lead info saved to: {failed_filepath}")
        except Exception as e:
            print(f"✗ Could not save failure info: {e}")


if __name__ == "__main__":
    main()
