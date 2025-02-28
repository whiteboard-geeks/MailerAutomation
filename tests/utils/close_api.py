import requests
import os
from base64 import b64encode
from datetime import datetime, timedelta


class CloseAPI:
    def __init__(self, api_key=None):
        # Use test API key in test environment
        self.api_key = api_key or os.environ.get("CLOSE_API_KEY")
        self.encoded_key = b64encode(f"{self.api_key}:".encode()).decode()
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.encoded_key}",
        }
        self.base_url = "https://api.close.com/api/v1"

    def create_test_lead(self, email_suffix=None):
        """Create a test lead in Close."""
        # Generate unique email to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format as YYYYMMDDhhmmss
        email_suffix = email_suffix or timestamp
        email = f"lance+instantly{email_suffix}@whiteboardgeeks.com"

        payload = {
            "name": f"Test Instantly{timestamp}",
            "contacts": [
                {
                    "name": f"Lance Instantly{timestamp}",
                    "emails": [{"email": email, "type": "office"}],
                }
            ],
            "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9": "2/27 to Richmond, VA",  # Date & Location Mailer Delivered
            "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o": "InstantlyTest",  # Company
            "status_id": "stat_vlsrwwLdhID2Gl4Csn8UFeFc5RhzzJDBmoUHNngYV1E",  # Test
        }

        response = requests.post(
            f"{self.base_url}/lead/", json=payload, headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to create test lead: {response.text}")

        return response.json()

    def create_task_for_lead(self, lead_id, campaign_name):
        """Create a task with Instantly campaign name for a lead."""
        payload = {
            "lead_id": lead_id,
            "text": f"Instantly: {campaign_name}",
            "due_date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
        }

        response = requests.post(
            f"{self.base_url}/task/", json=payload, headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to create task: {response.text}")

        return response.json()

    def delete_lead(self, lead_id):
        """Delete a lead from Close."""
        response = requests.delete(
            f"{self.base_url}/lead/{lead_id}/", headers=self.headers
        )

        if response.status_code != 204:
            raise Exception(f"Failed to delete lead: {response.text}")

        return True
