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

    def create_test_lead(
        self, email=None, first_name=None, last_name=None, email_suffix=None
    ):
        """Create a test lead in Close."""
        # Generate unique email to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format as YYYYMMDDhhmmss
        email_suffix = email_suffix or timestamp

        # Use provided email or generate one
        if not email:
            email = f"lance+instantly{email_suffix}@whiteboardgeeks.com"

        # Use provided names or generate ones
        if not first_name:
            first_name = f"Lance Instantly{timestamp}"
        if not last_name:
            last_name = "Test"

        payload = {
            "name": f"{first_name} {last_name}",
            "contacts": [
                {
                    "name": f"{first_name} {last_name}",
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

    def create_webhook_to_catch_task_created(self):
        """Create a webhook to catch task created events."""
        payload = {
            "url": "http://locust-pleased-thankfully.ngrok-free.app/instantly/add_task",
            "events": [
                {
                    "object_type": "task.lead",
                    "action": "created",
                    "extra_filter": {
                        "type": "field_accessor",
                        "field": "data",
                        "filter": {
                            "type": "field_accessor",
                            "field": "text",
                            "filter": {"type": "contains", "value": "Instantly"},
                        },
                    },
                }
            ],
            "verify_ssl": False,
        }

        retry_count = 0
        max_retries = 1

        while retry_count <= max_retries:
            response = requests.post(
                f"{self.base_url}/webhook/", json=payload, headers=self.headers
            )

            if response.status_code == 201:
                webhook_id = response.json()["id"]
                return webhook_id
            elif response.status_code == 400 and retry_count < max_retries:
                # Check if error is due to duplicate webhook
                error_data = response.json()
                error_message = error_data.get("message", "")

                if "Duplicate active subscription" in error_message:
                    # Extract webhook ID using string manipulation
                    import re

                    webhook_match = re.search(r"whsub_[a-zA-Z0-9]+", error_message)

                    if webhook_match:
                        duplicate_webhook_id = webhook_match.group(0)
                        print(f"Found duplicate webhook: {duplicate_webhook_id}")

                        # Delete the duplicate webhook
                        self.delete_webhook(duplicate_webhook_id)
                        print(f"Deleted duplicate webhook: {duplicate_webhook_id}")

                        # Increment retry counter
                        retry_count += 1
                        continue

            # If we get here, either it's not a duplicate webhook error or we've exceeded retries
            raise Exception(f"Failed to create webhook: {response.text}")

        # This should never be reached due to the exception above
        return None

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

    def delete_webhook(self, webhook_id):
        """Delete a webhook from Close."""
        response = requests.delete(
            f"{self.base_url}/webhook/{webhook_id}", headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to delete webhook: {response.text}")

        return True
