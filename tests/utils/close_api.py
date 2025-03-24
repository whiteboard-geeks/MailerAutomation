import requests
import os
from base64 import b64encode
from datetime import datetime, timedelta
import json


class CloseAPI:
    def __init__(self, api_key=None):
        # Use test API key in test environment
        self.api_key = api_key or os.environ.get("CLOSE_API_KEY")
        print(f"CLOSE_API_KEY environment variable: {os.environ.get('CLOSE_API_KEY')}")
        print(f"API key being used: {self.api_key}")

        # Check if API key is None or empty
        if not self.api_key:
            raise ValueError(
                "CLOSE_API_KEY is not set. Please set the CLOSE_API_KEY environment variable."
            )

        self.encoded_key = b64encode(f"{self.api_key}:".encode()).decode()
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.encoded_key}",
        }
        self.base_url = "https://api.close.com/api/v1"

    def create_test_lead(
        self,
        email=None,
        first_name=None,
        last_name=None,
        email_suffix=None,
        custom_fields=None,
        include_date_location=True,
    ):
        """Create a test lead in Close."""
        # Generate unique email to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format as YYYYMMDDhhmmss
        email_suffix = email_suffix or timestamp

        # Get environment type for debugging
        env_type = os.environ.get("ENV_TYPE", "development")

        # Use provided email or generate one
        if not email:
            email = f"lance+{env_type}.instantly{email_suffix}@whiteboardgeeks.com"

        # Use provided names or generate ones
        if not first_name:
            first_name = f"contactFname{timestamp}"
        if not last_name:
            last_name = "contactLname"

        payload = {
            "name": f"CompanyFname{timestamp} CompanyLname{timestamp}",
            "contacts": [
                {
                    "name": f"{first_name} {last_name}",
                    "emails": [{"email": email, "type": "office"}],
                }
            ],
            "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o": "InstantlyTest",  # Company
            "status_id": "stat_vlsrwwLdhID2Gl4Csn8UFeFc5RhzzJDBmoUHNngYV1E",  # Test
        }

        # Add Date & Location Mailer Delivered field if requested
        if include_date_location:
            payload["custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9"] = (
                "2/27 to Richmond, VA"  # Date & Location Mailer Delivered
            )

        # Add any custom fields provided
        if custom_fields:
            payload.update(custom_fields)

        response = requests.post(
            f"{self.base_url}/lead/", json=payload, headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to create test lead: {response.text}")

        return response.json()

    def create_webhook_to_catch_task_created(self):
        """Create a webhook to catch task created events."""
        # Get the base URL from environment or use ngrok for development
        base_url = os.environ.get(
            "BASE_URL", "http://locust-pleased-thankfully.ngrok-free.app"
        )

        # Remove trailing slash if present
        if base_url.endswith("/"):
            base_url = base_url[:-1]

        webhook_url = f"{base_url}/instantly/add_lead"

        payload = {
            "url": webhook_url,
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

        try:
            response_data = response.json()
            return response_data  # Should be {} for successful deletion
        except ValueError:  # If response is not JSON
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

    def get_task(self, task_id):
        """Get a task by ID from Close."""
        response = requests.get(
            f"{self.base_url}/task/{task_id}/", headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get task: {response.text}")

        return response.json()

    def get_lead_email_activities(self, lead_id):
        """Get email activities for a lead from Close."""
        url = f"{self.base_url}/activity/email/"
        params = {"lead_id": lead_id}

        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to get email activities: {response.text}")

        return response.json()["data"]

    def get_lead_tasks(self, lead_id):
        """Get tasks for a lead from Close."""
        url = f"{self.base_url}/task/"
        params = {"lead_id": lead_id}

        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to get tasks: {response.text}")

        return response.json()["data"]

    def get_lead(self, lead_id):
        """Get a lead by ID from Close."""
        response = requests.get(
            f"{self.base_url}/lead/{lead_id}/", headers=self.headers
        )
        if response.status_code != 200:
            raise Exception(f"Failed to get lead: {response.text}")

        return response.json()

    def subscribe_contact_to_sequence(self, contact_id, sequence_id):
        """
        Subscribe a contact to a sequence.

        Args:
            contact_id (str): The contact ID to subscribe
            sequence_id (str): The sequence ID

        Returns:
            dict: The created subscription data
        """
        # Get the contact details to get the email
        response = requests.get(
            f"{self.base_url}/contact/{contact_id}/", headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get contact details: {response.text}")

        contact = response.json()

        # Get first email from contact
        contact_email = None
        for email in contact.get("emails", []):
            contact_email = email.get("email")
            if contact_email:
                break

        if not contact_email:
            raise Exception(f"Contact {contact_id} does not have an email address")

        # Minimal payload needed for subscription
        payload = {
            "sequence_id": sequence_id,
            "contact_id": contact_id,
            "contact_email": contact_email,
        }

        print(f"Sending subscription request with payload: {payload}")
        response = requests.post(
            f"{self.base_url}/sequence_subscription/",
            json=payload,
            headers=self.headers,
        )

        print(f"Response status code: {response.status_code}")

        # Parse the response data
        try:
            response_data = response.json()
            print(f"Response includes ID: {response_data.get('id')}")
            print(f"Response includes status: {response_data.get('status')}")

            # If we get an ID and the status is 'active', consider it a success
            if response_data.get("id") and response_data.get("status") == "active":
                print("Subscription created successfully!")
                return response_data
            else:
                print(f"Subscription may have failed. Full response: {response_data}")
                raise Exception(
                    f"Failed to subscribe contact to sequence: {response.text}"
                )
        except ValueError as e:
            print(f"Error parsing JSON response: {e}")
            raise Exception(
                f"Failed to subscribe contact to sequence - invalid JSON response: {response.text}"
            )

    def get_sequence_subscriptions(self, lead_id=None, contact_id=None):
        """
        Get sequence subscriptions for a lead or contact.

        Args:
            lead_id (str, optional): Lead ID to get subscriptions for
            contact_id (str, optional): Contact ID to get subscriptions for

        Returns:
            list: List of subscription objects
        """
        params = {}
        if lead_id:
            params["lead_id"] = lead_id
        if contact_id:
            params["contact_id"] = contact_id

        if not params:
            raise ValueError("Either lead_id or contact_id must be provided")

        response = requests.get(
            f"{self.base_url}/sequence_subscription/",
            params=params,
            headers=self.headers,
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get sequence subscriptions: {response.text}")

        return response.json().get("data", [])

    def check_subscription_status(self, subscription_id):
        """
        Check the status of a sequence subscription.

        Args:
            subscription_id (str): The ID of the subscription to check

        Returns:
            dict: The subscription data
        """
        response = requests.get(
            f"{self.base_url}/sequence_subscription/{subscription_id}/",
            headers=self.headers,
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get subscription status: {response.text}")

        return response.json()

    def create_webhook_for_tracking_id_and_carrier(self):
        """
        Create a webhook in Close that triggers when:
        1. A lead is created with both tracking number and carrier present
        2. A lead is updated where carrier is updated and tracking number is present
        3. A lead is updated where tracking number is updated and carrier is present
        """
        # Get base URL from environment or use default
        base_url = os.environ.get(
            "BASE_URL", "http://locust-pleased-thankfully.ngrok-free.app"
        )

        # Create webhook payload with complex filtering
        with open("tests/utils/close_webhook_delivery_info_filters.json", "r") as f:
            webhook_data = json.load(f)
        webhook_data["url"] = f"{base_url}/easypost/create_tracker"

        # Create webhook in Close with retry logic for duplicates
        retry_count = 0
        max_retries = 1

        while retry_count <= max_retries:
            response = requests.post(
                f"{self.base_url}/webhook",
                json=webhook_data,
                headers=self.headers,
            )

            if response.status_code == 201:
                return response.json()["id"]
            elif response.status_code != 201 and retry_count < max_retries:
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

    def search_leads_by_tracking_number(self, tracking_number):
        """Search for leads with a specific tracking number.

        Args:
            tracking_number (str): The tracking number to search for

        Returns:
            list: List of matching leads
        """
        search_query = {
            "limit": None,
            "query": {
                "negate": False,
                "queries": [
                    {"negate": False, "object_type": "lead", "type": "object_type"},
                    {
                        "negate": False,
                        "queries": [
                            {
                                "condition": {
                                    "mode": "exact_value",
                                    "type": "text",
                                    "value": tracking_number,
                                },
                                "field": {
                                    "custom_field_id": "cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii",
                                    "type": "custom_field",
                                },
                                "negate": False,
                                "type": "field_condition",
                            }
                        ],
                        "type": "and",
                    },
                ],
                "type": "and",
            },
        }

        response = requests.post(
            f"{self.base_url}/data/search/", json=search_query, headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to search leads: {response.text}")

        return response.json().get("data", [])
