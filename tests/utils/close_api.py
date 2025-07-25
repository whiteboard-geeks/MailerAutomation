import requests
import os
import time
from base64 import b64encode
from datetime import datetime, timedelta
import json

from tenacity import retry, stop_after_attempt, wait_fixed
from close_utils import create_email_search_query, search_close_leads


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

    def _make_request_with_retry(self, method, url, max_retries=3, **kwargs):
        """
        Make a request with automatic retry for rate limiting (429 responses).

        Args:
            method (str): HTTP method ('GET', 'POST', 'PUT', 'DELETE')
            url (str): Request URL
            max_retries (int): Maximum number of retries for rate limiting
            **kwargs: Additional arguments passed to requests

        Returns:
            requests.Response: The response object

        Raises:
            Exception: If the request fails after all retries
        """
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # Make the request
                response = getattr(requests, method.lower())(url, **kwargs)

                # If we get a 429 (rate limited), check for retry
                if response.status_code == 429:
                    if retry_count < max_retries:
                        # Parse rate limit headers - Close API uses "RateLimit" header
                        # Format: "limit=240, remaining=238, reset=8"
                        rate_limit_header = response.headers.get("RateLimit", "")
                        retry_after_header = response.headers.get("retry-after", "")

                        reset_seconds = None

                        # First try to parse the RateLimit header for reset time
                        if rate_limit_header:
                            parts = rate_limit_header.split(",")
                            for part in parts:
                                part = part.strip()
                                if part.startswith("reset="):
                                    try:
                                        reset_seconds = float(
                                            part.split("=")[1].strip()
                                        )
                                        break
                                    except (ValueError, IndexError):
                                        pass

                        # Fall back to retry-after header if available
                        if reset_seconds is None and retry_after_header:
                            try:
                                reset_seconds = float(retry_after_header)
                            except (ValueError, TypeError):
                                pass

                        # If we couldn't parse the reset time, use a default wait
                        if reset_seconds is None:
                            reset_seconds = 60  # Default to 60 seconds

                        print(
                            f"Rate limited (429). Waiting {reset_seconds} seconds before retry {retry_count + 1}/{max_retries}"
                        )
                        time.sleep(reset_seconds)
                        retry_count += 1
                        continue
                    else:
                        # Exceeded max retries for rate limiting
                        raise Exception(
                            f"Request failed after {max_retries} retries due to rate limiting"
                        )

                # Return the response for any other status code (including success)
                return response

            except requests.exceptions.RequestException as e:
                if retry_count < max_retries:
                    print(
                        f"Request failed: {e}. Retrying {retry_count + 1}/{max_retries}"
                    )
                    time.sleep(2)  # Short delay for connection errors
                    retry_count += 1
                    continue
                else:
                    raise e

        # This should never be reached due to the logic above
        raise Exception(
            f"Request failed after {max_retries} retries due to rate limiting"
        )

    def create_test_lead(
        self,
        email=None,
        first_name=None,
        last_name=None,
        email_suffix=None,
        custom_fields=None,
        include_date_location=True,
        consultant=None,
    ):
        """Create a test lead in Close.

        Args:
            email (str, optional): Contact email
            first_name (str, optional): Contact first name
            last_name (str, optional): Contact last name
            email_suffix (str, optional): Suffix for generated email
            custom_fields (dict, optional): Additional custom fields
            include_date_location (bool): Whether to include date/location field
            consultant (str, optional): Consultant name for custom field
        """
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

        # Add consultant field if provided
        if consultant:
            payload["custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi"] = (
                consultant
            )

        # Add any custom fields provided
        if custom_fields:
            payload.update(custom_fields)

        response = self._make_request_with_retry(
            "POST", f"{self.base_url}/lead/", json=payload, headers=self.headers
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
            response = self._make_request_with_retry(
                "POST", f"{self.base_url}/webhook/", json=payload, headers=self.headers
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

        response = self._make_request_with_retry(
            "POST", f"{self.base_url}/task/", json=payload, headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to create task: {response.text}")

        return response.json()

    def delete_lead(self, lead_id):
        """Delete a lead from Close."""
        response = self._make_request_with_retry(
            "DELETE", f"{self.base_url}/lead/{lead_id}/", headers=self.headers
        )

        try:
            response_data = response.json()
            return response_data  # Should be {} for successful deletion
        except ValueError:  # If response is not JSON
            raise Exception(f"Failed to delete lead: {response.text}")

        return True

    def delete_webhook(self, webhook_id):
        """Delete a webhook from Close."""
        response = self._make_request_with_retry(
            "DELETE", f"{self.base_url}/webhook/{webhook_id}", headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to delete webhook: {response.text}")

        return True

    def get_task(self, task_id):
        """Get a task by ID from Close."""
        response = self._make_request_with_retry(
            "GET", f"{self.base_url}/task/{task_id}/", headers=self.headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get task: {response.text}")

        return response.json()

    def get_lead_email_activities(self, lead_id):
        """Get email activities for a lead from Close."""
        url = f"{self.base_url}/activity/email/"
        params = {"lead_id": lead_id}

        response = self._make_request_with_retry(
            "GET", url, headers=self.headers, params=params
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get email activities: {response.text}")

        return response.json()["data"]

    def get_lead_tasks(self, lead_id):
        """Get tasks for a lead from Close."""
        url = f"{self.base_url}/task/"
        params = {"lead_id": lead_id}

        response = self._make_request_with_retry(
            "GET", url, headers=self.headers, params=params
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get tasks: {response.text}")

        return response.json()["data"]

    def get_lead(self, lead_id):
        """Get a lead by ID from Close."""
        response = self._make_request_with_retry(
            "GET", f"{self.base_url}/lead/{lead_id}/", headers=self.headers
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
        response = self._make_request_with_retry(
            "GET", f"{self.base_url}/contact/{contact_id}/", headers=self.headers
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
        response = self._make_request_with_retry(
            "POST",
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

        response = self._make_request_with_retry(
            "GET",
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
        response = self._make_request_with_retry(
            "GET",
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
            response = self._make_request_with_retry(
                "POST",
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

        response = self._make_request_with_retry(
            "POST",
            f"{self.base_url}/data/search/",
            json=search_query,
            headers=self.headers,
        )

        if response.status_code != 200:
            raise Exception(f"Failed to search leads: {response.text}")

        return response.json().get("data", [])

    def get_lead_custom_activities(self, lead_id, activity_type_id):
        """Get custom activities for a lead filtered by activity type.

        Args:
            lead_id (str): The lead ID to get activities for
            activity_type_id (str): The custom activity type ID to filter by

        Returns:
            list: List of custom activities matching the criteria
        """
        params = {"lead_id": lead_id, "custom_activity_type_id": activity_type_id}

        response = self._make_request_with_retry(
            "GET",
            f"{self.base_url}/activity/custom/",
            params=params,
            headers=self.headers,
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get custom activities: {response.text}")

        return response.json().get("data", [])

    @retry(stop=stop_after_attempt(6), wait=wait_fixed(5))
    def wait_for_lead_by_email(self, email: str) -> None:
        """Wait until a lead can be found in Close CRM by email.

        Args:
            email (str): The email address to search for.

        Returns:
            None
        """
        query = create_email_search_query(email)
        leads = search_close_leads(query)
        if len(leads) == 0:
            raise Exception(f"Failed to find lead with email: {email}")
