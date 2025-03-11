import requests
import os


class EasyPostAPI:
    def __init__(self, api_key=None):
        # Use test API key in test environment
        self.api_key = api_key or os.environ.get("EASYPOST_TEST_API_KEY")

        # Check if API key is None or empty
        if not self.api_key:
            raise ValueError(
                "EASYPOST_TEST_API_KEY is not set. Please set the EASYPOST_TEST_API_KEY environment variable."
            )

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        self.base_url = "https://api.easypost.com/v2"

    def create_webhook(self, url=None):
        """
        Create a webhook in EasyPost that will send delivery status updates to the specified URL.

        Args:
            url (str, optional): The URL to send webhook events to. If not provided,
                                will use BASE_URL/easypost/delivery_status

        Returns:
            dict: The created webhook data
        """
        # Get the base URL from environment or use default
        base_url = os.environ.get(
            "BASE_URL", "https://locust-pleased-thankfully.ngrok-free.app"
        )

        # Remove trailing slash if present
        if base_url.endswith("/"):
            base_url = base_url[:-1]

        # Use provided URL or construct default
        webhook_url = url or f"{base_url}/easypost/delivery_status"

        payload = {"webhook": {"url": webhook_url}}

        response = requests.post(
            f"{self.base_url}/webhooks", json=payload, auth=(self.api_key, "")
        )

        if response.status_code not in [200, 201]:
            raise Exception(f"Failed to create webhook: {response.text}")

        return response.json()

    def delete_webhook(self, webhook_id):
        """
        Delete a webhook from EasyPost.

        Args:
            webhook_id (str): The ID of the webhook to delete

        Returns:
            bool: True if deletion was successful
        """
        response = requests.delete(
            f"{self.base_url}/webhooks/{webhook_id}", auth=(self.api_key, "")
        )

        if response.status_code not in [200, 204]:
            raise Exception(f"Failed to delete webhook: {response.text}")

        return True

    def list_webhooks(self):
        """
        List all webhooks in EasyPost.

        Returns:
            list: List of webhook objects
        """
        response = requests.get(f"{self.base_url}/webhooks", auth=(self.api_key, ""))

        if response.status_code != 200:
            raise Exception(f"Failed to list webhooks: {response.text}")

        return response.json().get("webhooks", [])

    def create_or_update_webhook(self, url=None):
        """
        Create a webhook in EasyPost, handling duplicate URL errors by deleting existing webhooks.

        Args:
            url (str, optional): The URL to send webhook events to. If not provided,
                                will use BASE_URL/easypost/delivery_status

        Returns:
            dict: The created webhook data
        """
        # Get the base URL from environment or use default
        base_url = os.environ.get(
            "BASE_URL", "https://locust-pleased-thankfully.ngrok-free.app"
        )

        # Remove trailing slash if present
        if base_url.endswith("/"):
            base_url = base_url[:-1]

        # Use provided URL or construct default
        webhook_url = url or f"{base_url}/easypost/delivery_status"

        try:
            # First try to create the webhook
            return self.create_webhook(webhook_url)
        except Exception as e:
            error_message = str(e)
            # Check if it's a duplicate URL error
            if "WEBHOOK.DUPLICATE_URL" in error_message:
                print(f"Duplicate webhook URL detected: {webhook_url}")

                # Get all existing webhooks
                webhooks = self.list_webhooks()

                # Find and delete webhooks with the same URL
                for webhook in webhooks:
                    if webhook.get("url") == webhook_url:
                        print(
                            f"Deleting duplicate webhook with ID: {webhook.get('id')}"
                        )
                        self.delete_webhook(webhook.get("id"))

                # Try creating the webhook again
                print("Creating new webhook after deleting duplicates")
                return self.create_webhook(webhook_url)
            else:
                # If it's a different error, re-raise it
                raise
