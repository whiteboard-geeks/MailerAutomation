#!/usr/bin/env python
"""
Verify and create required webhooks for the mailer automation system.
This script checks if necessary webhooks exist and creates any missing ones.
"""

import os
import json
import base64
import requests
import logging
from typing import Dict, List, Optional, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("webhook-verifier")

# Get environment variables
PRODUCTION_URL = os.environ.get("PRODUCTION_URL")
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
EASYPOST_PROD_API_KEY = os.environ.get("EASYPOST_PROD_API_KEY")

# Remove trailing slash from PRODUCTION_URL if present to avoid double slashes
if PRODUCTION_URL and PRODUCTION_URL.endswith("/"):
    PRODUCTION_URL = PRODUCTION_URL.rstrip("/")
    logger.info(f"Removed trailing slash from production URL: {PRODUCTION_URL}")

# Verification status
webhooks_verified = True


class CloseAPI:
    def __init__(self, api_key):
        """Initialize Close API client with the API key."""
        self.api_key = api_key
        self.encoded_key = base64.b64encode(f"{api_key}:".encode()).decode()
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.encoded_key}",
        }
        self.base_url = "https://api.close.com/api/v1"

    def list_webhooks(self) -> List[Dict[str, Any]]:
        """List all webhooks in Close."""
        response = requests.get(f"{self.base_url}/webhook/", headers=self.headers)

        if response.status_code != 200:
            logger.error(f"Failed to list Close webhooks: {response.text}")
            return []

        return response.json().get("data", [])

    def create_webhook_for_task_created(self) -> Optional[str]:
        """Create a webhook to catch task created events with "Instantly" prefix."""
        webhook_url = f"{PRODUCTION_URL}/instantly/add_lead"

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
                            "filter": {"type": "contains", "value": "Instantly:"},
                        },
                    },
                }
            ],
        }

        response = requests.post(
            f"{self.base_url}/webhook/", json=payload, headers=self.headers
        )

        if response.status_code == 201:
            webhook_id = response.json()["id"]
            return webhook_id
        else:
            logger.error(
                f"Failed to create Close webhook for task created: {response.text}"
            )
            return None

    def create_webhook_for_tracking_info(self) -> Optional[str]:
        """Create a webhook for tracking number and carrier updates."""
        webhook_url = f"{PRODUCTION_URL}/easypost/create_tracker"

        # Load webhook configuration with complex filters for tracking info
        try:
            with open("tests/utils/close_webhook_delivery_info_filters.json", "r") as f:
                webhook_data = json.load(f)
            webhook_data["url"] = webhook_url

            response = requests.post(
                f"{self.base_url}/webhook/", json=webhook_data, headers=self.headers
            )

            if response.status_code == 201:
                webhook_id = response.json()["id"]
                return webhook_id
            else:
                logger.error(
                    f"Failed to create Close webhook for tracking info: {response.text}"
                )
                return None
        except FileNotFoundError:
            logger.error("Failed to load webhook configuration file")
            return None


class EasyPostAPI:
    def __init__(self, api_key):
        """Initialize EasyPost API client with the API key."""
        self.api_key = api_key
        self.base_url = "https://api.easypost.com/v2"

    def list_webhooks(self) -> List[Dict[str, Any]]:
        """List all webhooks in EasyPost."""
        response = requests.get(f"{self.base_url}/webhooks", auth=(self.api_key, ""))

        if response.status_code != 200:
            logger.error(f"Failed to list EasyPost webhooks: {response.text}")
            return []

        return response.json().get("webhooks", [])

    def create_webhook(self) -> Optional[str]:
        """Create a webhook for delivery status updates."""
        webhook_url = f"{PRODUCTION_URL}/easypost/delivery_status"

        create_data = {"webhook": {"url": webhook_url, "mode": "production"}}

        response = requests.post(
            f"{self.base_url}/webhooks", auth=(self.api_key, ""), json=create_data
        )

        if response.status_code in [200, 201]:
            return response.json()["id"]
        else:
            logger.error(f"Failed to create EasyPost webhook: {response.text}")
            return None


def verify_close_webhooks() -> bool:
    """Verify and create Close webhooks."""
    global webhooks_verified

    if not CLOSE_API_KEY:
        logger.error("CLOSE_API_KEY not provided")
        webhooks_verified = False
        return False

    close_api = CloseAPI(CLOSE_API_KEY)
    success = True

    # Get existing webhooks
    logger.info("Listing existing Close webhooks...")
    existing_webhooks = close_api.list_webhooks()

    # Check for Instantly task created webhook
    instantly_webhook_url = f"{PRODUCTION_URL}/instantly/add_lead"
    instantly_webhook_exists = any(
        webhook["url"] == instantly_webhook_url for webhook in existing_webhooks
    )

    if instantly_webhook_exists:
        logger.info("✓ Close webhook for Instantly task creation already exists")
    else:
        logger.info("! Creating Close webhook for Instantly task creation...")
        webhook_id = close_api.create_webhook_for_task_created()
        if webhook_id:
            logger.info(
                f"✓ Created Close webhook for Instantly task creation with ID: {webhook_id}"
            )
        else:
            logger.error("✗ Failed to create Close webhook for Instantly task creation")
            success = False

    # Check for EasyPost tracking info webhook
    easypost_webhook_url = f"{PRODUCTION_URL}/easypost/create_tracker"
    easypost_webhook_exists = any(
        webhook["url"] == easypost_webhook_url for webhook in existing_webhooks
    )

    if easypost_webhook_exists:
        logger.info("✓ Close webhook for EasyPost tracking info already exists")
    else:
        logger.info("! Creating Close webhook for EasyPost tracking info...")
        webhook_id = close_api.create_webhook_for_tracking_info()
        if webhook_id:
            logger.info(
                f"✓ Created Close webhook for EasyPost tracking info with ID: {webhook_id}"
            )
        else:
            logger.error("✗ Failed to create Close webhook for EasyPost tracking info")
            success = False

    webhooks_verified = webhooks_verified and success
    return success


def verify_easypost_webhooks() -> bool:
    """Verify and create EasyPost webhooks."""
    global webhooks_verified

    if not EASYPOST_PROD_API_KEY:
        logger.error("EASYPOST_PROD_API_KEY not provided")
        webhooks_verified = False
        return False

    easypost_api = EasyPostAPI(EASYPOST_PROD_API_KEY)
    success = True

    # Get existing webhooks
    logger.info("Listing existing EasyPost webhooks...")
    existing_webhooks = easypost_api.list_webhooks()

    # Check for delivery status webhook
    delivery_webhook_url = f"{PRODUCTION_URL}/easypost/delivery_status"
    delivery_webhook_exists = any(
        webhook["url"] == delivery_webhook_url for webhook in existing_webhooks
    )

    if delivery_webhook_exists:
        logger.info("✓ EasyPost webhook for delivery status already exists")
    else:
        logger.info("! Creating EasyPost webhook for delivery status...")
        webhook_id = easypost_api.create_webhook()
        if webhook_id:
            logger.info(
                f"✓ Created EasyPost webhook for delivery status with ID: {webhook_id}"
            )
        else:
            logger.error("✗ Failed to create EasyPost webhook for delivery status")
            success = False

    webhooks_verified = webhooks_verified and success
    return success


def remind_about_manually_configured_webhooks():
    """Display reminder about webhooks that need manual configuration."""
    logger.warning(
        "\n⚠️  MANUAL ACTION REQUIRED ⚠️\n"
        "Some webhooks need to be configured manually in the Instantly dashboard:\n"
        f"1. Email Sent Webhook: {PRODUCTION_URL}/instantly/email_sent\n"
        f"2. Reply Received Webhook: {PRODUCTION_URL}/instantly/reply_received\n"
        "Please ensure these are properly configured in the Instantly dashboard."
    )


def main():
    """Main function to verify and create all necessary webhooks."""
    global webhooks_verified

    logger.info(f"Starting webhook verification for {PRODUCTION_URL}")

    # Verify we have a production URL
    if not PRODUCTION_URL:
        logger.error("PRODUCTION_URL environment variable not set")
        exit(1)

    # Verify Close webhooks
    verify_close_webhooks()

    # Verify EasyPost webhooks
    verify_easypost_webhooks()

    # Remind about manually configured webhooks
    remind_about_manually_configured_webhooks()

    # Report overall status
    if webhooks_verified:
        logger.info("✅ All automatically configured webhooks verified successfully!")
    else:
        logger.error("❌ Some webhooks could not be verified or created")
        exit(1)


if __name__ == "__main__":
    main()
