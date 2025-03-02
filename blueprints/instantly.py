"""
Blueprint for handling Instantly API integrations.
"""

import logging
import os
from datetime import datetime, timedelta
import traceback
from base64 import b64encode
import threading
import re
import requests
import time

from flask import Blueprint, request, jsonify, current_app

from close_utils import get_lead_by_id

# Set up blueprint
instantly_bp = Blueprint("instantly", __name__)

# Configure logging
logger = logging.getLogger(__name__)


# Track processed webhooks for testing purposes (only used in test environment)
# Using a class to manage lifecycle and memory usage
class WebhookTracker:
    def __init__(self, max_size=100, expiration_minutes=30):
        self.webhooks = {}
        self.lock = threading.Lock()
        self.max_size = max_size
        self.expiration_minutes = expiration_minutes
        self.last_cleanup = datetime.now()

    def add(self, task_id, data):
        """Add a processed webhook to the tracker."""
        with self.lock:
            # Add timestamp if not provided
            if "timestamp" not in data:
                data["timestamp"] = datetime.now().isoformat()

            self.webhooks[task_id] = data

            # Clean up if we've hit max size or it's been a while
            if len(self.webhooks) > self.max_size or (
                datetime.now() - self.last_cleanup
            ) > timedelta(minutes=5):
                self._cleanup()

    def get(self, task_id):
        """Get information about a processed webhook."""
        with self.lock:
            return self.webhooks.get(task_id, {})

    def get_all(self):
        """Get all processed webhooks (for debugging)."""
        with self.lock:
            return {k: v for k, v in self.webhooks.items()}

    def _cleanup(self):
        """Remove old webhooks to prevent memory leaks."""
        now = datetime.now()
        self.last_cleanup = now

        # Convert to list first to avoid modifying dict during iteration
        to_remove = []
        for task_id, data in self.webhooks.items():
            try:
                # Parse the timestamp and check if it's expired
                timestamp = datetime.fromisoformat(data["timestamp"])
                if (now - timestamp) > timedelta(minutes=self.expiration_minutes):
                    to_remove.append(task_id)
            except (KeyError, ValueError):
                # If there's any problem with the timestamp, consider it expired
                to_remove.append(task_id)

        # Remove expired entries
        for task_id in to_remove:
            del self.webhooks[task_id]

        logger.debug(
            f"Webhook tracker cleanup: removed {len(to_remove)} items, {len(self.webhooks)} remaining"
        )


# Create the webhook tracker instance
_webhook_tracker = WebhookTracker()

# Get API keys from environment
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = None  # This will be initialized when it's needed
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY")
INSTANTLY_API_KEY = os.environ.get("INSTANTLY_API_KEY")
ENV_TYPE = os.environ.get("ENV_TYPE", "development")


def get_close_encoded_key():
    """Get Base64 encoded Close API key."""
    return b64encode(f"{CLOSE_API_KEY}:".encode()).decode()


def send_email(subject, body, **kwargs):
    """Send email notification through Mailgun."""
    # Access the send_email function from the main app
    return current_app.send_email(subject, body, **kwargs)


def get_instantly_campaign_name(task_text):
    """
    Extract the campaign name from a Close task text.

    This function removes "Instantly" and any trailing non-space characters
    (like ":", "!", "--") and returns the rest of the text as the campaign name.

    Args:
        task_text (str): The text of the task from Close

    Returns:
        str: The extracted campaign name
    """
    if not task_text:
        return ""

    # First check if task starts with "Instantly"
    if not task_text.lower().startswith("instantly"):
        return task_text

    # Try to match pattern with a separator (Instantly: Test or Instantly:Test)
    match = re.search(r"^Instantly[:!,\-\s]+(.*)$", task_text)
    if match:
        return match.group(1).strip()

    # Handle case where there is no separator (InstantlyTest)
    # For this case, we want to return empty string
    if re.match(r"^Instantly[a-zA-Z0-9]", task_text):
        return ""

    # Fallback - just remove "Instantly" prefix
    remaining = task_text[len("Instantly") :].strip()
    return remaining


def get_instantly_campaigns(limit=100, starting_after=None, fetch_all=False):
    """
    Get campaigns from Instantly with cursor-based pagination support.

    Args:
        limit (int): Maximum number of items to return
        starting_after (str): Cursor for fetching the next page (campaign ID)
        fetch_all (bool): Whether to fetch all pages

    Returns:
        dict: A dictionary containing all campaigns with their details
              or an error message if the request failed
    """
    # Correct endpoint URL based on the API documentation
    url = "https://api.instantly.ai/api/v2/campaigns"

    if not INSTANTLY_API_KEY:
        error_msg = "Instantly API key is not configured"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
    }

    # Parameters for cursor-based pagination
    params = {"limit": limit}

    # Add starting_after parameter if provided
    if starting_after:
        params["starting_after"] = starting_after

    try:
        if fetch_all:
            # Fetch all pages using cursor-based pagination
            all_campaigns = []
            current_cursor = starting_after
            has_more = True

            while has_more:
                # Update cursor for next page
                if current_cursor:
                    params["starting_after"] = current_cursor
                elif "starting_after" in params and not current_cursor:
                    # Remove starting_after for first page if cursor is None
                    del params["starting_after"]

                # Make request
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                # Extract campaigns from this page
                page_campaigns = data.get("items", [])
                all_campaigns.extend(page_campaigns)

                # Get cursor for next page
                current_cursor = data.get("next_starting_after")

                # If no next cursor, we've reached the end
                if not current_cursor:
                    has_more = False
                else:
                    # Add a small delay to avoid rate limiting
                    time.sleep(0.5)

            # Return combined results
            return {
                "status": "success",
                "campaigns": all_campaigns,
                "count": len(all_campaigns),
            }
        else:
            # Fetch single page
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract campaigns from the response
            campaigns = data.get("items", [])
            next_cursor = data.get("next_starting_after")

            return {
                "status": "success",
                "campaigns": campaigns,
                "count": len(campaigns),
                "pagination": {
                    "limit": limit,
                    "next_starting_after": next_cursor,
                    "has_more": bool(next_cursor),
                },
            }
    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching campaigns from Instantly: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}


def campaign_exists(campaign_name):
    """
    Check if a campaign with the given name exists in Instantly.

    Args:
        campaign_name (str): The name of the campaign to check

    Returns:
        dict: A dictionary containing:
            - exists (bool): Whether the campaign exists
            - campaign_id (str, optional): The ID of the campaign if it exists
            - error (str, optional): Error message if an error occurred
    """
    if not campaign_name:
        return {"exists": False, "error": "No campaign name provided"}

    # Get all campaigns from Instantly (fetch all pages)
    campaigns_response = get_instantly_campaigns(fetch_all=True)

    # Check if there was an error getting campaigns
    if campaigns_response.get("status") == "error":
        return {
            "exists": False,
            "error": campaigns_response.get("message", "Unknown error occurred"),
        }

    # Extract campaigns from response
    campaigns = campaigns_response.get("campaigns", [])

    # Look for a campaign with matching name
    # Case-insensitive comparison for more flexibility
    for campaign in campaigns:
        if campaign.get("name", "").lower() == campaign_name.lower():
            return {
                "exists": True,
                "campaign_id": campaign.get("id"),
                "campaign_data": campaign,
            }

    # If we get here, no campaign with that name was found
    return {"exists": False}


@instantly_bp.route("/add_task", methods=["POST"])
def add_task_to_instantly():
    """Handle webhooks from Close when a task is created with 'Instantly:' prefix."""
    try:
        # Parse the webhook payload
        data = request.json
        logger.info(f"Received webhook from Close for task creation: {data}")

        # Extract the event data
        event = data.get("event", {})
        action = event.get("action")

        # Verify this is a task creation event
        if action != "created" or event.get("object_type") != "task.lead":
            logger.warning(f"Received non-task-creation event: {action}")
            return jsonify(
                {"status": "success", "message": "Not a task creation event"}
            ), 200

        # Get the task data
        task_data = event.get("data", {})
        task_id = task_data.get("id")
        task_text = task_data.get("text", "")
        lead_id = task_data.get("lead_id")

        # Check if this is an Instantly task
        if not task_text.lower().startswith("instantly"):
            logger.info(f"Task doesn't start with 'Instantly': {task_text}")
            return jsonify(
                {"status": "success", "message": "Not an Instantly task"}
            ), 200

        # Extract campaign name using our helper function
        campaign_name = get_instantly_campaign_name(task_text)

        # Make sure we have a campaign name
        if not campaign_name:
            logger.warning(f"Could not extract campaign name from task: {task_text}")
            return jsonify(
                {"status": "error", "message": "No campaign name found in task text"}
            ), 400

        logger.info(
            f"Processing Instantly campaign: {campaign_name} for lead: {lead_id}"
        )

        # Check if the campaign exists in Instantly
        campaign_check = campaign_exists(campaign_name)

        if not campaign_check.get("exists"):
            error_msg = f"Campaign '{campaign_name}' does not exist in Instantly"
            if "error" in campaign_check:
                error_msg = f"{error_msg}: {campaign_check['error']}"

            logger.warning(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 404

        # Campaign exists, so get the campaign ID
        campaign_id = campaign_check.get("campaign_id")
        logger.info(f"Found Instantly campaign: {campaign_name} with ID: {campaign_id}")

        # Get lead details from Close
        lead_details = get_lead_by_id(lead_id)
        if not lead_details:
            error_msg = f"Could not retrieve lead details for lead ID: {lead_id}"
            logger.warning(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 404

        logger.info(f"Retrieved lead details for lead ID: {lead_id}")

        # Add to Instantly campaign
        # Implement the logic to add this lead to the Instantly campaign
        # This might involve calling Instantly's API

        # For example:
        # instantly_result = add_to_instantly_campaign(lead_id, campaign_name, campaign_id)

        # If in test environment, track this webhook
        if ENV_TYPE == "test":
            webhook_data = {
                "lead_id": lead_id,
                "campaign_name": campaign_name,
                "campaign_id": campaign_id,
                "processed": True,
                "timestamp": datetime.now().isoformat(),
            }

            # Track in memory (with expiration)
            _webhook_tracker.add(task_id, webhook_data)

            logger.info(f"Recorded task {task_id} as processed for testing")

        # For now, just return success
        return jsonify(
            {
                "status": "success",
                "message": f"Task added to Instantly campaign: {campaign_name}",
                "lead_id": lead_id,
                "task_id": task_id,
                "campaign_name": campaign_name,
                "campaign_id": campaign_id,
            }
        ), 200

    except Exception as e:
        # Capture the traceback
        tb = traceback.format_exc()
        error_message = (
            f"Error processing Close task webhook: {str(e)}\nTraceback: {tb}"
        )
        logger.error(error_message)
        send_email(subject="Close Task Webhook Error", body=error_message)

        return jsonify(
            {
                "status": "error",
                "message": "An error occurred processing the Close task webhook",
                "error": str(e),
            }
        ), 500


# Testing endpoints - only available in test environment
@instantly_bp.route("/test/webhooks", methods=["GET"])
def get_processed_webhooks():
    """Get all processed webhooks for testing purposes."""
    # Get task_id from query parameters if provided
    task_id = request.args.get("task_id")

    if task_id:
        # Return data for specific task
        webhook_data = _webhook_tracker.get(task_id)
        if webhook_data:
            return jsonify({"status": "success", "data": webhook_data}), 200
        else:
            return jsonify(
                {
                    "status": "not_found",
                    "message": f"No webhook data found for task_id: {task_id}",
                }
            ), 404
    else:
        # Return all webhooks (limited by WebhookTracker's internal max size and expiration)
        return jsonify({"status": "success", "data": _webhook_tracker.get_all()}), 200


@instantly_bp.route("/campaigns", methods=["GET"])
def list_instantly_campaigns():
    """
    List all campaigns from Instantly or check if a specific campaign exists.

    Query Parameters:
        name (optional): The name of a campaign to check for existence
        limit (optional): Maximum number of items to return (default: 100)
        starting_after (optional): Campaign ID cursor for pagination
        fetch_all (optional): Whether to fetch all pages (default: false)

    Returns:
        JSON response with campaign data or existence check result
    """
    campaign_name = request.args.get("name")

    if campaign_name:
        # Check if specific campaign exists
        result = campaign_exists(campaign_name)
        return jsonify(result), 200 if result.get("exists", False) else 404
    else:
        # Get pagination parameters
        try:
            limit = int(request.args.get("limit", 100))
            starting_after = request.args.get("starting_after")
            fetch_all = request.args.get("fetch_all", "").lower() == "true"
        except ValueError:
            return jsonify(
                {"status": "error", "message": "Invalid pagination parameters"}
            ), 400

        # List campaigns with cursor-based pagination
        campaigns = get_instantly_campaigns(
            limit=limit, starting_after=starting_after, fetch_all=fetch_all
        )
        return jsonify(campaigns), 200 if campaigns.get("status") != "error" else 500
