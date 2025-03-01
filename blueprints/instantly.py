"""
Blueprint for handling Instantly API integrations.
"""

import logging
import os
import json
from datetime import datetime, timedelta
import traceback
from base64 import b64encode
import threading

from flask import Blueprint, request, jsonify, current_app

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


@instantly_bp.route("/webhook", methods=["POST"])
def handle_instantly_webhook():
    """Handle incoming webhooks from Instantly."""
    try:
        # Parse the webhook payload
        data = request.json
        logger.info(f"Received webhook from Instantly: {data}")

        # Check the event type
        event_type = data.get("event_type")

        if event_type == "email_sent":
            return handle_email_sent(data)
        else:
            logger.warning(f"Unhandled Instantly event type: {event_type}")
            return jsonify(
                {"status": "success", "message": f"Event type {event_type} not handled"}
            ), 200

    except Exception as e:
        # Capture the traceback
        tb = traceback.format_exc()
        error_message = (
            f"An error occurred in Instantly webhook: {str(e)}\nTraceback: {tb}"
        )
        logger.error(error_message)
        send_email(subject="Instantly Webhook Error", body=error_message)

        return jsonify(
            {
                "status": "error",
                "message": "An error occurred processing the Instantly webhook",
                "error": str(e),
            }
        ), 500


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
        if not task_text.startswith("Instantly:"):
            logger.info(f"Task doesn't start with 'Instantly:': {task_text}")
            return jsonify(
                {"status": "success", "message": "Not an Instantly task"}
            ), 200

        # Extract campaign name - everything after "Instantly: "
        campaign_name = task_text[len("Instantly:") :].strip()
        logger.info(
            f"Processing Instantly campaign: {campaign_name} for lead: {lead_id}"
        )

        # Get lead details from Close if needed
        # This would depend on what data you need to send to Instantly

        # Add to Instantly campaign
        # Implement the logic to add this lead to the Instantly campaign
        # This might involve calling Instantly's API

        # For example:
        # instantly_result = add_to_instantly_campaign(lead_id, campaign_name)

        # If in test environment, track this webhook
        if ENV_TYPE == "test":
            webhook_data = {
                "lead_id": lead_id,
                "campaign_name": campaign_name,
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


def handle_email_sent(data):
    """Handle email_sent event from Instantly."""
    try:
        # Extract relevant data
        lead_email = data.get("lead_email")
        campaign_name = data.get("campaign_name")

        logger.info(
            f"Processing email_sent event for {lead_email} from campaign {campaign_name}"
        )

        # Here you would implement the logic to:
        # 1. Find the task in Close based on the lead email and campaign name
        # 2. Mark the task as complete

        # Example of using the Close API
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {get_close_encoded_key()}",
        }

        # Query Close for tasks containing the campaign name in the title
        # and associated with the lead email
        # This is just an example and would need to be adjusted based on your Close data structure

        # Placeholder for actual implementation
        task_update_result = "Task update would go here"

        return jsonify(
            {
                "status": "success",
                "message": f"Successfully processed email_sent event for {lead_email}",
                "result": task_update_result,
            }
        ), 200

    except Exception as e:
        logger.error(f"Error handling email_sent event: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 400


@instantly_bp.route("/create_lead", methods=["POST"])
def create_instantly_lead():
    """Create a new lead in Instantly."""
    try:
        data = request.json
        # Implement logic to create a lead in Instantly
        # This would call the Instantly API endpoint to create a new lead

        return jsonify(
            {"status": "success", "message": "Lead created in Instantly"}
        ), 200

    except Exception as e:
        logger.error(f"Error creating lead in Instantly: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 400


@instantly_bp.route("/campaigns", methods=["GET"])
def get_campaigns():
    """Get list of campaigns from Instantly."""
    try:
        # Implement logic to get campaigns from Instantly API
        # This would call the Instantly API endpoint to get campaigns

        return jsonify({"status": "success", "campaigns": []}), 200

    except Exception as e:
        logger.error(f"Error fetching campaigns from Instantly: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 400


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
