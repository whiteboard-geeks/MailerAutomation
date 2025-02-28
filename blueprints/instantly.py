"""
Blueprint for handling Instantly API integrations.
"""

import logging
import os
import json
from datetime import datetime
import traceback
from base64 import b64encode

from flask import Blueprint, request, jsonify, current_app
import requests
import pytz

# Set up blueprint
instantly_bp = Blueprint("instantly", __name__)

# Configure logging
logger = logging.getLogger(__name__)

# Get API keys from environment
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = None  # This will be initialized when it's needed
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY")


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
        # Check for API key for security (optional)
        api_key = request.headers.get("X-API-KEY")
        if api_key != WEBHOOK_API_KEY:
            return jsonify({"status": "error", "message": "Unauthorized access"}), 401

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
