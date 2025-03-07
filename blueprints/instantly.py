"""
Blueprint for handling Instantly API integrations.
"""

import logging
import os
from datetime import datetime
import traceback
from base64 import b64encode
import re
import requests
import time
import json
from redis import Redis

from flask import Blueprint, request, jsonify, current_app

from close_utils import (
    get_lead_by_id,
    search_close_leads,
    get_close_headers,
    create_email_search_query,
)

# Set up blueprint
instantly_bp = Blueprint("instantly", __name__)

# Configure logging
logger = logging.getLogger(__name__)


def check_route_response(status_code, response_data, context=None):
    """
    Check route response status and allow for breakpoint debugging on non-200 responses.

    Args:
        status_code (int): The HTTP status code
        response_data (dict): The response data
        context (dict, optional): Additional context for debugging

    Returns:
        tuple: (response_data, status_code) unchanged
    """
    if status_code != 200:
        # This is where you can set your breakpoint
        # The status_code, response_data, and context will be available in the debugger
        logger.error(f"Non-200 response: {status_code}")
        if context:
            logger.error(f"Context: {context}")
        logger.error(f"Response data: {response_data}")

    return response_data, status_code


# Track processed webhooks using Redis for persistence across environments
class WebhookTracker:
    def __init__(self, expiration_seconds=1800):  # Default 30 minutes
        self.redis_url = os.environ.get("REDISCLOUD_URL")
        self.redis = Redis.from_url(self.redis_url) if self.redis_url else None
        self.expiration_seconds = expiration_seconds
        self.prefix = "webhook_tracker:"

        if not self.redis:
            logger.warning(
                "Redis not configured. WebhookTracker will not persist data."
            )
            self.webhooks = {}  # Fallback to in-memory if Redis not available

    def add(self, task_id, data):
        """Add a processed webhook to the tracker."""
        # Add timestamp if not provided
        if "timestamp" not in data:
            data["timestamp"] = datetime.now().isoformat()

        if self.redis:
            # Store in Redis with expiration
            key = f"{self.prefix}{task_id}"
            self.redis.setex(key, self.expiration_seconds, json.dumps(data))
            logger.info(f"Stored webhook data in Redis for task {task_id}")
        else:
            # Fallback to in-memory storage
            self.webhooks[task_id] = data
            logger.info(f"Stored webhook data in memory for task {task_id}")

    def get(self, task_id):
        """Get information about a processed webhook."""
        if self.redis:
            key = f"{self.prefix}{task_id}"
            data = self.redis.get(key)
            if data:
                return json.loads(data)
            return {}
        else:
            # Fallback to in-memory
            return self.webhooks.get(task_id, {})

    def get_all(self):
        """Get all processed webhooks (for debugging)."""
        if self.redis:
            keys = self.redis.keys(f"{self.prefix}*")
            result = {}
            for key in keys:
                task_id = key.decode("utf-8").replace(self.prefix, "")
                data = self.redis.get(key)
                if data:
                    result[task_id] = json.loads(data)
            return result
        else:
            # Fallback to in-memory
            return {k: v for k, v in self.webhooks.items()}


# Create the webhook tracker instance
_webhook_tracker = WebhookTracker()

# Get API keys from environment
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = None  # This will be initialized when it's needed
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY")
INSTANTLY_API_KEY = os.environ.get("INSTANTLY_API_KEY")
ENV_TYPE = os.environ.get("ENV_TYPE", "development")
BARBARA_USER_ID = "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as"


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
    # Case-insensitive comparison and trim whitespace for more flexibility
    for campaign in campaigns:
        if campaign.get("name", "").strip().lower() == campaign_name.strip().lower():
            return {
                "exists": True,
                "campaign_id": campaign.get("id"),
                "campaign_data": campaign,
            }

    # If we get here, no campaign with that name was found
    return {"exists": False}


@instantly_bp.route("/add_lead", methods=["POST"])
def add_lead_to_instantly():
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

            # Create Close lead URL
            close_lead_url = f"https://app.close.com/lead/{lead_id}/"

            # Send error email notification
            email_subject = f"Instantly Campaign Not Found: {campaign_name}"
            email_body = f"""
Error: Campaign not found in Instantly

Lead ID: {lead_id}
Lead URL: {close_lead_url}
Task Text: {task_text}
Campaign Name (extracted): {campaign_name}

The campaign name could not be found in Instantly. Please verify the campaign exists or check the task text format.

Error details: {error_msg}
            """

            send_email(subject=email_subject, body=email_body)

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

        # Extract first and last name from the lead details
        full_name = lead_details.get("name", "")
        first_name, last_name = split_name(full_name)

        # Get contact email
        email = None
        contacts = lead_details.get("contacts", [])
        for contact in contacts:
            emails = contact.get("emails", [])
            if emails:
                email = emails[0].get("email")
                break

        if not email:
            error_msg = f"No email found for lead ID: {lead_id}"
            logger.warning(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 400

        # Get company name and date & location from custom fields
        company_name = lead_details.get(
            "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o", ""
        )
        date_location = lead_details.get(
            "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9", ""
        )

        # Add to Instantly campaign
        instantly_result = add_to_instantly_campaign(
            campaign_id=campaign_id,
            email=email,
            first_name=first_name,
            last_name=last_name,
            company_name=company_name,
            date_location=date_location,
        )

        if instantly_result.get("status") == "error":
            error_msg = (
                f"Failed to add lead to Instantly: {instantly_result.get('message')}"
            )
            logger.error(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 500

        # Track this webhook
        webhook_data = {
            "route": "add_lead",
            "lead_id": lead_id,
            "campaign_name": campaign_name,
            "campaign_id": campaign_id,
            "processed": True,
            "timestamp": datetime.now().isoformat(),
            "instantly_result": instantly_result,
        }

        # Track in Redis (with expiration)
        _webhook_tracker.add(task_id, webhook_data)

        logger.info(f"Recorded task {task_id} as processed")

        return jsonify(
            {
                "status": "success",
                "message": f"Lead added to Instantly campaign: {campaign_name}",
                "lead_id": lead_id,
                "task_id": task_id,
                "campaign_name": campaign_name,
                "campaign_id": campaign_id,
                "instantly_result": instantly_result,
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


def split_name(full_name):
    """
    Split a full name into first name and last name.

    Args:
        full_name (str): The full name to split

    Returns:
        tuple: (first_name, last_name)
    """
    if not full_name:
        return "", ""

    # Split the name by spaces
    parts = full_name.strip().split()

    if len(parts) == 1:
        # Only one word, assume it's the first name
        return parts[0], ""
    else:
        # Assume last word is last name, everything else is first name
        return " ".join(parts[:-1]), parts[-1]


def add_to_instantly_campaign(
    campaign_id, email, first_name="", last_name="", company_name="", date_location=""
):
    """
    Add a lead to an Instantly campaign.

    Args:
        campaign_id (str): Instantly campaign ID
        email (str): Email address of the lead
        first_name (str): First name of the lead
        last_name (str): Last name of the lead
        company_name (str): Company name of the lead
        date_location (str): Date & Location Mailer Delivered value

    Returns:
        dict: API response from Instantly
    """
    if not INSTANTLY_API_KEY:
        error_msg = "Instantly API key is not configured"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}

    url = "https://api.instantly.ai/api/v2/leads"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
    }

    # Prepare payload
    payload = {
        "campaign": campaign_id,
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "company_name": company_name,
        "custom_variables": {"date_and_location_delivered": date_location},
    }

    # Remove empty fields
    for key, value in list(payload.items()):
        if value == "" and key not in [
            "first_name",
            "last_name",
        ]:  # Allow empty first/last names
            del payload[key]

    # Remove empty custom variables
    if not date_location:
        del payload["custom_variables"]

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        # Parse response
        data = response.json()
        return {
            "status": "success",
            "lead_id": data.get("id"),
            "message": "Lead added to Instantly campaign",
            "response": data,
        }
    except requests.exceptions.RequestException as e:
        error_msg = f"Error adding lead to Instantly: {str(e)}"
        if hasattr(e, "response") and e.response is not None:
            try:
                error_data = e.response.json()
                error_msg = f"{error_msg} - {error_data}"
            except (ValueError, json.JSONDecodeError, AttributeError):
                error_msg = f"{error_msg} - Status code: {e.response.status_code}"

        logger.error(error_msg)
        return {"status": "error", "message": error_msg}


# Webhook tracking endpoints - available in all environments
@instantly_bp.route("/webhooks/status", methods=["GET"])
def get_processed_webhooks():
    """Get processed webhooks for testing and monitoring purposes."""
    # Get task_id and route from query parameters if provided
    task_id = request.args.get("task_id")
    route = request.args.get("route")

    if task_id:
        # Return data for specific task
        webhook_data = _webhook_tracker.get(task_id)
        if webhook_data:
            # If route is specified, only return data for that route
            if route and webhook_data.get("route") != route:
                return jsonify(
                    {
                        "status": "not_found",
                        "message": f"No webhook data found for task_id: {task_id} and route: {route}",
                    }
                ), 404
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


@instantly_bp.route("/email_sent", methods=["POST"])
def handle_instantly_email_sent():
    """Handle webhooks from Instantly when an email is sent."""
    try:
        # Parse the webhook payload
        data = request.json
        logger.info(f"Received email sent webhook from Instantly: {data}")

        # Verify this is an email sent event
        if data.get("event_type") != "email_sent":
            logger.warning(f"Received non-email-sent event: {data.get('event_type')}")
            response_data = {"status": "success", "message": "Not an email sent event"}
            return check_route_response(200, response_data)

        # Extract relevant data from the webhook
        lead_email = data.get("lead_email")
        campaign_name = data.get("campaign_name")
        email_subject = data.get("email_subject")
        email_html = data.get("email_html")

        if not all([lead_email, campaign_name, email_subject, email_html]):
            error_msg = "Missing required fields in webhook payload"
            logger.error(error_msg)
            response_data = {"status": "error", "message": error_msg}
            return check_route_response(400, response_data)

        # Search for leads with this email
        query = create_email_search_query(lead_email)
        leads = search_close_leads(query)
        if not leads:
            error_msg = f"No lead found with email: {lead_email}"
            logger.error(error_msg)
            response_data = {"status": "error", "message": error_msg}
            return check_route_response(404, response_data, {"lead_email": lead_email})

        if len(leads) > 1:
            error_msg = f"Multiple leads found with email: {lead_email}"
            logger.error(error_msg)
            response_data = {"status": "error", "message": error_msg}
            return check_route_response(400, response_data, {"lead_email": lead_email})

        lead = leads[0]
        lead_id = lead["id"]

        # Get all tasks for the lead
        headers = get_close_headers()
        tasks_url = f"https://api.close.com/api/v1/task/?lead_id={lead_id}"
        tasks_response = requests.get(tasks_url, headers=headers)
        tasks_response.raise_for_status()
        tasks = tasks_response.json().get("data", [])

        # Find the matching task
        matching_task = None
        for task in tasks:
            if campaign_name in task.get("text", "") and not task.get("is_complete"):
                matching_task = task
                break

        if not matching_task:
            error_msg = (
                f"No matching non-completed task found for campaign: {campaign_name}"
            )
            logger.error(error_msg)
            response_data = {"status": "error", "message": error_msg}
            return check_route_response(
                404, response_data, {"lead_id": lead_id, "campaign_name": campaign_name}
            )

        # Mark the task as complete
        task_id = matching_task["id"]
        complete_url = f"https://api.close.com/api/v1/task/{task_id}/"
        complete_data = {"is_complete": True}
        complete_response = requests.put(
            complete_url, headers=headers, json=complete_data
        )
        complete_response.raise_for_status()

        # Track this webhook
        webhook_data = {
            "route": "email_sent",
            "lead_id": lead_id,
            "task_id": task_id,
            "campaign_name": campaign_name,
            "processed": True,
            "timestamp": datetime.now().isoformat(),
            "email_data": {
                "subject": email_subject,
                "to": lead_email,
                "from": data.get("email_account"),
            },
        }
        _webhook_tracker.add(task_id, webhook_data)
        logger.info(f"Recorded email sent webhook for task {task_id}")

        # Get the contact with the matching email
        lead_details = get_lead_by_id(lead_id)
        if not lead_details:
            error_msg = f"Could not retrieve lead details for lead ID: {lead_id}"
            logger.error(error_msg)
            response_data = {"status": "error", "message": error_msg}
            return check_route_response(404, response_data, {"lead_id": lead_id})

        contact = None
        for c in lead_details.get("contacts", []):
            for email in c.get("emails", []):
                if email.get("email") == lead_email:
                    contact = c
                    break
            if contact:
                break

        if not contact:
            error_msg = f"No contact found with email: {lead_email}"
            logger.error(error_msg)
            response_data = {"status": "error", "message": error_msg}
            return check_route_response(
                404, response_data, {"lead_id": lead_id, "lead_email": lead_email}
            )

        # Create email activity in Close
        email_data = {
            "contact_id": contact["id"],
            "user_id": BARBARA_USER_ID,
            "lead_id": lead_id,
            "direction": "outgoing",
            "created_by": BARBARA_USER_ID,
            "created_by_name": "Barbara Pigg",  # Hardcoded since we know it's Barbara
            "date_created": data.get("timestamp")
            .replace("Z", "+00:00")
            .replace("T", "T"),
            "subject": email_subject,
            "sender": data.get("email_account"),
            "to": [lead_email],
            "bcc": [],
            "cc": [],
            "status": "sent",
            "body_text": "",  # We don't have plain text version
            "body_html": email_html,
            "attachments": [],
            "template_id": None,
        }

        email_url = "https://api.close.com/api/v1/activity/email/"
        email_response = requests.post(email_url, headers=headers, json=email_data)
        email_response.raise_for_status()

        logger.info(
            f"Successfully processed email sent webhook for lead {lead_id} and task {task_id}"
        )

        response_data = {
            "status": "success",
            "message": "Email sent webhook processed successfully",
            "lead_id": lead_id,
            "task_id": task_id,
            "email_id": email_response.json()["id"],
        }
        return check_route_response(200, response_data)

    except Exception as e:
        # Capture the traceback
        tb = traceback.format_exc()
        error_message = (
            f"Error processing Instantly email sent webhook: {str(e)}\nTraceback: {tb}"
        )
        logger.error(error_message)
        send_email(subject="Instantly Email Sent Webhook Error", body=error_message)

        response_data = {
            "status": "error",
            "message": "An error occurred processing the Instantly email sent webhook",
            "error": str(e),
        }
        return check_route_response(500, response_data, {"error": str(e)})
