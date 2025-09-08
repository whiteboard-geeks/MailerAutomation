"""
Blueprint for handling Instantly API integrations.
"""

import os
from datetime import datetime
import traceback
from base64 import b64encode
import re
import requests
import time
import json
from redis import Redis
import structlog
from temporal.service import temporal
import uuid

from flask import Blueprint, request, jsonify, g

from close_utils import (
    get_lead_by_id,
    search_close_leads,
    create_email_search_query,
    get_sequence_subscriptions,
    pause_sequence_subscription,
    make_close_request,
)

# Import rate limiter
from utils.rate_limiter import RedisRateLimiter, APIRateConfig

# Import the Celery instance
from celery_worker import celery

from temporal.workflows.instantly import WebhookEmailSentWorkflow, WebhookEmailSentPaylod
from temporal.shared import TASK_QUEUE_NAME

# Set up blueprint
instantly_bp = Blueprint("instantly", __name__)

# Configure logging using structlog
logger = structlog.get_logger("instantly")

# Global rate limiter instance
_rate_limiter = None


def determine_notification_recipients(lead_details, env_type):
    """
    Determine notification recipients based on consultant field.

    Args:
        lead_details (dict): Lead details from Close API
        env_type (str): Environment type (production/development)

    Returns:
        tuple: (recipients_string, error_message)
               recipients_string is None if error or should use default
               error_message is None if success
    """
    # Get the consultant field value
    consultant_field_key = "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi"
    consultant = lead_details.get(consultant_field_key)
    lead_id = lead_details.get("id", "unknown")

    # Handle missing, empty, or null consultant field gracefully
    if consultant is None:
        logger.warning(
            "consultant_field_missing",
            lead_id=lead_id,
            message=f"Consultant field missing for lead {lead_id}. Using default recipients.",
        )
        return None, None  # Use default recipients

    if consultant == "":
        logger.warning(
            "consultant_field_empty",
            lead_id=lead_id,
            message=f"Consultant field empty for lead {lead_id}. Using default recipients.",
        )
        return None, None  # Use default recipients

    # Handle known consultants
    if consultant == "Barbara Pigg":
        # Barbara uses default notification behavior (existing team)
        logger.info(
            "consultant_determined",
            lead_id=lead_id,
            consultant="Barbara Pigg",
            recipients="default",
        )
        return None, None  # None means use default recipients

    elif consultant == "April Lowrie":
        # April's behavior depends on environment
        if env_type == "development":
            # Development: Lance only
            recipients = "lance@whiteboardgeeks.com"
            logger.info(
                "consultant_determined",
                lead_id=lead_id,
                consultant="April Lowrie",
                environment="development",
                recipients=recipients,
            )
            return recipients, None
        else:
            # Production: April's team
            recipients = "april.lowrie@whiteboardgeeks.com,noura.mahmoud@whiteboardgeeks.com,kori.watkins@whiteboardgeeks.com"
            logger.info(
                "consultant_determined",
                lead_id=lead_id,
                consultant="April Lowrie",
                environment="production",
                recipients=recipients,
            )
            return recipients, None

    else:
        # Unknown consultant - log warning but use default recipients
        logger.warning(
            "consultant_unknown",
            lead_id=lead_id,
            consultant=consultant,
            message=f"Unknown consultant '{consultant}' for lead {lead_id}. Using default recipients.",
        )
        return None, None  # Use default recipients instead of returning error


def get_rate_limiter():
    """Get or create the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        try:
            # Get Redis URL from environment
            redis_url = os.environ.get("REDISCLOUD_URL")

            if redis_url and redis_url.lower() != "null":
                _rate_limiter = RedisRateLimiter(
                    redis_url=redis_url,
                    api_config=APIRateConfig.instantly(),  # 600 req/min = 10 req/sec
                    safety_factor=0.8,  # 80% of limit = 8 req/sec effective
                    fallback_on_redis_error=True,  # Allow requests if Redis fails
                )
                logger.info(f"Rate limiter initialized: {_rate_limiter}")
            else:
                logger.warning("Redis not configured, rate limiter disabled")
                _rate_limiter = None
        except Exception as e:
            logger.warning(f"Failed to initialize rate limiter: {e}")
            _rate_limiter = None

    return _rate_limiter


def log_webhook_response(status_code, response_data, webhook_data=None, error=None):
    """
    Log webhook response with appropriate context.

    Args:
        status_code (int): HTTP status code
        response_data (dict): Response data to return
        webhook_data (dict, optional): The webhook payload data
        error (Exception, optional): Exception if one occurred
    """
    # Include timestamp for better tracing
    log = logger.bind(
        status_code=status_code,
        response=response_data,
        request_id=getattr(g, "request_id", "unknown"),
        timestamp=datetime.utcnow().isoformat(),
    )

    if webhook_data:
        # Include webhook data but filter sensitive information
        filtered_data = (
            webhook_data.copy()
            if isinstance(webhook_data, dict)
            else {"data": str(webhook_data)}
        )
        # Remove any potentially sensitive fields
        for key in ["email_html", "auth_token"]:
            if key in filtered_data:
                filtered_data[key] = "[FILTERED]"

        log = log.bind(webhook_data=filtered_data)

    if error:
        log = log.bind(
            error_type=type(error).__name__,
            error_message=str(error),
            traceback=traceback.format_exc(),
        )
        log.error("webhook_error")
    elif status_code >= 400:
        log.error("webhook_failed")
    elif status_code >= 300:
        log.warning("webhook_redirected")
    else:
        log.info("webhook_success")

    return response_data, status_code


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
        log = logger.bind(status_code=status_code)
        if context:
            log = log.bind(**context)
        log.error("non_200_response", response_data=response_data)

    return response_data, status_code


# Track processed webhooks using Redis for persistence across environments
class WebhookTracker:
    def __init__(self, expiration_seconds=1800):  # Default 30 minutes
        self.redis_url = os.environ.get("REDISCLOUD_URL")
        self.redis = None
        self.expiration_seconds = expiration_seconds
        self.prefix = "webhook_tracker:"

        if self.redis_url and self.redis_url.lower() != "null":
            try:
                self.redis = Redis.from_url(self.redis_url)
                # Test the connection
                self.redis.ping()
                logger.info("Successfully connected to Redis")
            except Exception as e:
                logger.warning(f"Failed to connect to Redis: {str(e)}")
                self.redis = None
                self.webhooks = {}  # Fallback to in-memory
        else:
            logger.warning(
                "Redis not configured. WebhookTracker will not persist data."
            )
            self.webhooks = {}  # Fallback to in-memory

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
                webhook_data = json.loads(data)
                webhook_data["task_id"] = task_id  # Add task_id to response
                return webhook_data
            return {}
        else:
            # Fallback to in-memory
            data = self.webhooks.get(task_id, {})
            if data:
                data["task_id"] = task_id  # Add task_id to response
            return data

    def get_all(self):
        """Get all processed webhooks (for debugging)."""
        if self.redis:
            keys = self.redis.keys(f"{self.prefix}*")
            result = {}
            for key in keys:
                task_id = key.decode("utf-8").replace(self.prefix, "")
                data = self.redis.get(key)
                if data:
                    webhook_data = json.loads(data)
                    webhook_data["task_id"] = task_id  # Add task_id to response
                    result[task_id] = webhook_data
            return result
        else:
            # Fallback to in-memory
            result = {}
            for task_id, data in self.webhooks.items():
                data_copy = data.copy()
                data_copy["task_id"] = task_id  # Add task_id to response
                result[task_id] = data_copy
            return result


# Create the webhook tracker instance
_webhook_tracker = WebhookTracker()

# Get API keys from environment
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = None  # This will be initialized when it's needed
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY")
INSTANTLY_API_KEY = os.environ.get("INSTANTLY_API_KEY")
ENV_TYPE = os.environ.get("ENV_TYPE", "development")
BARBARA_USER_ID = "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as"


# --- Redis cache helpers ---
def get_redis_client():
    redis_url = os.environ.get("REDISCLOUD_URL")
    return Redis.from_url(redis_url) if redis_url else None


def get_from_cache(key):
    client = get_redis_client()
    if client:
        cached = client.get(key)
        if cached:
            try:
                return json.loads(cached)
            except Exception as e:
                logger.warning(f"Failed to decode cache for {key}: {e}")
    return None


def set_to_cache(key, value, expiration_seconds=600):
    client = get_redis_client()
    if client:
        try:
            client.setex(key, expiration_seconds, json.dumps(value))
        except Exception as e:
            logger.warning(f"Failed to set cache for {key}: {e}")


# --- End Redis cache helpers ---


def get_close_encoded_key():
    """Get Base64 encoded Close API key."""
    return b64encode(f"{CLOSE_API_KEY}:".encode()).decode()


def send_email(subject, body, **kwargs):
    """Send email notification through Gmail."""
    # Import directly to avoid Flask application context issues in Celery tasks
    from app import send_email as app_send_email

    return app_send_email(subject, body, **kwargs)


def get_instantly_campaign_name(task_text):
    """
    Extract the campaign name from a Close task text.

    This function removes "Instantly" and any trailing non-space characters
    (like ":", "!", "--") and returns the rest of the text as the campaign name.
    It also removes any text enclosed in square brackets [].

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
        # Remove any text in square brackets and then strip
        text = match.group(1)
        text = re.sub(r"\s*\[.*?\]\s*", " ", text).strip()
        return text

    # Handle case where there is no separator (InstantlyTest)
    # For this case, we want to return empty string
    if re.match(r"^Instantly[a-zA-Z0-9]", task_text):
        return ""

    # Fallback - just remove "Instantly" prefix and any text in square brackets
    remaining = task_text[len("Instantly") :].strip()
    remaining = re.sub(r"\s*\[.*?\]\s*", " ", remaining).strip()
    return remaining


def get_instantly_campaigns(
    limit=100, starting_after=None, fetch_all=False, search=None
):
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

    if search:
        params["search"] = search

    cache_key = None
    CACHE_EXPIRATION_SECONDS = 3600  # 1 hour
    if search:
        cache_key = f"instantly:campaign_search:{search.lower().strip()}"
        cached = get_from_cache(cache_key)
        if cached:
            logger.info(f"Returning cached Instantly campaign search for '{search}'")
            return cached

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
            result = {
                "status": "success",
                "campaigns": all_campaigns,
                "count": len(all_campaigns),
            }
            # Cache if search is present
            if search and cache_key:
                set_to_cache(cache_key, result, CACHE_EXPIRATION_SECONDS)
            return result
        else:
            # Fetch single page
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract campaigns from the response
            campaigns = data.get("items", [])
            next_cursor = data.get("next_starting_after")

            result = {
                "status": "success",
                "campaigns": campaigns,
                "count": len(campaigns),
                "pagination": {
                    "limit": limit,
                    "next_starting_after": next_cursor,
                    "has_more": bool(next_cursor),
                },
            }
            # Cache if search is present
            if search and cache_key:
                set_to_cache(cache_key, result, CACHE_EXPIRATION_SECONDS)
            return result
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

    # Retrieve campaigns using the Instantly API's built-in "search" parameter so we
    # only make a single request instead of walking every page.  This keeps the
    # request well under Heroku's 30-second router timeout even when the
    # Instantly account has thousands of campaigns.
    campaigns_response = get_instantly_campaigns(search=campaign_name)

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
    """Handle webhooks from Close when a task is created with 'Instantly:' prefix - now with async processing."""
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
        close_task_id = task_data.get("id")
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
            error_msg = f"Could not extract campaign name from task: {task_text}"
            logger.warning(error_msg)
            send_email(subject="Instantly Campaign Name Error", body=error_msg)
            return jsonify(
                {"status": "success", "message": "No campaign name found in task text"}
            ), 200

        logger.info(
            f"Queueing async processing for Instantly campaign: {campaign_name} for lead: {lead_id}"
        )

        # Create initial webhook tracker entry immediately for monitoring
        initial_webhook_data = {
            "route": "add_lead",
            "lead_id": lead_id,
            "close_task_id": close_task_id,
            "campaign_name": campaign_name,
            "processed": False,  # Will be updated to True when Celery task completes
            "timestamp": datetime.now().isoformat(),
            "processing_type": "async",
            "status": "queued",  # Initial status
        }
        _webhook_tracker.add(close_task_id, initial_webhook_data)

        logger.info(f"Initial webhook tracker entry created for task {close_task_id}")

        # Queue the Celery task for async processing immediately - no validation
        celery_task = process_lead_batch_task.delay(data)

        # Update the webhook tracker with the Celery task ID
        initial_webhook_data["celery_task_id"] = celery_task.id
        initial_webhook_data["status"] = "processing"
        _webhook_tracker.add(close_task_id, initial_webhook_data)

        logger.info(
            "async_task_queued",
            close_task_id=close_task_id,
            lead_id=lead_id,
            campaign_name=campaign_name,
            celery_task_id=celery_task.id,
        )

        # Return immediate success response with Celery task ID
        return jsonify(
            {
                "status": "success",
                "message": f"Lead processing queued for Instantly campaign: {campaign_name}",
                "lead_id": lead_id,
                "celery_task_id": celery_task.id,  # Celery background task ID
                "close_task_id": close_task_id,  # Close CRM task ID
                "campaign_name": campaign_name,
                "processing_type": "async",
            }
        ), 202  # 202 Accepted - request accepted for processing

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
                "status": "success",
                "message": "An error occurred processing the Close task webhook",
                "error": str(e),
            }
        ), 200


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

    if len(parts) == 0:
        # Empty string after stripping
        return "", ""
    elif len(parts) == 1:
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
        # Apply rate limiting before making the API request
        rate_limiter = get_rate_limiter()
        if rate_limiter:
            rate_limiter_key = "instantly_api"
            start_time = time.time()

            # Wait for rate limiter to allow the request
            while not rate_limiter.acquire_token(rate_limiter_key):
                time.sleep(0.1)  # Wait 100ms before retrying

                # Safety check to prevent infinite waiting
                if time.time() - start_time > 30:
                    logger.warning("Rate limiter timeout after 30 seconds")
                    break

            logger.debug(
                f"Rate limiter allowed request after {time.time() - start_time:.2f}s wait"
            )

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
    """
    Get processed webhooks for testing and monitoring purposes.

    Supports filtering by multiple parameters:
    - task_id: Filter by specific task ID (legacy, same as close_task_id)
    - close_task_id: Filter by Close CRM task ID
    - route: Filter by webhook route (e.g., 'email_sent', 'reply_received', 'add_lead')
    - email_id: Filter by email activity ID
    - lead_id: Filter by lead ID
    - lead_email: Filter by lead email

    Returns all webhooks that match ALL provided filter parameters.
    """
    # Get filter parameters - support both task_id (legacy) and close_task_id
    task_id = request.args.get("task_id")
    close_task_id = request.args.get("close_task_id")
    # Use close_task_id if provided, otherwise fall back to task_id for backward compatibility
    lookup_task_id = close_task_id or task_id

    route = request.args.get("route")
    email_id = request.args.get("email_id")
    lead_id = request.args.get("lead_id")
    lead_email = request.args.get("lead_email")

    # Dictionary of filter parameters that were provided
    filters = {}
    if lookup_task_id:
        filters["close_task_id"] = lookup_task_id
    if route:
        filters["route"] = route
    if email_id:
        filters["email_id"] = email_id
    if lead_id:
        filters["lead_id"] = lead_id
    if lead_email:
        filters["lead_email"] = lead_email

    # If close_task_id/task_id is provided, check that specific task first for efficiency
    if lookup_task_id:
        webhook_data = _webhook_tracker.get(lookup_task_id)
        if webhook_data:
            # Remove close_task_id from filters since we already matched on it
            if "close_task_id" in filters:
                del filters["close_task_id"]

            # Check if the webhook matches all other filters
            matches_all_filters = True
            for key, value in filters.items():
                # Handle special case where close_task_id could be None for some webhooks
                if (
                    key == "close_task_id"
                    and webhook_data.get(key) is None
                    and value.lower() == "none"
                ):
                    continue

                if webhook_data.get(key) != value:
                    matches_all_filters = False
                    break

            if matches_all_filters:
                return jsonify({"status": "success", "data": webhook_data}), 200
            else:
                filter_str = ", ".join([f"{k}: {v}" for k, v in filters.items()])
                return jsonify(
                    {
                        "status": "not_found",
                        "message": f"Webhook for close_task_id: {lookup_task_id} doesn't match filters: {filter_str}",
                    }
                ), 404
        else:
            return jsonify(
                {
                    "status": "not_found",
                    "message": f"No webhook data found for close_task_id: {lookup_task_id}",
                }
            ), 404

    # If no task_id or multiple filters, get all webhooks and filter
    all_webhooks = _webhook_tracker.get_all()

    # If no filters, return all webhooks
    if not filters:
        return jsonify({"status": "success", "data": all_webhooks}), 200

    # Filter webhooks based on provided parameters
    filtered_webhooks = {}
    for webhook_key, webhook in all_webhooks.items():
        matches_all_filters = True
        for key, value in filters.items():
            # Special handling for None values that might be stored in the webhook data
            if key in webhook and webhook[key] is None and value.lower() == "none":
                continue

            if webhook.get(key) != value:
                matches_all_filters = False
                break

        if matches_all_filters:
            filtered_webhooks[webhook_key] = webhook

    # Return filtered results
    if filtered_webhooks:
        return jsonify({"status": "success", "data": filtered_webhooks}), 200
    else:
        filter_str = ", ".join([f"{k}: {v}" for k, v in filters.items()])
        return jsonify(
            {
                "status": "not_found",
                "message": f"No webhooks found matching filters: {filter_str}",
            }
        ), 404


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
    g_run_id = getattr(g, "request_id", str(uuid.uuid4()))

    input = WebhookEmailSentPaylod(
        json_payload=request.get_json(),
    )

    _ = temporal.run(temporal.client.start_workflow(
        WebhookEmailSentWorkflow.run,
        input,
        id=g_run_id,
        task_queue=TASK_QUEUE_NAME
    ))

    return jsonify({"status": "success", "message": "Webhook received"}), 200


@instantly_bp.route("/reply_received", methods=["POST"])
def handle_instantly_reply_received():
    """Handle webhooks from Instantly when a reply is received."""
    try:
        # Parse the webhook payload
        data = request.json
        logger.info(
            "reply_received_webhook_received",
            event_type=data.get("event_type"),
            campaign_name=data.get("campaign_name"),
            lead_email=data.get("lead_email"),
        )

        # Verify this is a reply received event
        if data.get("event_type") != "reply_received":
            logger.warning(
                "non_reply_received_event", event_type=data.get("event_type")
            )
            response_data = {
                "status": "success",
                "message": "Not a reply received event",
            }
            return log_webhook_response(200, response_data, data)

        # Extract relevant data from the webhook
        lead_email = data.get("lead_email")
        campaign_name = data.get("campaign_name")
        reply_subject = data.get("reply_subject")
        reply_text = data.get("reply_text")
        reply_html = data.get("reply_html")

        if not all(
            [lead_email, campaign_name, reply_subject, reply_text or reply_html]
        ):
            error_msg = "Missing required fields in webhook payload"
            logger.error(
                "webhook_missing_fields",
                lead_email=lead_email,
                campaign_name=campaign_name,
                reply_subject=bool(reply_subject),
                reply_text=bool(reply_text),
                reply_html=bool(reply_html),
            )
            response_data = {"status": "error", "message": error_msg}
            return log_webhook_response(400, response_data, data)

        # Search for leads with this email
        query = create_email_search_query(lead_email)
        leads = search_close_leads(query)
        if not leads:
            error_msg = f"No lead found with email: {lead_email}"
            logger.error("lead_not_found", lead_email=lead_email)
            response_data = {"status": "error", "message": error_msg}
            return log_webhook_response(404, response_data, data)

        if len(leads) > 1:
            error_msg = f"Multiple leads found with email: {lead_email}"
            logger.error(
                "multiple_leads_found", lead_email=lead_email, lead_count=len(leads)
            )
            response_data = {"status": "error", "message": error_msg}
            return log_webhook_response(400, response_data, data)

        lead = leads[0]
        lead_id = lead["id"]
        logger.info("lead_found", lead_id=lead_id, lead_email=lead_email)

        # Get lead details
        lead_details = get_lead_by_id(lead_id)
        if not lead_details:
            error_msg = f"Could not retrieve lead details for lead ID: {lead_id}"
            logger.error("lead_details_not_found", lead_id=lead_id)
            response_data = {"status": "error", "message": error_msg}
            return log_webhook_response(404, response_data, data)

        # Get the contact with the matching email
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
            logger.error("contact_not_found", lead_id=lead_id, lead_email=lead_email)
            response_data = {"status": "error", "message": error_msg}
            return log_webhook_response(404, response_data, data)

        # Create email activity in Close
        email_data = {
            "contact_id": contact["id"],
            "user_id": BARBARA_USER_ID,
            "lead_id": lead_id,
            "direction": "incoming",
            "created_by": None,  # For incoming emails, no created_by
            "date_created": data.get("timestamp")
            .replace("Z", "+00:00")
            .replace("T", "T"),
            "subject": reply_subject,
            "sender": lead_email,
            "to": [data.get("email_account")],
            "bcc": [],
            "cc": [],
            "status": "inbox",
            "body_text": reply_text or "",
            "body_html": reply_html or "",
            "attachments": [],
            "template_id": None,
        }

        email_url = "https://api.close.com/api/v1/activity/email/"
        email_response = make_close_request("post", email_url, json=email_data)

        # Pause any active sequence subscriptions for this contact
        subscriptions = get_sequence_subscriptions(lead_id=lead_id)

        # Track paused subscriptions
        paused_subscriptions = []

        # Pause each active subscription
        for subscription in subscriptions:
            if subscription.get("status") == "active":
                subscription_id = subscription.get("id")
                result = pause_sequence_subscription(
                    subscription_id, status_reason="replied"
                )
                if result:
                    paused_subscriptions.append(
                        {
                            "subscription_id": subscription_id,
                            "sequence_id": subscription.get("sequence_id"),
                            "sequence_name": subscription.get(
                                "sequence_name", "Unknown"
                            ),
                        }
                    )
                    logger.info(
                        "sequence_paused",
                        subscription_id=subscription_id,
                        lead_id=lead_id,
                        lead_email=lead_email,
                    )

        # Get lead name for notification
        lead_name = lead_details.get("name", "Unknown")

        # Get environment information
        env_type = os.environ.get("ENV_TYPE", "development")

        # Determine notification recipients based on consultant
        custom_recipients, consultant_error = determine_notification_recipients(
            lead_details, env_type
        )

        if consultant_error:
            # Return error response for unknown/missing consultant
            error_msg = f"Error processing reply received webhook: {consultant_error}"
            logger.error(
                "consultant_determination_failed",
                lead_id=lead_id,
                lead_email=lead_email,
                error=consultant_error,
            )

            response_data = {
                "status": "error",
                "message": consultant_error,
                "lead_id": lead_id,
                "consultant": lead_details.get(
                    "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi"
                ),
            }
            return log_webhook_response(400, response_data, data)

        # Format the notification email content
        notification_html = f"""
        <h2>Instantly Email Reply Received</h2>
        <p>A reply has been received from an Instantly email campaign.</p>
        
        <h3>Details:</h3>
        <ul>
            <li><strong>Lead:</strong> {lead_name}</li>
            <li><strong>Lead Email:</strong> {lead_email}</li>
            <li><strong>Campaign:</strong> {campaign_name}</li>
            <li><strong>Subject:</strong> {reply_subject}</li>
            <li><strong>Environment:</strong> {env_type}</li>
            <li><strong>Time:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</li>
        </ul>
        
        <h3>Reply Content:</h3>
        <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; background-color: #f9f9f9;">
            {reply_html or reply_text or "No content available"}
        </div>
        """

        # Add sequence info to notification if any were paused
        if paused_subscriptions:
            notification_html += """
            <h3>Sequences Paused:</h3>
            <ul>
            """
            for sub in paused_subscriptions:
                notification_html += f"<li>{sub.get('sequence_name', 'Unknown Sequence')} (ID: {sub.get('sequence_id')})</li>"
            notification_html += "</ul>"

        notification_html += f"""
        <p><a href="https://app.close.com/lead/{lead_id}/" style="padding: 10px 15px; background-color: #4CAF50; color: white; text-decoration: none; border-radius: 4px; display: inline-block; margin-top: 10px;">View Lead in Close</a></p>
        """

        # Removed recipient determination code since it's now handled in app.py

        # Prepare text content for the email notification
        text_content = f"""Instantly Reply Received

Lead: {lead_name}
Email: {lead_email}
Campaign: {campaign_name}
Subject: {reply_subject}
Environment: {env_type}
Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"""

        if paused_subscriptions:
            text_content += "\n\nSequences Paused:"
            for sub in paused_subscriptions:
                text_content += f"\n- {sub.get('sequence_name', 'Unknown Sequence')} (ID: {sub.get('sequence_id')})"

        # Send email notification using Gmail API
        try:
            # Send notification email using our wrapper function
            # Pass custom recipients if determined, otherwise use default
            email_kwargs = {
                "subject": f"Instantly Reply: {reply_subject} from {lead_name}",
                "body": notification_html,
                "text_content": text_content,
            }

            # Add custom recipients if determined
            if custom_recipients:
                email_kwargs["recipients"] = custom_recipients
                logger.info(
                    "using_custom_recipients",
                    lead_id=lead_id,
                    recipients=custom_recipients,
                    consultant=lead_details.get(
                        "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi"
                    ),
                )

            notification_result = send_email(**email_kwargs)
            # Initialize notification status
            notification_status = notification_result.get("status", "unknown")
            logger.info(
                "notification_email_sent",
                email_status=notification_status,
                message_id=notification_result.get("message_id"),
            )
        except Exception as email_error:
            logger.error(
                "gmail_notification_failed",
                error=str(email_error),
            )
            notification_status = "error"

        logger.info(f"Successfully processed reply received webhook for lead {lead_id}")

        # Track this webhook - use lead_email + timestamp as key since no close_task_id
        webhook_tracking_key = (
            f"reply_{lead_email}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        webhook_data = {
            "route": "reply_received",
            "lead_id": lead_id,
            "lead_email": lead_email,
            "close_task_id": None,  # Reply webhooks don't have associated Close tasks
            "email_id": email_response.json().get("id"),
            "paused_subscriptions": paused_subscriptions,
            "notification_status": notification_status,
        }
        _webhook_tracker.add(webhook_tracking_key, webhook_data)
        logger.info(
            f"Recorded reply received webhook for lead {lead_id} with key {webhook_tracking_key}"
        )

        response_data = {
            "status": "success",
            "message": "Reply received webhook processed successfully",
            "data": {
                "lead_id": lead_id,
                "email_id": email_response.json().get("id"),
                "close_task_id": None,  # Reply webhooks don't have associated Close tasks
                "paused_subscriptions": paused_subscriptions,
                "notification_status": notification_status,
                "consultant": lead_details.get(
                    "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi"
                ),
                "custom_recipients_used": bool(custom_recipients),
            },
        }

        return log_webhook_response(200, response_data, webhook_data)

    except Exception as e:
        error_msg = f"Error processing reply received webhook: {str(e)}"
        # Capture the traceback
        tb = traceback.format_exc()

        # Get request ID which serves as run ID
        run_id = getattr(g, "request_id", str(uuid.uuid4()))

        # Extract calling function name
        calling_function = "handle_instantly_reply_received"

        error_message = f"""
        <h2>Instantly Reply Received Webhook Error</h2>
        <p><strong>Error:</strong> {str(e)}</p>
        <p><strong>Route:</strong> {request.path}</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Origin:</strong> {calling_function}</p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Webhook Data:</h3>
        <pre>{json.dumps({k: v for k, v in request.get_json().items() if k not in ["auth_token", "email_html", "password"]}, indent=2, default=str)}</pre>
        
        <h3>Traceback:</h3>
        <pre>{tb}</pre>
        """

        logger.error(
            "reply_received_webhook_error",
            error=str(e),
            traceback=traceback.format_exc(),
            run_id=run_id,
            route=request.path,
            origin=calling_function,
        )

        # Removed recipient determination code since it's now handled in app.py

        # Send email notification
        send_email(subject="Instantly Reply Received Webhook Error", body=error_message)

        response_data = {"status": "error", "message": error_msg}
        return log_webhook_response(500, response_data, None, error=str(e))


@celery.task(name="blueprints.instantly.process_lead_batch_task")
def process_lead_batch_task(payload_data):
    """
    Celery task for processing lead batch asynchronously.

    This task integrates all the components:
    - Redis rate limiter (Step 2)
    - Request queue system (Step 3)
    - Circuit breaker pattern (Step 4)

    Args:
        payload_data (dict): The Close webhook payload data

    Returns:
        dict: Processing result with status and details
    """
    import time

    # Start timing the entire task
    task_start_time = time.time()
    step_start_time = task_start_time

    def log_timing(step_name, **extra_data):
        """Helper function to log timing for each step."""
        nonlocal step_start_time
        current_time = time.time()
        step_duration = current_time - step_start_time
        total_duration = current_time - task_start_time

        logger.info(
            f"timing_{step_name}",
            step_duration_seconds=round(step_duration, 3),
            total_duration_seconds=round(total_duration, 3),
            celery_task_id=process_lead_batch_task.request.id,
            **extra_data,
        )
        step_start_time = current_time
        return step_duration

    try:
        # Set up structured logging for this task
        logger.info(
            "process_lead_batch_task_started",
            celery_task_id=process_lead_batch_task.request.id,
            payload_keys=list(payload_data.keys()) if payload_data else [],
        )
        log_timing("task_initialization")

        # Extract event data from payload
        event = payload_data.get("event", {})
        task_data = event.get("data", {})
        close_task_id = task_data.get("id")
        task_text = task_data.get("text", "")
        lead_id = task_data.get("lead_id")
        log_timing("data_extraction", close_task_id=close_task_id, lead_id=lead_id)

        # Extract campaign name
        campaign_name = get_instantly_campaign_name(task_text)
        log_timing("campaign_name_extraction", campaign_name=campaign_name)
        if not campaign_name:
            error_msg = f"Could not extract campaign name from task: {task_text}"
            logger.warning("campaign_name_extraction_failed", task_text=task_text)

            # Update webhook tracker with error
            existing_data = _webhook_tracker.get(close_task_id) or {}
            error_webhook_data = {
                **existing_data,
                "processed": True,
                "status": "error",
                "error": error_msg,
                "completion_timestamp": datetime.now().isoformat(),
            }
            _webhook_tracker.add(close_task_id, error_webhook_data)

            # Send error email notification
            send_email(
                subject="Instantly Campaign Name Extraction Error",
                body=f"Error in async processing: {error_msg}\n\nPayload: {payload_data}\nCelery Task ID: {process_lead_batch_task.request.id}",
            )

            return {
                "status": "error",
                "message": error_msg,
                "celery_task_id": process_lead_batch_task.request.id,
            }

        logger.info(
            "processing_lead_batch",
            close_task_id=close_task_id,
            lead_id=lead_id,
            campaign_name=campaign_name,
            celery_task_id=process_lead_batch_task.request.id,
        )

        # Check if campaign exists
        campaign_check = campaign_exists(campaign_name)
        log_timing(
            "campaign_exists_check",
            campaign_name=campaign_name,
            exists=campaign_check.get("exists"),
        )
        if not campaign_check.get("exists"):
            error_msg = f"Campaign '{campaign_name}' does not exist in Instantly"
            logger.warning("campaign_not_found", campaign_name=campaign_name)

            # Update webhook tracker with error
            existing_data = _webhook_tracker.get(close_task_id) or {}
            error_webhook_data = {
                **existing_data,
                "processed": True,
                "status": "error",
                "error": error_msg,
                "completion_timestamp": datetime.now().isoformat(),
            }
            _webhook_tracker.add(close_task_id, error_webhook_data)

            # Send error email notification
            close_lead_url = f"https://app.close.com/lead/{lead_id}/"
            email_subject = f"Instantly Campaign Not Found: {campaign_name}"
            email_body = f"""
Error: Campaign not found in Instantly (Async Processing)

Lead ID: {lead_id}
Lead URL: {close_lead_url}
Task Text: {task_text}
Campaign Name (extracted): {campaign_name}
Celery Task ID: {process_lead_batch_task.request.id}

The campaign name could not be found in Instantly. Please verify the campaign exists or check the task text format.

Error details: {error_msg}
            """
            send_email(subject=email_subject, body=email_body)
            return {"status": "error", "message": error_msg}

        campaign_id = campaign_check.get("campaign_id")
        logger.info(
            "campaign_found", campaign_name=campaign_name, campaign_id=campaign_id
        )

        # Get lead details from Close
        lead_details = get_lead_by_id(lead_id)
        log_timing("close_api_get_lead", lead_id=lead_id, success=bool(lead_details))
        if not lead_details:
            error_msg = f"Could not retrieve lead details for lead ID: {lead_id}"
            logger.warning("lead_details_not_found", lead_id=lead_id)

            # Update webhook tracker with error
            existing_data = _webhook_tracker.get(close_task_id) or {}
            error_webhook_data = {
                **existing_data,
                "processed": True,
                "status": "error",
                "error": error_msg,
                "completion_timestamp": datetime.now().isoformat(),
            }
            _webhook_tracker.add(close_task_id, error_webhook_data)

            send_email(subject="Close Lead Details Error (Async)", body=error_msg)
            return {"status": "error", "message": error_msg}

        # Extract lead information
        full_name = lead_details.get("contacts", [{}])[0].get("name", "")
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
            logger.warning("lead_email_not_found", lead_id=lead_id)
            log_timing("lead_data_processing_error", error="no_email_found")

            # Update webhook tracker with error
            existing_data = _webhook_tracker.get(close_task_id) or {}
            error_webhook_data = {
                **existing_data,
                "processed": True,
                "status": "error",
                "error": error_msg,
                "completion_timestamp": datetime.now().isoformat(),
            }
            _webhook_tracker.add(close_task_id, error_webhook_data)

            send_email(subject="Close Lead Email Error (Async)", body=error_msg)
            return {"status": "error", "message": error_msg}

        # Get custom fields
        company_name = lead_details.get(
            "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o", ""
        )
        date_location = lead_details.get(
            "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9", ""
        )
        log_timing(
            "lead_data_processing",
            email=email,
            has_company=bool(company_name),
            has_date_location=bool(date_location),
        )

        logger.info(
            "lead_data_extracted",
            lead_id=lead_id,
            email=email,
            first_name=first_name,
            last_name=last_name,
            company_name=company_name,
        )

        # Use the existing add_to_instantly_campaign function which already includes
        # rate limiting via get_rate_limiter()
        instantly_result = add_to_instantly_campaign(
            campaign_id=campaign_id,
            email=email,
            first_name=first_name,
            last_name=last_name,
            company_name=company_name,
            date_location=date_location,
        )
        log_timing("instantly_api_call", result_status=instantly_result.get("status"))

        if instantly_result.get("status") == "error":
            error_msg = (
                f"Failed to add lead to Instantly: {instantly_result.get('message')}"
            )
            logger.error("instantly_api_error", error=error_msg)

            # Update webhook tracker with error
            existing_data = _webhook_tracker.get(close_task_id) or {}
            error_webhook_data = {
                **existing_data,
                "processed": True,
                "status": "error",
                "error": error_msg,
                "completion_timestamp": datetime.now().isoformat(),
                "instantly_result": instantly_result,
            }
            _webhook_tracker.add(close_task_id, error_webhook_data)

            send_email(subject="Instantly API Error (Async)", body=error_msg)
            return {"status": "error", "message": error_msg}

        # Update the existing webhook tracker entry with completion data
        # Get existing entry first to preserve initial data
        existing_data = _webhook_tracker.get(close_task_id) or {}

        # Update with completion data
        webhook_data = {
            **existing_data,  # Preserve existing data
            "campaign_id": campaign_id,
            "processed": True,
            "completion_timestamp": datetime.now().isoformat(),
            "instantly_result": instantly_result,
            "status": "completed",
        }

        # Update tracker in Redis (with expiration) - use close_task_id as the key
        _webhook_tracker.add(close_task_id, webhook_data)
        log_timing("webhook_tracker_update")

        # Log final completion timing
        log_timing("task_completion")

        logger.info(
            "process_lead_batch_task_completed",
            close_task_id=close_task_id,
            lead_id=lead_id,
            campaign_name=campaign_name,
            celery_task_id=process_lead_batch_task.request.id,
            instantly_result_status=instantly_result.get("status"),
            total_duration_seconds=round(time.time() - task_start_time, 3),
        )

        return {
            "status": "success",
            "message": f"Lead added to Instantly campaign: {campaign_name}",
            "lead_id": lead_id,
            "close_task_id": close_task_id,
            "campaign_name": campaign_name,
            "campaign_id": campaign_id,
            "instantly_result": instantly_result,
            "celery_task_id": process_lead_batch_task.request.id,
        }

    except Exception as e:
        # Log timing for error case
        try:
            error_duration = time.time() - task_start_time
            logger.error(
                "timing_task_error",
                total_duration_seconds=round(error_duration, 3),
                celery_task_id=process_lead_batch_task.request.id,
                error=str(e),
            )
        except Exception:
            pass  # Don't let timing logging cause additional errors

        # Capture the traceback
        tb = traceback.format_exc()
        error_message = f"Error in process_lead_batch_task: {str(e)}\nTraceback: {tb}"

        # Extract close_task_id for error tracking
        try:
            event = payload_data.get("event", {}) if payload_data else {}
            task_data = event.get("data", {})
            close_task_id = task_data.get("id")

            if close_task_id:
                # Update webhook tracker with error
                existing_data = _webhook_tracker.get(close_task_id) or {}
                error_webhook_data = {
                    **existing_data,
                    "processed": True,
                    "status": "error",
                    "error": str(e),
                    "completion_timestamp": datetime.now().isoformat(),
                }
                _webhook_tracker.add(close_task_id, error_webhook_data)
        except Exception as tracker_error:
            logger.warning(
                f"Failed to update webhook tracker for error case: {tracker_error}"
            )

        logger.error(
            "process_lead_batch_task_error",
            error=str(e),
            traceback=tb,
            celery_task_id=process_lead_batch_task.request.id,
            payload=payload_data,
        )

        send_email(subject="Instantly Async Processing Error", body=error_message)

        return {
            "status": "error",
            "message": str(e),
            "celery_task_id": process_lead_batch_task.request.id,
        }
