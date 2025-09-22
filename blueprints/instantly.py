"""
Blueprint for handling Instantly API integrations.
"""

import os
from datetime import datetime
import traceback
from base64 import b64encode
import json
from redis import Redis
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

# Import rate limiter and utilities
from utils.instantly import get_instantly_campaigns
from utils.instantly import campaign_exists
from utils.instantly import logger

from temporal.workflows.instantly.webhook_add_lead_workflow import WebhookAddLeadWorkflow, WebhookAddLeadPayload
from temporal.workflows.instantly.webhook_email_sent_workflow import WebhookEmailSentWorkflow, WebhookEmailSentPayload
from temporal.shared import TASK_QUEUE_NAME

# Set up blueprint
instantly_bp = Blueprint("instantly", __name__)


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
            recipients_list = [
                "april.lowrie@whiteboardgeeks.com",
                "noura.mahmoud@whiteboardgeeks.com",
                "lauren.poche@whiteboardgeeks.com",
            ]
            recipients = ",".join(recipients_list)
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
ENV_TYPE = os.environ.get("ENV_TYPE", "development")
BARBARA_USER_ID = "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as"


# --- End Redis cache helpers ---


def get_close_encoded_key():
    """Get Base64 encoded Close API key."""
    return b64encode(f"{CLOSE_API_KEY}:".encode()).decode()


def send_email(subject, body, **kwargs):
    """Send email notification through Gmail."""
    # Import directly to avoid Flask application context issues in Celery tasks
    from utils.email import send_email as app_send_email

    return app_send_email(subject, body, **kwargs)


@instantly_bp.route("/add_lead", methods=["POST"])
def add_lead_to_instantly():
    json_payload = request.get_json(silent=True)
    if json_payload is None:
        error_msg = "Invalid or missing JSON payload for Instantly add lead webhook"
        logger.error("invalid_json_payload", route="/instantly/add_lead")
        return jsonify({"status": "error", "message": error_msg}), 400

    g_run_id = getattr(g, "request_id", str(uuid.uuid4()))

    try:
        workflow_input = WebhookAddLeadPayload(json_payload=json_payload)

        temporal.ensure_started()
        start_workflow_coro = temporal.client.start_workflow(
            WebhookAddLeadWorkflow.run,
            workflow_input,
            id=g_run_id,
            task_queue=TASK_QUEUE_NAME,
        )

        temporal.run(start_workflow_coro)

        logger.info(
            "instantly_add_lead_workflow_started",
            run_id=g_run_id,
            request_has_payload=bool(json_payload),
        )

        return jsonify(
            {"status": "success", "message": 
             "Webhook received", 
             "processing_type": "async"}), 202
    except Exception as exc:
        tb = traceback.format_exc()
        safe_payload = json_payload
        if isinstance(json_payload, dict):
            safe_payload = json_payload.copy()
            for field in ("auth_token", "email_html", "password"):
                if field in safe_payload:
                    safe_payload[field] = "[FILTERED]"

        error_message = (
            f"Error starting Instantly add lead workflow: {exc}\n\n"
            f"Run ID: {g_run_id}\n"
            f"Route: {request.path}\n"
            f"Payload: {json.dumps(safe_payload, default=str)}\n\n"
            f"Traceback:\n{tb}"
        )

        logger.error(
            "instantly_add_lead_workflow_start_error",
            error=str(exc),
            run_id=g_run_id,
            route=request.path,
        )

        send_email(
            subject="Instantly Add Lead Workflow Error",
            body=error_message,
        )

        response = {
            "status": "success",
            "message": "An error occurred starting the Instantly workflow",
            "error": str(exc),
        }
        return jsonify(response), 202


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

    input = WebhookEmailSentPayload(
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
