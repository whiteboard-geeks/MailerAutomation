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

from utils.instantly import get_instantly_campaigns
from utils.instantly import campaign_exists
from utils.instantly import logger

from temporal.workflows.instantly.webhook_add_lead_workflow import WebhookAddLeadWorkflow, WebhookAddLeadPayload
from temporal.workflows.instantly.webhook_email_sent_workflow import WebhookEmailSentWorkflow, WebhookEmailSentPayload
from temporal.workflows.instantly.webhook_reply_received_workflow import (
    WebhookReplyReceivedPayload,
    WebhookReplyReceivedWorkflow,
)
from temporal.shared import TASK_QUEUE_NAME

# Set up blueprint
instantly_bp = Blueprint("instantly", __name__)


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
    """Handle Instantly reply webhooks using the Temporal workflow."""
    return handle_instantly_reply_received_temporal()


def handle_instantly_reply_received_temporal():
    """Enqueue Temporal workflow for reply received processing and return 202."""
    json_payload = request.get_json(silent=True)
    if json_payload is None:
        response_data = {
            "status": "error",
            "message": "Invalid or missing JSON payload",
        }
        return log_webhook_response(400, response_data, None)

    g_run_id = getattr(g, "request_id", str(uuid.uuid4()))

    logger.info(
        "reply_received_temporal_enqueue",
        run_id=g_run_id,
        event_type=json_payload.get("event_type"),
        campaign_name=json_payload.get("campaign_name"),
        lead_email=json_payload.get("lead_email"),
    )

    try:
        workflow_input = WebhookReplyReceivedPayload(json_payload=json_payload)
    except Exception as exc:
        response_data = {
            "status": "error",
            "message": f"Invalid payload: {exc}",
        }
        return log_webhook_response(400, response_data, json_payload, error=str(exc))

    try:
        temporal.ensure_started()
        start_coro = temporal.client.start_workflow(
            WebhookReplyReceivedWorkflow.run,
            workflow_input,
            id=g_run_id,
            task_queue=TASK_QUEUE_NAME,
        )
        temporal.run(start_coro)
    except Exception as exc:
        logger.exception(
            "reply_received_temporal_enqueue_failed",
            run_id=g_run_id,
            error=str(exc),
        )
        response_data = {
            "status": "error",
            "message": "Failed to enqueue Temporal workflow",
        }
        return log_webhook_response(500, response_data, json_payload, error=str(exc))

    response_data = {
        "status": "accepted",
        "message": "Reply received webhook accepted for asynchronous processing",
        "workflow_id": g_run_id,
    }
    minimal_webhook_data = {
        "event_type": json_payload.get("event_type"),
        "campaign_name": json_payload.get("campaign_name"),
        "lead_email": json_payload.get("lead_email"),
        "reply_subject": json_payload.get("reply_subject"),
        "timestamp": json_payload.get("timestamp"),
    }
    return log_webhook_response(202, response_data, minimal_webhook_data)
