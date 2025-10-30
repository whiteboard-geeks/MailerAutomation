"""
Blueprint for EasyPost integration.
This module handles EasyPost webhook tracking and delivery status updates.
"""

import os
import json
from datetime import datetime, date
import traceback
from flask import Blueprint, request, jsonify, g
from redis import Redis
import structlog
from close_utils import (
    load_query,
    search_close_leads,
    get_lead_by_id,
    make_close_request,
)
from celery_worker import celery
import uuid
from temporal.service import temporal
from temporal.shared import TASK_QUEUE_NAME
from temporal.workflows.easypost.webhook_create_tracker_workflow import WebhookCreateTrackerPayload, WebhookCreateTrackerWorkflow
from utils.easypost import get_easypost_client


# Initialize Blueprint
easypost_bp = Blueprint("easypost", __name__)

# Initialize logger
logger = structlog.get_logger()

# API keys
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = None  # This will be initialized when needed

ENV_TYPE = os.environ.get("ENV_TYPE", "development")




# Initialize EasyPost Client (default with production API key)
easypost_client = get_easypost_client()


# Custom JSON encoder for date objects
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


# Webhook tracker for EasyPost events
class WebhookTracker:
    def __init__(self, expiration_seconds=1800):  # Default 30 minutes
        self.redis_url = os.environ.get("REDISCLOUD_URL")
        self.redis = Redis.from_url(self.redis_url) if self.redis_url else None
        self.expiration_seconds = expiration_seconds
        self.prefix = "easypost_webhook_tracker:"

        if not self.redis:
            logger.warning(
                "Redis not configured. WebhookTracker will not persist data."
            )
            self.webhooks = {}  # Fallback to in-memory if Redis not available

    def add(self, tracker_id, data):
        """Add a processed webhook to the tracker."""
        # Add timestamp if not provided
        if "timestamp" not in data:
            data["timestamp"] = datetime.now().isoformat()

        if self.redis:
            # Store in Redis with expiration
            key = f"{self.prefix}{tracker_id}"
            self.redis.setex(
                key, self.expiration_seconds, json.dumps(data, cls=DateEncoder)
            )
            logger.info(f"Stored webhook data in Redis for tracker {tracker_id}")
        else:
            # Fallback to in-memory storage
            self.webhooks[tracker_id] = data
            logger.info(f"Stored webhook data in memory for tracker {tracker_id}")

    def get(self, tracker_id):
        """Get information about a processed webhook."""
        if self.redis:
            key = f"{self.prefix}{tracker_id}"
            data = self.redis.get(key)
            if data:
                return json.loads(data)
            return {}
        else:
            # Fallback to in-memory
            return self.webhooks.get(tracker_id, {})

    def get_all(self):
        """Get all processed webhooks (for debugging)."""
        if self.redis:
            keys = self.redis.keys(f"{self.prefix}*")
            result = {}
            for key in keys:
                tracker_id = key.decode("utf-8").replace(self.prefix, "")
                data = self.redis.get(key)
                if data:
                    result[tracker_id] = json.loads(data)
            return result
        else:
            # Fallback to in-memory
            return {k: v for k, v in self.webhooks.items()}


# Create the webhook tracker instance
_webhook_tracker = WebhookTracker()


def get_close_encoded_key():
    """Get Base64 encoded Close API key."""
    global CLOSE_ENCODED_KEY
    if not CLOSE_ENCODED_KEY:
        import base64

        CLOSE_ENCODED_KEY = base64.b64encode(
            f"{CLOSE_API_KEY}:".encode("utf-8")
        ).decode("utf-8")
    return CLOSE_ENCODED_KEY


def send_email(subject, body, **kwargs):
    """Send email notification."""
    from utils.email import send_email as app_send_email

    return app_send_email(subject, body, **kwargs)


class CreateTrackerRequestError(Exception):
    """Raised when the create tracker request payload is invalid."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


@easypost_bp.route("/create_tracker", methods=["POST"])
def create_easypost_tracker():
    json_payload = request.get_json(silent=True)
    if json_payload is None:
        response_data = {
            "status": "error",
            "message": "Invalid request format",
        }
        return jsonify(response_data), 400

    g_run_id = getattr(g, "request_id", str(uuid.uuid4()))
    logger.info(
        "create_tracker_temporal_enqueue",
        run_id=g_run_id,
    )

    try:
        workflow_input = WebhookCreateTrackerPayload(json_payload=json_payload)
    except Exception as exc:
        response_data = {
            "status": "error",
            "message": f"Invalid payload: {exc}",
        }
        return jsonify(response_data), 400
    
    try:
        temporal.ensure_started()
        start_coro = temporal.client.start_workflow(
            WebhookCreateTrackerWorkflow.run,
            workflow_input,
            id=g_run_id,
            task_queue=TASK_QUEUE_NAME,
        )
        temporal.run(start_coro)
    except Exception as exc:
        logger.exception(
            "create_tracker_temporal_enqueue_failed",
            run_id=g_run_id,
            error=str(exc),
        )
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Error enqueuing Temporal tracker workflow",
                }
            ),
            500,
        )

    response_data = {
        "status": "accepted",
        "message": "Tracker creation workflow queued for background processing",
        "workflow_id": g_run_id,
    }
    return jsonify(response_data), 202


def update_easypost_tracker_id_for_lead(lead_id, update_information):
    """Update lead with EasyPost tracker ID."""

    def verify_delivery_information_updated(response_data, lead_update_data):
        for key, value in lead_update_data.items():
            if key not in response_data or response_data[key] != value:
                return False
        return True

    custom_field_ids = {
        "easypost_tracker_id": {
            "type": "text",
            "value": "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh",
        }
    }

    lead_update_data = {
        custom_field_ids["easypost_tracker_id"]["value"]: update_information[
            "easypost_tracker_id"
        ],
    }

    response = make_close_request(
        "put",
        f"https://api.close.com/api/v1/lead/{lead_id}",
        json=lead_update_data,
    )

    response_data = response.json()
    data_updated = verify_delivery_information_updated(response_data, lead_update_data)

    if not data_updated:
        error_message = f"EasyPost tracker ID update failed for lead {lead_id}."
        logger.error(error_message)
        send_email(subject="EasyPost tracker ID update failed", body=error_message)
        raise Exception("Close accepted the lead, but the fields did not update.")

    logger.info(f"EasyPost tracker ID updated for lead {lead_id}")
    return response_data


def parse_delivery_information(tracking_data):
    """Parse delivery information from tracking data."""
    delivery_information = {}
    delivery_tracking_data = tracking_data["tracking_details"][-1]
    delivery_information["delivery_city"] = delivery_tracking_data["tracking_location"][
        "city"
    ].title()
    delivery_information["delivery_state"] = delivery_tracking_data[
        "tracking_location"
    ]["state"].upper()

    delivery_datetime = datetime.strptime(
        delivery_tracking_data["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    )
    delivery_information["delivery_date"] = delivery_datetime.date()
    delivery_information["delivery_date_readable"] = delivery_datetime.strftime(
        "%a %-m/%-d"
    )
    delivery_information["date_and_location_of_mailer_delivered"] = (
        f"{delivery_information['delivery_date_readable']} to {delivery_information['delivery_city']}, {delivery_information['delivery_state']}"
    )
    delivery_information["location_delivered"] = (
        f"{delivery_information['delivery_city']}, {delivery_information['delivery_state']}"
    )

    logger.info(f"Delivery information parsed: {delivery_information}")
    return delivery_information


def update_delivery_information_for_lead(lead_id, delivery_information):
    """Update lead with delivery information."""

    def verify_delivery_information_updated(response_data, lead_update_data):
        for key, value in lead_update_data.items():
            if key not in response_data or response_data[key] != value:
                return False
        return True

    custom_field_ids = {
        "date_and_location_of_mailer_delivered": {
            "type": "text",
            "value": "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9",
        },
        "package_delivered": {
            "type": "dropdown_single",
            "value": "custom.cf_wkZ5ptOR1Ro3YPxJPYipI35M7ticuYvJHFgp2y4fzdQ",
        },
        "state_delivered": {
            "type": "text",
            "value": "custom.cf_vxfsYfTrFk6oYrnSx0ViYrUMpE7y5sxi0NnRgTyOf30",
        },
        "city_delivered": {
            "type": "text",
            "value": "custom.cf_1hWUFxiA6QhUXrYT3lDh96JSWKxVBBAKCB3XO8EXGUW",
        },
        "date_delivered": {
            "type": "date",
            "value": "custom.cf_jVU4YFLX5bDq2dRjvBapPYAJxGP0iQWid9QC7cQjSCR",
        },
        "date_delivered_readable": {
            "type": "text",
            "value": "custom.cf_jGC3O9doWfvwFV49NBIRGwA0PFIcKMzE0h8Zf65XLCQ",
        },
        "location_delivered": {
            "type": "text",
            "value": "custom.cf_hPAtbaFuztYBQcYVqsk4pIFV0hKvnlb696TknlzEERS",
        },
    }
    lead_update_data = {
        custom_field_ids["date_and_location_of_mailer_delivered"][
            "value"
        ]: delivery_information["date_and_location_of_mailer_delivered"],
        custom_field_ids["package_delivered"]["value"]: "Yes",
        custom_field_ids["state_delivered"]["value"]: delivery_information[
            "delivery_state"
        ],
        custom_field_ids["city_delivered"]["value"]: delivery_information[
            "delivery_city"
        ],
        custom_field_ids["date_delivered"]["value"]: delivery_information[
            "delivery_date"
        ].isoformat(),
        custom_field_ids["date_delivered_readable"]["value"]: delivery_information[
            "delivery_date_readable"
        ],
        custom_field_ids["location_delivered"]["value"]: delivery_information[
            "location_delivered"
        ],
    }

    response = make_close_request(
        "put",
        f"https://api.close.com/api/v1/lead/{lead_id}",
        json=lead_update_data,
    )
    if response.status_code != 200:
        logger.error(
            f"Failed to update delivery information for lead {lead_id}: {response.status_code}, \n {response.text}, \n {response.json()}"
        )
        raise Exception("Close accepted the lead, but the fields did not update.")
    response_data = response.json()
    data_updated = verify_delivery_information_updated(response_data, lead_update_data)
    if not data_updated:
        error_message = f"Delivery information update failed for lead {lead_id}."
        logger.error(error_message)
        send_email(subject="Delivery information update failed", body=error_message)
        raise Exception("Close accepted the lead, but the fields did not update.")
    logger.info(f"Delivery information updated for lead {lead_id}: {data_updated}")
    return response_data


def check_existing_mailer_delivered_activities(lead_id):
    """
    Check if there are existing 'Mailer Delivered' custom activities for a lead.

    Args:
        lead_id (str): The ID of the lead to check

    Returns:
        bool: True if existing activities found, False otherwise
    """
    try:
        params = {
            "lead_id": lead_id,
            "custom_activity_type_id": "custom.actitype_3KhBfWgjtVfiGYbczbgOWv",  # Mailer Delivered activity type
        }

        response = make_close_request(
            "get",
            "https://api.close.com/api/v1/activity/custom/",
            params=params,
        )

        if response.status_code == 200:
            response_data = response.json()
            activities = response_data.get("data", [])

            # Return True if any mailer delivered activities found, False otherwise
            has_existing_delivered_activities = len(activities) > 0

            if has_existing_delivered_activities:
                logger.info(
                    f"Found {len(activities)} existing mailer delivered activities for lead {lead_id}"
                )
            else:
                logger.info(
                    f"No existing mailer delivered activities found for lead {lead_id}"
                )

            return has_existing_delivered_activities
        else:
            logger.error(
                f"Failed to check existing activities for lead {lead_id}: {response.status_code}, {response.text}"
            )
            # Fail-safe: return False to allow activity creation if check fails
            return False

    except Exception as e:
        logger.error(
            f"Error checking existing mailer delivered activities for lead {lead_id}: {str(e)}"
        )
        # Fail-safe: return False to allow activity creation if check fails
        return False


def create_package_delivered_custom_activity_in_close(lead_id, delivery_information):
    """Create a custom activity in Close for delivered package."""
    # Check if there are already existing mailer delivered activities for this lead
    if check_existing_mailer_delivered_activities(lead_id):
        logger.info(
            f"Mailer delivered custom activity already exists for lead {lead_id}, skipping creation"
        )
        return {"status": "skipped", "reason": "duplicate_activity_exists"}

    custom_activity_field_ids = {
        "date_and_location_of_mailer_delivered": {
            "type": "text",
            "value": "custom.cf_f652JX1NlPz5P5h7Idqs0uOosb9nomncygP3pJ8GcOS",
        },
        "state_delivered": {
            "type": "text",
            "value": "custom.cf_7wWKPs9vnRZTpgJRdJ79S3NYeT9kq8dCSgRIrVvYe8S",
        },
        "city_delivered": {
            "type": "text",
            "value": "custom.cf_OJXwT7BAZi0qCfdFvzK0hTcPoUUGTxP6bWGIUpEGqOE",
        },
        "date_delivered": {
            "type": "date",
            "value": "custom.cf_wS7icPETKthDz764rkbuC1kQYzP0l88CzlMxoJAlOkO",
        },
        "date_delivered_readable": {
            "type": "text",
            "value": "custom.cf_gUsxB1J9TG1pWG8iC3XYZR9rRXBcHQ86aEJUIFme6LA",
        },
        "location_delivered": {
            "type": "text",
            "value": "custom.cf_Wzp0dZ2D8PQTCKUiKMGsYUVDnURtisF6g9Lwz72WM8m",
        },
    }
    lead_activity_data = {
        "lead_id": lead_id,
        "custom_activity_type_id": "custom.actitype_3KhBfWgjtVfiGYbczbgOWv",  # Activity Type: Mailer Delivered
        custom_activity_field_ids["date_and_location_of_mailer_delivered"][
            "value"
        ]: delivery_information["date_and_location_of_mailer_delivered"],
        custom_activity_field_ids["state_delivered"]["value"]: delivery_information[
            "delivery_state"
        ],
        custom_activity_field_ids["city_delivered"]["value"]: delivery_information[
            "delivery_city"
        ],
        custom_activity_field_ids["date_delivered"]["value"]: delivery_information[
            "delivery_date"
        ].isoformat(),
        custom_activity_field_ids["date_delivered_readable"][
            "value"
        ]: delivery_information["delivery_date_readable"],
        custom_activity_field_ids["location_delivered"]["value"]: delivery_information[
            "location_delivered"
        ],
    }

    response = make_close_request(
        "post",
        "https://api.close.com/api/v1/activity/custom/",
        json=lead_activity_data,
    )
    response_data = response.json()
    logger.info(f"Delivery activity updated for lead {lead_id}: {response.json()}")
    return response_data


@easypost_bp.route("/delivery_status", methods=["POST"])
def handle_package_delivery_update():
    """Handle package delivery status updates from EasyPost webhook - Async Processing."""
    try:
        # Quick validation of request data
        if not request.json or "result" not in request.json:
            return jsonify(
                {"status": "error", "message": "Invalid request format"}
            ), 400

        tracking_data = request.json["result"]
        easy_post_event_id = request.json["id"]
        logger.info(f"EasyPost Event ID: {easy_post_event_id}")

        # Quick check for non-delivered status - handle immediately without queuing
        if tracking_data["status"] != "delivered":
            logger.info("Tracking status is not 'delivered'; webhook did not run.")
            webhook_data = {
                "event_id": easy_post_event_id,
                "tracking_code": tracking_data.get("tracking_code"),
                "carrier": tracking_data.get("carrier"),
                "status": tracking_data.get("status"),
                "route": "delivery_status",
                "timestamp": datetime.now().isoformat(),
                "processed": True,
                "result": "Not delivered",
            }
            _webhook_tracker.add(tracking_data.get("id"), webhook_data)

            return jsonify(
                {
                    "status": "success",
                    "message": "Tracking status is not 'delivered' so did not run.",
                }
            ), 200

        # Quick check for delivered to original sender - handle immediately
        if (
            tracking_data.get("tracking_details")
            and len(tracking_data["tracking_details"]) > 0
            and tracking_data["tracking_details"][-1].get("message")
            == "Delivered, To Original Sender"
        ):
            logger.info(
                "Tracking status is 'delivered', but it is delivered to the original sender; webhook did not run."
            )
            webhook_data = {
                "event_id": easy_post_event_id,
                "tracking_code": tracking_data.get("tracking_code"),
                "carrier": tracking_data.get("carrier"),
                "status": tracking_data.get("status"),
                "route": "delivery_status",
                "timestamp": datetime.now().isoformat(),
                "processed": True,
                "result": "Delivered to original sender",
            }
            _webhook_tracker.add(tracking_data.get("id"), webhook_data)

            return jsonify(
                {
                    "status": "success",
                    "message": "Tracking status is 'delivered', but it is delivered to the original sender; webhook did not run.",
                }
            ), 200

        # Queue the task for background processing
        task = process_delivery_status_task.delay(request.json)

        # Store initial webhook data for tracking
        webhook_data = {
            "event_id": easy_post_event_id,
            "tracking_code": tracking_data.get("tracking_code"),
            "carrier": tracking_data.get("carrier"),
            "status": tracking_data.get("status"),
            "route": "delivery_status",
            "timestamp": datetime.now().isoformat(),
            "processed": False,
            "task_id": task.id,
        }

        # Use tracker ID as key for webhook tracking
        _webhook_tracker.add(tracking_data.get("id"), webhook_data)

        logger.info(
            f"EasyPost delivery status task queued: {task.id} for tracker {tracking_data.get('id')}"
        )

        return jsonify(
            {
                "status": "accepted",
                "message": "Delivery status processing task queued for background processing",
                "celery_task_id": task.id,
                "tracker_id": tracking_data.get("id"),
                "tracking_code": tracking_data.get("tracking_code"),
            }
        ), 202

    except Exception as e:
        error_msg = f"Error queuing EasyPost delivery status task: {str(e)}"
        logger.error(error_msg)
        return jsonify({"status": "error", "message": error_msg}), 500


@celery.task(
    name="easypost.process_delivery_status_task",
    bind=True,
    soft_time_limit=300,  # 5 minutes timeout
    time_limit=360,  # 6 minutes hard timeout
    max_retries=3,
    default_retry_delay=60,  # 1 minute retry delay
)
def process_delivery_status_task(self, payload_data):
    """
    Celery task to process delivery status updates in the background.
    This contains the original synchronous logic from handle_package_delivery_update.
    """
    try:
        tracking_data = payload_data["result"]
        easy_post_event_id = payload_data["id"]
        tracker_id = tracking_data.get("id")

        # Continue with processing for delivered packages
        delivery_information = parse_delivery_information(tracking_data)
        close_query_to_find_leads_with_tracking_number = load_query(
            "lead_by_tracking_number.json"
        )
        close_query_to_find_leads_with_tracking_number["query"]["queries"][1][
            "queries"
        ][0]["queries"][0]["condition"]["value"] = tracking_data["tracking_code"]

        close_leads = search_close_leads(close_query_to_find_leads_with_tracking_number)

        try:
            if len(close_leads) == 0:
                error_msg = f"No leads found with tracking number {tracking_data['tracking_code']}"
                logger.warning(error_msg)

                webhook_data = {
                    "processed": True,
                    "result": "No leads found",
                    "timestamp": datetime.now().isoformat(),
                }
                _webhook_tracker.add(tracker_id, webhook_data)

                return {"status": "success", "message": error_msg}

            if len(close_leads) > 1:
                logger.info(
                    f"Multiple leads ({len(close_leads)}) found with tracking number {tracking_data['tracking_code']} and tracker ID {tracking_data['id']}"
                )
                # Check each lead to see which one doesn't return a 404
                valid_leads = []
                for lead in close_leads:
                    lead_id = lead["id"]
                    # Use get_lead_by_id to verify this lead actually exists
                    valid_lead = get_lead_by_id(lead_id)
                    if valid_lead:
                        valid_leads.append(lead)
                        logger.info(f"Verified lead ID: {lead_id} exists")
                    else:
                        logger.warning(f"Lead ID: {lead_id} returned 404 or error")
                if len(valid_leads) == 1:
                    selected_lead = valid_leads[0]
                    logger.info(
                        f"Selected lead ID: {selected_lead['id']} for tracking number {tracking_data['tracking_code']}"
                    )
                    # Continue processing with the selected lead
                    close_leads = [selected_lead]
                elif len(valid_leads) > 1:
                    # If multiple valid leads found, log this and return
                    error_msg = f"Multiple valid leads found for tracking number {tracking_data['tracking_code']} and tracker ID {tracking_data['id']}"
                    logger.warning(error_msg)

                    webhook_data = {
                        "processed": True,
                        "result": "Multiple valid leads found",
                        "timestamp": datetime.now().isoformat(),
                    }
                    _webhook_tracker.add(tracker_id, webhook_data)

                    return {"status": "success", "message": error_msg}
                else:
                    # If no valid leads found, log this and return
                    error_msg = f"No valid leads found for tracking number {tracking_data['tracking_code']} and tracker ID {tracking_data['id']}"
                    logger.warning(error_msg)
                    webhook_data = {
                        "processed": True,
                        "result": "No valid leads found",
                        "timestamp": datetime.now().isoformat(),
                    }
                    _webhook_tracker.add(tracker_id, webhook_data)
                    return {"status": "success", "message": error_msg}
            else:
                # If there's only one lead, validate it
                valid_leads = []
                lead_id = close_leads[0]["id"]
                valid_lead = get_lead_by_id(lead_id)
                if valid_lead:
                    valid_leads.append(close_leads[0])
                    logger.info(f"Verified single lead ID: {lead_id} exists")
                else:
                    error_msg = (
                        f"The only found lead ID: {lead_id} returned 404 or error"
                    )
                    logger.warning(error_msg)
                    webhook_data = {
                        "processed": True,
                        "result": "Lead not found",
                        "timestamp": datetime.now().isoformat(),
                    }
                    _webhook_tracker.add(tracker_id, webhook_data)
                    return {"status": "success", "message": error_msg}

            # Update lead with delivery information
            if not valid_leads:
                error_msg = f"No valid leads available for tracking number {tracking_data['tracking_code']}"
                logger.warning(error_msg)
                webhook_data = {
                    "processed": True,
                    "result": "No valid leads",
                    "timestamp": datetime.now().isoformat(),
                }
                _webhook_tracker.add(tracker_id, webhook_data)
                return {"status": "success", "message": error_msg}

            update_delivery_information_for_lead(
                valid_leads[0]["id"], delivery_information
            )

            # Create custom activity
            create_package_delivered_custom_activity_in_close(
                valid_leads[0]["id"], delivery_information
            )

            # Update webhook tracker
            webhook_data = {
                "processed": True,
                "result": "Success",
                "lead_id": valid_leads[0]["id"],
                "delivery_information": delivery_information,
                "timestamp": datetime.now().isoformat(),
            }
            _webhook_tracker.add(tracker_id, webhook_data)

            logger.info(f"Close lead update: {delivery_information}")

            return {
                "status": "success",
                "delivery_information": delivery_information,
                "lead_id": valid_leads[0]["id"],
            }
        except Exception as e:
            error_message = f"Error updating Close lead: {e}"
            if close_leads and len(close_leads) > 0:
                error_message += f", lead_id={close_leads[0]['id']}"

            logger.error(error_message, exc_info=True)
            send_email(subject="Delivery information update failed", body=error_message + "\n\n" + traceback.format_exc())

            webhook_data = {
                "processed": True,
                "result": "Error",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }
            if close_leads and len(close_leads) > 0:
                webhook_data["lead_id"] = close_leads[0]["id"]
            _webhook_tracker.add(tracker_id, webhook_data)

            # Try to retry the task if possible
            try:
                raise self.retry(exc=e)
            except Exception as retry_error:
                logger.warning(f"Failed to retry task: {retry_error}")
                return {"status": "error", "message": error_message}

    except Exception as e:
        error_message = f"Error processing webhook: {e}"
        tracker_id = None

        try:
            # Add tracking code and carrier if available
            if "tracking_data" in locals() and tracking_data:
                error_message += f", tracking_code={tracking_data.get('tracking_code')}, carrier={tracking_data.get('carrier')}"
                tracker_id = tracking_data.get("id")

            # Add to webhook tracker if we have enough information
            if tracker_id:
                webhook_data = {
                    "event_id": payload_data.get("id", "unknown"),
                    "tracking_code": tracking_data.get("tracking_code")
                    if "tracking_data" in locals()
                    else None,
                    "carrier": tracking_data.get("carrier")
                    if "tracking_data" in locals()
                    else None,
                    "status": tracking_data.get("status")
                    if "tracking_data" in locals()
                    else None,
                    "route": "delivery_status (async)",
                    "timestamp": datetime.now().isoformat(),
                    "processed": True,
                    "result": "Error",
                    "error": str(e),
                }
                _webhook_tracker.add(tracker_id, webhook_data)
        except Exception as tracking_error:
            # Log the error but continue with the main error handling
            logger.warning(
                f"Error adding tracking info to error message: {tracking_error}"
            )

        # Get request ID which serves as run ID
        run_id = str(uuid.uuid4())

        # Extract calling function name
        calling_function = "process_delivery_status_task"

        # Capture the traceback
        tb = traceback.format_exc()

        # Format the error message with detailed information
        detailed_error_message = f"""
        <h2>Delivery Information Update Failed (Async)</h2>
        <p><strong>Error:</strong> {str(e)}</p>
        <p><strong>Route:</strong> delivery_status (async)</p>
        <p><strong>Run ID:</strong> {run_id}</p>
        <p><strong>Origin:</strong> {calling_function}</p>
        <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
        
        <h3>Webhook Data:</h3>
        <pre>{json.dumps(webhook_data if 'webhook_data' in locals() else {}, indent=2, default=str)}</pre>
        
        <h3>Tracking Data:</h3>
        <pre>{json.dumps(tracking_data if 'tracking_data' in locals() else {}, indent=2, default=str)}</pre>
        
        <h3>Traceback:</h3>
        <pre>{tb}</pre>
        """

        logger.error(
            "easypost_webhook_error",
            error=str(e),
            traceback=tb,
            run_id=run_id,
            route="delivery_status (async)",
            origin=calling_function,
        )

        send_email(
            subject="Delivery Information Update Failed (Async)",
            body=detailed_error_message,
        )

        # Try to retry the task if possible
        try:
            raise self.retry(exc=e)
        except Exception as retry_error:
            logger.warning(f"Failed to retry task: {retry_error}")
            return {"status": "error", "message": error_message}


@easypost_bp.route("/webhooks/status", methods=["GET"])
def get_processed_webhooks():
    """
    Get processed webhooks for testing and monitoring purposes.

    Supports filtering by multiple parameters:
    - tracker_id: Filter by EasyPost tracker ID
    - tracking_code: Filter by tracking code
    - event_id: Filter by EasyPost event ID
    - lead_id: Filter by Close lead ID

    Returns all webhooks that match ALL provided filter parameters.
    """
    # Get filter parameters
    tracker_id = request.args.get("tracker_id")
    tracking_code = request.args.get("tracking_code")
    event_id = request.args.get("event_id")
    lead_id = request.args.get("lead_id")

    # Dictionary of filter parameters that were provided
    filters = {}
    if tracker_id:
        filters["id"] = tracker_id  # EasyPost tracker ID is stored as 'id'
    if tracking_code:
        filters["tracking_code"] = tracking_code
    if event_id:
        filters["event_id"] = event_id
    if lead_id:
        filters["lead_id"] = lead_id

    # If tracker_id is provided, check that specific tracker first for efficiency
    if tracker_id:
        webhook_data = _webhook_tracker.get(tracker_id)
        if webhook_data:
            # Remove tracker_id from filters since we already matched on it
            if "id" in filters:
                del filters["id"]

            # Check if the webhook matches all other filters
            matches_all_filters = True
            for key, value in filters.items():
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
                        "message": f"Webhook for tracker_id: {tracker_id} doesn't match filters: {filter_str}",
                    }
                ), 404
        else:
            return jsonify(
                {
                    "status": "not_found",
                    "message": f"No webhook data found for tracker_id: {tracker_id}",
                }
            ), 404

    # If no tracker_id or multiple filters, get all webhooks and filter
    all_webhooks = _webhook_tracker.get_all()

    # If no filters, return all webhooks
    if not filters:
        return jsonify({"status": "success", "data": all_webhooks}), 200

    # Filter webhooks based on provided parameters
    filtered_webhooks = {}
    for webhook_id, webhook in all_webhooks.items():
        matches_all_filters = True
        for key, value in filters.items():
            if webhook.get(key) != value:
                matches_all_filters = False
                break

        if matches_all_filters:
            filtered_webhooks[webhook_id] = webhook

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


@easypost_bp.route("/sync_delivery_status", methods=["GET"])
def sync_delivery_status_from_easypost():
    """
    Manually trigger a sync of delivery status from EasyPost.
    This endpoint queues a background task that runs in two phases:
    1. Create EasyPost trackers for leads that have tracking numbers but no EasyPost tracker IDs
    2. Check delivery status for all leads with EasyPost tracker IDs

    The task runs asynchronously and can be monitored using the /sync_delivery_status/status/<task_id> endpoint.
    """
    # Queue the task to run in the background
    task = sync_delivery_status_task.delay()

    return jsonify(
        {
            "status": "success",
            "message": "Delivery status sync task has been queued",
            "task_id": task.id,
        }
    ), 200


@easypost_bp.route("/sync_delivery_status/status/<task_id>", methods=["GET"])
def check_sync_task_status(task_id):
    """
    Check the status of a running sync task.

    Args:
        task_id: The ID of the task to check

    Returns:
        JSON response with the task status
    """
    task = sync_delivery_status_task.AsyncResult(task_id)

    if task.state == "PENDING":
        response = {"state": task.state, "status": "Task is pending"}
    elif task.state == "FAILURE":
        response = {
            "state": task.state,
            "status": "Task failed",
            "error": str(task.info),
        }
    elif task.state == "SUCCESS":
        response = {
            "state": task.state,
            "status": "Task completed successfully",
            "result": task.info,
        }
    else:
        # Task is likely in STARTED or PROGRESS state
        response = {"state": task.state, "status": "Task is in progress"}

    return jsonify(response)


@celery.task(
    name="easypost.sync_delivery_status_task",
    bind=True,
    soft_time_limit=3600,  # 1 hour timeout
    time_limit=3900,  # 1 hour 5 minutes hard timeout
    max_retries=3,
    default_retry_delay=300,  # 5 minutes retry delay
)
def sync_delivery_status_task(self):
    """
    Celery task to sync delivery status from EasyPost in two phases:
    1. Create EasyPost trackers for leads that have tracking numbers but no EasyPost tracker IDs
    2. Check delivery status for all leads with EasyPost tracker IDs
    """
    results = {"trackers_created": 0, "delivery_updates": 0, "errors": 0}

    try:
        # Update task state to show progress
        self.update_state(state="PROGRESS", meta={"status": "Starting sync task"})

        # Phase 1: Create trackers for leads without EasyPost tracker IDs
        logger.info("Phase 1: Creating trackers for leads without EasyPost tracker IDs")
        query_leads_without_easypost_trackers = load_query(
            "undelivered_package_without_easypost_trackers.json"
        )
        leads_without_trackers = search_close_leads(
            query_leads_without_easypost_trackers
        )

        if leads_without_trackers:
            logger.info(
                f"Found {len(leads_without_trackers)} leads without EasyPost tracker IDs"
            )

            # Update task state with progress information
            self.update_state(
                state="PROGRESS",
                meta={
                    "status": f"Found {len(leads_without_trackers)} leads without EasyPost tracker IDs",
                    "current": 0,
                    "total": len(leads_without_trackers),
                    "phase": "Creating trackers",
                },
            )

            for i, lead in enumerate(leads_without_trackers):
                try:
                    lead_id = lead["id"]
                    # Extract tracking number and carrier
                    tracking_number = lead.get(
                        "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
                    )
                    carrier_field = lead.get(
                        "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l"
                    )

                    if not tracking_number or not carrier_field:
                        logger.warning(
                            f"Lead {lead_id} is missing tracking number or carrier"
                        )
                        continue

                    carrier = (
                        carrier_field[0]
                        if isinstance(carrier_field, list)
                        else carrier_field
                    )

                    # Get appropriate EasyPost client based on tracking number
                    client = get_easypost_client(tracking_number)

                    # Create tracker in EasyPost
                    tracker = client.tracker.create(
                        tracking_code=tracking_number, carrier=carrier
                    )

                    # Update lead with EasyPost tracker ID
                    update_easypost_tracker_id_for_lead(
                        lead_id, {"easypost_tracker_id": tracker.id}
                    )

                    logger.info(
                        f"Created EasyPost tracker {tracker.id} for lead {lead_id}"
                    )
                    results["trackers_created"] += 1

                    # Update progress every 5 items or at the end
                    if (i + 1) % 5 == 0 or i == len(leads_without_trackers) - 1:
                        self.update_state(
                            state="PROGRESS",
                            meta={
                                "status": f"Creating trackers: {i+1}/{len(leads_without_trackers)}",
                                "current": i + 1,
                                "total": len(leads_without_trackers),
                                "phase": "Creating trackers",
                                "results": results,
                            },
                        )

                except Exception as e:
                    error_msg = (
                        f"Error creating tracker for lead {lead.get('id')}: {str(e)}"
                    )
                    logger.error(error_msg)
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    results["errors"] += 1
                    continue
        else:
            logger.info("No leads found without EasyPost tracker IDs")

        # Phase 2: Check delivery status for leads with EasyPost tracker IDs
        logger.info(
            "Phase 2: Checking delivery status for leads with EasyPost tracker IDs"
        )

        # Update task state for phase 2
        self.update_state(
            state="PROGRESS",
            meta={
                "status": "Starting Phase 2: Checking delivery status",
                "current": 0,
                "total": 0,  # Will be updated when we know how many leads we have
                "phase": "Checking delivery status",
                "results": results,
            },
        )

        query_leads_with_undelivered_packages_in_close = load_query(
            "undelivered_package_with_easypost_tracker_id.json"
        )
        leads_with_trackers = search_close_leads(
            query_leads_with_undelivered_packages_in_close
        )

        if not leads_with_trackers:
            logger.info(
                "No leads found with undelivered packages that have EasyPost tracker IDs"
            )

            # Update state to show no leads found
            self.update_state(
                state="PROGRESS",
                meta={
                    "status": "No leads found with undelivered packages that have EasyPost tracker IDs",
                    "current": 0,
                    "total": 0,
                    "phase": "Checking delivery status - Complete",
                    "results": results,
                },
            )
        else:
            logger.info(
                f"Found {len(leads_with_trackers)} leads with EasyPost tracker IDs to check"
            )

            # Update state with total leads found
            self.update_state(
                state="PROGRESS",
                meta={
                    "status": f"Found {len(leads_with_trackers)} leads with EasyPost tracker IDs to check",
                    "current": 0,
                    "total": len(leads_with_trackers),
                    "phase": "Checking delivery status",
                    "results": results,
                },
            )

            # Check each lead's shipment status via EasyPost
            for i, lead in enumerate(leads_with_trackers):
                try:
                    easypost_tracker_id = lead[
                        "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
                    ]

                    # Get tracking number to determine which client to use
                    tracking_number = lead.get(
                        "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
                    )

                    # Get the appropriate client based on tracking number
                    client = get_easypost_client(tracking_number)

                    tracker = client.tracker.retrieve(easypost_tracker_id)
                    tracking_data = tracker

                    if tracking_data["status"] != "delivered":
                        logger.info(
                            f"Lead {lead['id']}: Tracking status is not 'delivered'; webhook did not run."
                        )
                    elif (
                        tracking_data["tracking_details"][-1]["message"]
                        == "Delivered, To Original Sender"
                    ):
                        logger.info(
                            f"Lead {lead['id']}: Tracking status is 'delivered', but it is delivered to the original sender; webhook did not run."
                        )
                    else:
                        delivery_information = parse_delivery_information(tracking_data)
                        # Update the delivery information in Close
                        update_delivery_information_for_lead(
                            lead["id"], delivery_information
                        )
                        create_package_delivered_custom_activity_in_close(
                            lead["id"], delivery_information
                        )
                        logger.info(f"Updated delivery status for lead {lead['id']}")
                        results["delivery_updates"] += 1

                    # Update progress every 5 items or at the end
                    if (i + 1) % 5 == 0 or i == len(leads_with_trackers) - 1:
                        self.update_state(
                            state="PROGRESS",
                            meta={
                                "status": f"Checking delivery status: {i+1}/{len(leads_with_trackers)}",
                                "current": i + 1,
                                "total": len(leads_with_trackers),
                                "phase": "Checking delivery status",
                                "results": results,
                            },
                        )

                except Exception as e:
                    error_msg = f"Error checking delivery status for lead {lead['id']}: {str(e)}"
                    logger.error(error_msg)
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    results["errors"] += 1
                    continue

        # Return summary of all operations
        summary = (
            f"Process completed. {results['trackers_created']} tracker(s) created, "
            f"{results['delivery_updates']} delivery status(es) updated, "
            f"{results['errors']} error(s) encountered."
        )
        logger.info(summary)

        # Return results for task tracking
        return {
            "status": "success",
            "message": summary,
            "details": results,
        }

    except Exception as e:
        error_msg = f"Error syncing delivery status: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")

        # Try to retry the task if possible
        try:
            # Retry up to max_retries times
            raise self.retry(exc=e)
        except Exception as retry_error:
            # If we've exceeded retries or can't retry, return error
            logger.warning(f"Failed to retry task: {retry_error}")
            return {"status": "error", "message": error_msg}
