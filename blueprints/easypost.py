"""
Blueprint for EasyPost integration.
This module handles EasyPost webhook tracking and delivery status updates.
"""

import os
import json
from datetime import datetime, date
import traceback
import requests
from flask import Blueprint, request, jsonify
import easypost
from redis import Redis
import structlog
from close_utils import load_query, search_close_leads, get_lead_by_id
from celery_worker import celery

# Initialize Blueprint
easypost_bp = Blueprint("easypost", __name__)

# Initialize logger
logger = structlog.get_logger()

# API keys
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = None  # This will be initialized when needed
EASYPOST_PROD_API_KEY = os.environ.get("EASYPOST_PROD_API_KEY")
EASYPOST_TEST_API_KEY = os.environ.get("EASYPOST_TEST_API_KEY")
ENV_TYPE = os.environ.get("ENV_TYPE", "development")


# EasyPost client setup
def get_easypost_client(tracking_number=None):
    """
    Get EasyPost client based on tracking number.

    Args:
        tracking_number: The tracking number to check. If it follows test format
                         (e.g., starts with "EZ"), use test API key.

    Returns:
        EasyPost client instance with appropriate API key

    Raises:
        ValueError: If a test tracking number is used but EASYPOST_TEST_API_KEY is not set
    """
    # Default to production API key
    api_key = EASYPOST_PROD_API_KEY

    # If tracking number follows test format (e.g., starts with "EZ"), use test API key
    if tracking_number and (
        tracking_number.startswith("EZ") or tracking_number.startswith("ez")
    ):
        if EASYPOST_TEST_API_KEY:
            api_key = EASYPOST_TEST_API_KEY
            logger.info(
                f"Using EasyPost test API key for tracking number: {tracking_number}"
            )
        else:
            error_msg = f"EASYPOST_TEST_API_KEY is not set but required for test tracking number: {tracking_number}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    else:
        logger.info(
            f"Using EasyPost production API key for tracking number: {tracking_number}"
        )

    return easypost.EasyPostClient(api_key=api_key)


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
    from app import send_email as app_send_email

    return app_send_email(subject, body, **kwargs)


@easypost_bp.route("/create_tracker", methods=["POST"])
def create_easypost_tracker():
    """Create an EasyPost tracker for a lead in Close."""
    try:
        # Get lead ID from request
        data = request.json.get("event").get("data")
        lead_id = data.get("id")

        if not lead_id:
            return jsonify({"status": "error", "message": "No lead_id provided"}), 400

        # Get close API key
        get_close_encoded_key()

        # Get lead data from Close
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {CLOSE_ENCODED_KEY}",
        }

        response = requests.get(
            f"https://api.close.com/api/v1/lead/{lead_id}",
            headers=headers,
        )

        if response.status_code != 200:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Failed to fetch lead data: {response.text}",
                }
            ), response.status_code

        lead_data = response.json()

        # Extract tracking number and carrier
        tracking_number = lead_data.get(
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
        )
        carrier_field = lead_data.get(
            "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l"
        )

        if not tracking_number or not carrier_field:
            return jsonify(
                {
                    "status": "error",
                    "message": "Lead doesn't have tracking number or carrier",
                }
            ), 400

        carrier = carrier_field[0] if isinstance(carrier_field, list) else carrier_field

        # Get appropriate EasyPost client based on tracking number
        client = get_easypost_client(tracking_number)

        # Create tracker in EasyPost using the appropriate client
        tracker = client.tracker.create(tracking_code=tracking_number, carrier=carrier)

        # Update lead with EasyPost tracker ID
        update_easypost_tracker_id_for_lead(
            lead_id, {"easypost_tracker_id": tracker.id}
        )

        logger.info(f"EasyPost Tracker Created: {tracker} for lead {lead_id}")

        return jsonify(
            {
                "status": "success",
                "tracker_id": tracker.id,
                "tracking_code": tracking_number,
                "carrier": carrier,
            }
        ), 200

    except Exception as e:
        error_msg = f"Error creating EasyPost tracker: {str(e)}"
        logger.error(error_msg)
        return jsonify({"status": "error", "message": error_msg}), 500


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

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {get_close_encoded_key()}",
    }

    response = requests.put(
        f"https://api.close.com/api/v1/lead/{lead_id}",
        json=lead_update_data,
        headers=headers,
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

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {get_close_encoded_key()}",
    }

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

    response = requests.put(
        f"https://api.close.com/api/v1/lead/{lead_id}",
        json=lead_update_data,
        headers=headers,
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


def create_package_delivered_custom_activity_in_close(lead_id, delivery_information):
    """Create a custom activity in Close for delivered package."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {get_close_encoded_key()}",
    }

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

    response = requests.post(
        "https://api.close.com/api/v1/activity/custom/",
        json=lead_activity_data,
        headers=headers,
    )
    response_data = response.json()
    logger.info(f"Delivery activity updated for lead {lead_id}: {response.json()}")
    return response_data


@easypost_bp.route("/delivery_status", methods=["POST"])
def handle_package_delivery_update():
    """Handle package delivery status updates from EasyPost webhook."""
    try:
        tracking_data = request.json["result"]
        easy_post_event_id = request.json["id"]
        logger.info(f"EasyPost Event ID: {easy_post_event_id}")

        # Store webhook data for status tracking
        webhook_data = {
            "event_id": easy_post_event_id,
            "tracking_code": tracking_data.get("tracking_code"),
            "carrier": tracking_data.get("carrier"),
            "status": tracking_data.get("status"),
            "route": "delivery_status",
            "timestamp": datetime.now().isoformat(),
            "processed": False,
        }

        if tracking_data["status"] != "delivered":
            logger.info("Tracking status is not 'delivered'; webhook did not run.")
            webhook_data["processed"] = True
            webhook_data["result"] = "Not delivered"
            _webhook_tracker.add(tracking_data.get("id"), webhook_data)

            return jsonify(
                {
                    "status": "success",
                    "message": "Tracking status is not 'delivered' so did not run.",
                }
            ), 200

        if (
            tracking_data["tracking_details"][-1]["message"]
            == "Delivered, To Original Sender"
        ):
            logger.info(
                "Tracking status is 'delivered', but it is delivered to the original sender; webhook did not run."
            )
            webhook_data["processed"] = True
            webhook_data["result"] = "Delivered to original sender"
            _webhook_tracker.add(tracking_data.get("id"), webhook_data)

            return jsonify(
                {
                    "status": "success",
                    "message": "Tracking status is 'delivered', but it is delivered to the original sender; webhook did not run.",
                }
            ), 200

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

                    webhook_data["processed"] = True
                    webhook_data["result"] = "Multiple valid leads found"
                    _webhook_tracker.add(tracking_data.get("id"), webhook_data)

                    return jsonify({"status": "success", "message": error_msg}), 200
                else:
                    # If no valid leads found, log this and return
                    error_msg = f"No valid leads found for tracking number {tracking_data['tracking_code']} and tracker ID {tracking_data['id']}"
                    logger.warning(error_msg)
                    webhook_data["processed"] = True
                    webhook_data["result"] = "No valid leads found"
                    _webhook_tracker.add(tracking_data.get("id"), webhook_data)
                    return jsonify({"status": "success", "message": error_msg}), 200
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
                    webhook_data["processed"] = True
                    webhook_data["result"] = "Lead not found"
                    _webhook_tracker.add(tracking_data.get("id"), webhook_data)
                    return jsonify({"status": "success", "message": error_msg}), 200

            if len(close_leads) == 0:
                error_msg = f"No leads found with tracking number {tracking_data['tracking_code']}"
                logger.warning(error_msg)

                webhook_data["processed"] = True
                webhook_data["result"] = "No leads found"
                _webhook_tracker.add(tracking_data.get("id"), webhook_data)

                return jsonify({"status": "success", "message": error_msg}), 200

            # Update lead with delivery information
            if not valid_leads:
                error_msg = f"No valid leads available for tracking number {tracking_data['tracking_code']}"
                logger.warning(error_msg)
                webhook_data["processed"] = True
                webhook_data["result"] = "No valid leads"
                _webhook_tracker.add(tracking_data.get("id"), webhook_data)
                return jsonify({"status": "success", "message": error_msg}), 200

            update_delivery_information_for_lead(
                valid_leads[0]["id"], delivery_information
            )

            # Create custom activity
            create_package_delivered_custom_activity_in_close(
                valid_leads[0]["id"], delivery_information
            )

            # Update webhook tracker
            webhook_data["processed"] = True
            webhook_data["result"] = "Success"
            webhook_data["lead_id"] = valid_leads[0]["id"]
            webhook_data["delivery_information"] = delivery_information
            _webhook_tracker.add(tracking_data.get("id"), webhook_data)

            logger.info(f"Close lead update: {delivery_information}")

            return jsonify(
                {"status": "success", "delivery_information": delivery_information}
            ), 200
        except Exception as e:
            error_message = f"Error updating Close lead: {e}"
            if close_leads and len(close_leads) > 0:
                error_message += f", lead_id={close_leads[0]['id']}"

            logger.error(error_message)
            send_email(subject="Delivery information update failed", body=error_message)

            webhook_data["processed"] = True
            webhook_data["result"] = "Error"
            webhook_data["error"] = str(e)
            if close_leads and len(close_leads) > 0:
                webhook_data["lead_id"] = close_leads[0]["id"]
            _webhook_tracker.add(tracking_data.get("id"), webhook_data)

            return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        error_message = f"Error processing webhook: {e}"

        try:
            # Add tracking code and carrier if available
            if "tracking_data" in locals() and tracking_data:
                error_message += f", tracking_code={tracking_data.get('tracking_code')}, carrier={tracking_data.get('carrier')}"

            # Add to webhook tracker if we have enough information
            if (
                "tracking_data" in locals()
                and tracking_data
                and tracking_data.get("id")
            ):
                webhook_data = {
                    "event_id": request.json.get("id", "unknown"),
                    "tracking_code": tracking_data.get("tracking_code"),
                    "carrier": tracking_data.get("carrier"),
                    "status": tracking_data.get("status"),
                    "route": "delivery_status",
                    "timestamp": datetime.now().isoformat(),
                    "processed": True,
                    "result": "Error",
                    "error": str(e),
                }
                _webhook_tracker.add(tracking_data.get("id"), webhook_data)
        except Exception as tracking_error:
            # Log the error but continue with the main error handling
            logger.warning(
                f"Error adding tracking info to error message: {tracking_error}"
            )

        logger.error(error_message)
        send_email(subject="Delivery information update failed", body=error_message)
        return jsonify({"status": "error", "message": str(e)}), 400


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


@easypost_bp.route("/create_tracker_for_lead/<lead_id>", methods=["POST"])
def create_tracker_for_lead(lead_id):
    """
    Create an EasyPost tracker for a specific lead.
    This endpoint is designed to be called when a new tracking number is added to a lead.

    Args:
        lead_id: The Close lead ID to create a tracker for

    Returns:
        JSON response with the created tracker information
    """
    try:
        # Get lead data from Close
        lead = get_lead_by_id(lead_id)

        if not lead:
            return jsonify(
                {"status": "error", "message": f"Lead {lead_id} not found"}
            ), 404

        # Extract tracking number and carrier
        tracking_number = lead.get(
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
        )
        carrier_field = lead.get(
            "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l"
        )

        if not tracking_number or not carrier_field:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Lead {lead_id} is missing tracking number or carrier",
                }
            ), 400

        carrier = carrier_field[0] if isinstance(carrier_field, list) else carrier_field

        # Get appropriate EasyPost client based on tracking number
        client = get_easypost_client(tracking_number)

        # Create tracker in EasyPost
        tracker = client.tracker.create(tracking_code=tracking_number, carrier=carrier)

        # Update lead with EasyPost tracker ID
        update_easypost_tracker_id_for_lead(
            lead_id, {"easypost_tracker_id": tracker.id}
        )

        logger.info(f"Created EasyPost tracker {tracker.id} for lead {lead_id}")

        # Queue a task to check the delivery status immediately
        check_delivery_status_for_lead_task.delay(lead_id)

        return jsonify(
            {
                "status": "success",
                "message": f"Created EasyPost tracker for lead {lead_id}",
                "tracker_id": tracker.id,
                "tracking_code": tracking_number,
                "carrier": carrier,
            }
        ), 200

    except Exception as e:
        error_msg = f"Error creating EasyPost tracker for lead {lead_id}: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": error_msg}), 500


@celery.task(
    name="easypost.check_delivery_status_for_lead_task",
    bind=True,
    soft_time_limit=600,  # 10 minutes timeout
    max_retries=3,
    default_retry_delay=300,  # 5 minutes retry delay
)
def check_delivery_status_for_lead_task(self, lead_id):
    """
    Celery task to check delivery status for a specific lead.

    Args:
        lead_id: The Close lead ID to check delivery status for
    """
    try:
        # Get lead data from Close
        lead = get_lead_by_id(lead_id)

        if not lead:
            logger.error(f"Lead {lead_id} not found")
            return {"status": "error", "message": f"Lead {lead_id} not found"}

        # Get EasyPost tracker ID
        easypost_tracker_id = lead.get(
            "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
        )

        if not easypost_tracker_id:
            logger.error(f"Lead {lead_id} does not have an EasyPost tracker ID")
            return {
                "status": "error",
                "message": f"Lead {lead_id} does not have an EasyPost tracker ID",
            }

        # Get tracking number to determine which client to use
        tracking_number = lead.get(
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
        )

        # Get the appropriate client based on tracking number
        client = get_easypost_client(tracking_number)

        # Retrieve tracker from EasyPost
        tracker = client.tracker.retrieve(easypost_tracker_id)
        tracking_data = tracker

        if tracking_data["status"] != "delivered":
            logger.info(f"Lead {lead_id}: Tracking status is not 'delivered'")
            return {
                "status": "success",
                "message": f"Lead {lead_id}: Tracking status is {tracking_data['status']}",
            }

        if (
            tracking_data["tracking_details"][-1]["message"]
            == "Delivered, To Original Sender"
        ):
            logger.info(
                f"Lead {lead_id}: Tracking status is 'delivered', but it is delivered to the original sender"
            )
            return {
                "status": "success",
                "message": f"Lead {lead_id}: Delivered to original sender",
            }

        # Parse delivery information and update lead
        delivery_information = parse_delivery_information(tracking_data)
        update_delivery_information_for_lead(lead_id, delivery_information)
        create_package_delivered_custom_activity_in_close(lead_id, delivery_information)

        logger.info(f"Updated delivery status for lead {lead_id}")
        return {
            "status": "success",
            "message": f"Updated delivery status for lead {lead_id}",
            "delivery_information": delivery_information,
        }

    except Exception as e:
        error_msg = f"Error checking delivery status for lead {lead_id}: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")

        # Try to retry the task if possible
        try:
            # Retry up to max_retries times
            raise self.retry(exc=e)
        except Exception as retry_error:
            # If we've exceeded retries or can't retry, return error
            logger.warning(f"Failed to retry task for lead {lead_id}: {retry_error}")
            return {"status": "error", "message": error_msg}
