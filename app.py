import csv
import json
import os
import io
import logging
import traceback
from datetime import datetime
from base64 import b64encode
from io import StringIO
from time import sleep
import sys
import uuid
import time

import requests
from flask import Flask, request, jsonify, g
import pytz
import pytest
import structlog
from close_utils import make_close_request


# Configure structlog
def configure_structlog():
    """Configure structured logging for the application."""
    # Set up structlog processors
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Configure structlog based on environment
    if os.environ.get("ENV_TYPE") in ["production", "staging"]:
        # JSON logging for production/staging
        structlog.configure(
            processors=shared_processors
            + [
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    else:
        # Dev-friendly console logging for local development
        structlog.configure(
            processors=shared_processors + [structlog.dev.ConsoleRenderer()],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    # Set up stdlib logging to work with structlog
    handler = logging.StreamHandler()

    # Format as JSON for production/staging environments
    if os.environ.get("ENV_TYPE") in ["production", "staging"]:
        # Use structlog's built-in JSON formatting instead of python-json-logger
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    # Suppress excessive logging from third-party libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


# Configure structlog BEFORE importing blueprints
configure_structlog()

# Create a logger instance for app.py
logger = structlog.get_logger("app")

# Print environment information to verify ENV_TYPE is correctly set
logger.info(
    "environment_info",
    env_type=os.environ.get("ENV_TYPE", "not_set"),
    is_production=os.environ.get("ENV_TYPE") == "production",
    is_staging=os.environ.get("ENV_TYPE") == "staging",
)

# Setup Flask app
flask_app = Flask(__name__)

try:
    from temporal.service import temporal
    temporal.start()
    logger.info("temporal_client_started", ok=True)
except Exception as e:
    logger.exception("temporal_client_start_failed", error=str(e))


# Configure Redis and Celery
REDISCLOUD_URL = os.environ.get("REDISCLOUD_URL")
flask_app.config["CELERY_BROKER_URL"] = REDISCLOUD_URL
flask_app.config["CELERY_RESULT_BACKEND"] = REDISCLOUD_URL

# Import celery instance from celery_worker
from celery_worker import celery

# Configure Celery timezone (but don't use Beat scheduling)
celery.conf.timezone = "America/Chicago"

# Now import blueprints after Celery is configured
# noqa: E402 - Disable linter warning about imports not at top of file
from blueprints.instantly import instantly_bp  # noqa: E402
from blueprints.easypost import easypost_bp  # noqa: E402
from blueprints.gmail import gmail_bp  # noqa: E402


# Middleware to add request ID to each request
@flask_app.before_request
def add_request_id():
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    g.request_id = request_id
    # Store request start time for duration calculation
    g.start_time = time.time()

    # Add request_id to all log entries for this request
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        method=request.method,
        path=request.path,
        timestamp=datetime.utcnow().isoformat(),
    )

    # For webhook requests, log the start of processing with detailed info
    if "/webhook" in request.path or "/email_sent" in request.path:
        logger.info(
            "webhook_received",
            content_type=request.content_type,
            content_length=request.content_length,
            params=dict(request.args),
            remote_addr=request.remote_addr,
            heroku_request_id=request.headers.get("X-Request-ID", "none"),
        )


@flask_app.after_request
def log_response(response):
    """Log the response status for all requests."""
    # Only log details for webhook endpoints
    if "/webhook" in request.path or "/email_sent" in request.path:
        # Calculate request processing time
        processing_time = None
        if hasattr(g, "start_time"):
            processing_time = time.time() - g.start_time

        # Log the response with detailed timing
        logger.info(
            "webhook_response_sent",
            status_code=response.status_code,
            content_length=response.content_length,
            content_type=response.content_type,
            processing_time_ms=round(processing_time * 1000, 2)
            if processing_time
            else None,
            timestamp=datetime.utcnow().isoformat(),
        )
    return response


@flask_app.errorhandler(Exception)
def handle_exception(e):
    """Comprehensive error handler for all exceptions."""
    # Capture the traceback
    tb = traceback.format_exc()

    # Get the current route from the request object
    current_route = request.path

    # Get the request ID which serves as a run ID
    run_id = getattr(g, "request_id", str(uuid.uuid4()))

    # Extract the calling function name from the traceback
    calling_function = "Unknown"
    for frame in traceback.extract_tb(sys.exc_info()[2]):
        if frame.name != "handle_exception":
            calling_function = f"{frame.filename}:{frame.name}:{frame.lineno}"
            break

    # Extract webhook information if available
    webhook_info = {}
    if request.method == "POST" and request.is_json:
        try:
            # Only include safe webhook data, filter out potentially sensitive info
            webhook_data = request.get_json()
            if isinstance(webhook_data, dict):
                webhook_info = {
                    k: v
                    for k, v in webhook_data.items()
                    if k not in ["auth_token", "email_html", "password"]
                }
        except Exception:
            webhook_info = {"error": "Could not parse webhook data"}

    # Format error message with detailed information
    error_message = f"""
    <h2>Application Error</h2>
    <p><strong>Error:</strong> {str(e)}</p>
    <p><strong>Route:</strong> {current_route}</p>
    <p><strong>Run ID:</strong> {run_id}</p>
    <p><strong>Origin:</strong> {calling_function}</p>
    <p><strong>Time:</strong> {datetime.now().isoformat()}</p>
    
    <h3>Webhook Information:</h3>
    <pre>{json.dumps(webhook_info, indent=2)}</pre>
    
    <h3>Traceback:</h3>
    <pre>{tb}</pre>
    """

    # Plain text version for text_content
    plain_error_message = f"""
    Application Error
    
    Error: {str(e)}
    Route: {current_route}
    Run ID: {run_id}
    Origin: {calling_function}
    Time: {datetime.now().isoformat()}
    
    Webhook Information:
    {json.dumps(webhook_info, indent=2)}
    
    Traceback:
    {tb}
    """

    # General error logging for all routes
    logger.error(
        "application_error",
        error=str(e),
        route=current_route,
        run_id=run_id,
        origin=calling_function,
        webhook_info=webhook_info,
    )

    # Send email notification for all errors
    send_email(
        subject="Application Error",
        body=error_message,
        text_content=plain_error_message,
    )

    # Additional detailed logging for webhook endpoints
    if "/webhook" in request.path or "/email_sent" in request.path:
        logger.exception(
            "webhook_processing_error",
            error_type=type(e).__name__,
            error_message=str(e),
            path=request.path,
            method=request.method,
            run_id=run_id,
            origin=calling_function,
        )

    # Prepare appropriate response based on environment
    if env_type == "development":
        response_body = {
            "status": "error",
            "message": str(e),
            "traceback": tb,
            "route": current_route,
            "run_id": run_id,
            "origin": calling_function,
        }
    else:
        response_body = {
            "status": "error",
            "message": "An internal server error occurred at " + current_route,
            "error_type": type(e).__name__,
            "run_id": run_id,
        }

    return jsonify(response_body), 500


# Add your project to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def app():
    # Configure your app for testing
    flask_app.config.update(
        {
            "TESTING": True,
            "ENV": "test",
        }
    )
    yield flask_app


@pytest.fixture
def client(app):
    return flask_app.test_client()


@pytest.fixture
def runner(app):
    return flask_app.test_cli_runner()


# Fixture to load mock webhook payloads
@pytest.fixture
def close_task_created_payload():
    with open("tests/fixtures/close_webhook_payloads/task_created.json", "r") as f:
        return json.load(f)


@pytest.fixture
def instantly_email_sent_payload():
    with open("tests/fixtures/instantly_webhook_payloads/email_sent.json", "r") as f:
        return json.load(f)


env_type = os.getenv("ENV_TYPE", "development")
print("=== ENVIRONMENT INFO ===")
print(f"ENV_TYPE: {env_type}")
print("=== END ENVIRONMENT INFO ===")

# API Keys
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
CLOSE_ENCODED_KEY = b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
SKYLEAD_API_KEY = os.environ.get("SKYLEAD_API_KEY")
WEBHOOK_API_KEY = os.environ.get("WEBHOOK_API_KEY")
BYTESCALE_ACCOUNT_ID = os.environ.get("BYTESCALE_ACCOUNT_ID")
BYTESCALE_API_KEY = os.environ.get("BYTESCALE_API_KEY")


# Check if development scheduling is enabled
ENABLE_DEV_SCHEDULING = (
    os.environ.get("ENABLE_DEV_SCHEDULING", "false").lower() == "true"
)


def send_email(subject, body, **kwargs):
    """
    Send an email using the Gmail API.

    In production, emails are sent to the entire team by default:
    - Lance Johnson
    - Barbara Pigg
    - Kori Watkins
    - Noura Mahmoud

    In development/staging, no emails are sent.

    Args:
        subject (str): The email subject
        body (str): The HTML content for the email body
        **kwargs: Additional parameters
            - recipients: Override default recipients for this specific email
            - text_content: Plain text version of the email (optional)

    Returns:
        dict: Response from Gmail API
    """
    if env_type.lower() != "production":
        return {"status": "success", "message": "Email not sent in non-production env"}
    
    central_time_zone = pytz.timezone("America/Chicago")
    central_time_now = datetime.now(central_time_zone)
    time_now_formatted = central_time_now.strftime("%Y-%m-%d %H:%M:%S%z")

    # Import the send_gmail function from our Gmail blueprint
    from blueprints.gmail import send_gmail

    recipients = "Lance Johnson <lance@whiteboardgeeks.com>, Barbara Pigg <barbara.pigg@whiteboardgeeks.com>, Kori Watkins <kori.watkins@whiteboardgeeks.com>, Noura Mahmoud <noura.mahmoud@whiteboardgeeks.com>"

    # Override with any explicitly provided recipients
    recipients = kwargs.get("recipients", recipients)

    # Add environment information to the body
    environment_info = f"<p><strong>Environment:</strong> {env_type}</p>"
    html_body = environment_info + body

    # For text content, if it's provided separately
    text_content = kwargs.get("text_content", body)
    text_environment_info = f"Environment: {env_type}\n\n"
    text_content = text_environment_info + text_content

    # Send email using Gmail API
    gmail_response = send_gmail(
        sender="lance@whiteboardgeeks.com",
        to=recipients,
        subject=f"[MailerAutomation] [{env_type}] {subject} {time_now_formatted}",
        html_content=html_body,
        text_content=text_content,
    )

    return gmail_response


# Register blueprints after send_email is defined
flask_app.register_blueprint(instantly_bp, url_prefix="/instantly")
flask_app.register_blueprint(easypost_bp, url_prefix="/easypost")
flask_app.register_blueprint(gmail_bp, url_prefix="/gmail")

# Expose the send_email function to blueprints
flask_app.send_email = send_email


# /sync_delivery_status_from_easypost
@flask_app.route("/sync_delivery_status_from_easypost", methods=["GET"])
def sync_delivery_status_from_easypost():
    """
    Legacy endpoint for manually triggering a sync of delivery status from EasyPost.
    This endpoint queues a Celery task to run in the background.

    The task runs asynchronously and can be monitored using the
    /easypost/sync_delivery_status/status/<task_id> endpoint.
    """
    # Import the task here to avoid circular imports
    from blueprints.easypost import sync_delivery_status_task

    # Queue the task to run in the background
    task = sync_delivery_status_task.delay()

    # Return success response with task ID
    return jsonify(
        {
            "status": "success",
            "message": "Delivery status sync task has been queued. This endpoint is deprecated, please use /easypost/sync_delivery_status instead.",
            "task_id": task.id,
        }
    ), 200


# /delivery_status
def parse_delivery_information(tracking_data):
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


@flask_app.route("/delivery_status", methods=["POST"])
def handle_package_delivery_update():
    # This route has been moved to the easypost blueprint
    # Redirecting to the new endpoint for backward compatibility
    return jsonify(
        {
            "status": "redirect",
            "message": "This endpoint has been moved to /easypost/delivery_status",
        }
    ), 308  # 308 Permanent Redirect


# /prepare_contact_list_for_address_verification
def download_csv_as_list_of_dicts(csv_url):
    response = requests.get(csv_url)
    response.raise_for_status()  # Ensure the request was successful

    # Use StringIO to convert the text data into a file-like object so csv can read it
    csv_file = StringIO(response.text)

    # Read the CSV data
    reader = csv.DictReader(csv_file)

    # Convert the reader to a list of dictionaries
    list_of_dicts = list(reader)

    return list_of_dicts


def search_close_for_contact_by_email_or_phone(contact):
    contact_email = contact["Email"]
    contact_phone_number = contact[
        "Mobile Phone"
    ]  # Apollo gives the format '+1 888-888-8888. The ' at the beginning is weird, but seems to work with Close.
    # I need to find the lead instead of the contact because we set the Consultant field on the lead, not the contact
    # QUESTION FOR RICH: should I put these big jsons in a file and read them in?
    close_query_to_find_lead_by_email_or_phone = {
        "limit": None,
        "query": {
            "negate": False,
            "queries": [
                {"negate": False, "object_type": "lead", "type": "object_type"},
                {
                    "negate": False,
                    "queries": [
                        {
                            "negate": False,
                            "related_object_type": "contact",
                            "related_query": {
                                "negate": False,
                                "queries": [
                                    {
                                        "negate": False,
                                        "related_object_type": "contact_email",
                                        "related_query": {
                                            "negate": False,
                                            "queries": [
                                                {
                                                    "condition": {
                                                        "mode": "full_words",
                                                        "type": "text",
                                                        "value": contact_email,
                                                    },
                                                    "field": {
                                                        "field_name": "email",
                                                        "object_type": "contact_email",
                                                        "type": "regular_field",
                                                    },
                                                    "negate": False,
                                                    "type": "field_condition",
                                                }
                                            ],
                                            "type": "and",
                                        },
                                        "this_object_type": "contact",
                                        "type": "has_related",
                                    },
                                    {
                                        "negate": False,
                                        "related_object_type": "contact_phone",
                                        "related_query": {
                                            "negate": False,
                                            "queries": [
                                                {
                                                    "condition": {
                                                        "mode": "exact_value",
                                                        "type": "text",
                                                        "value": contact_phone_number,
                                                    },
                                                    "field": {
                                                        "field_name": "phone",
                                                        "object_type": "contact_phone",
                                                        "type": "regular_field",
                                                    },
                                                    "negate": False,
                                                    "type": "field_condition",
                                                }
                                            ],
                                            "type": "and",
                                        },
                                        "this_object_type": "contact",
                                        "type": "has_related",
                                    },
                                ],
                                "type": "or",
                            },
                            "this_object_type": "lead",
                            "type": "has_related",
                        }
                    ],
                    "type": "and",
                },
            ],
            "type": "and",
        },
        "results_limit": None,
        "sort": [],
    }
    response = make_close_request(
        "post",
        "https://api.close.com/api/v1/data/search",
        json=close_query_to_find_lead_by_email_or_phone,
    )
    resp_data = response.json()

    # Check if 'data' key is in response
    if response.status_code == 429:
        first_name = contact["First Name"]
        last_name = contact["Last Name"]
        company = contact["Company"]
        logger.error(
            f"Rate limit exceeded. Response: {resp_data} Contact: {first_name} {last_name} - {company}"
        )
        sleep(float(resp_data["error"]["rate_reset"]))
        return search_close_for_contact_by_email_or_phone(contact)
    if "data" not in resp_data:
        logger.error(
            f"No 'data' key in response. Response: {resp_data} Contact: {contact}"
        )
        return None

    leads_found = resp_data["data"]
    is_in_close = True if len(leads_found) > 0 else False
    contact["is_in_close"] = is_in_close
    return contact


def check_if_contacts_present_in_close(contacts):
    checked_contacts = []
    for contact in contacts:
        checked_contacts.append(search_close_for_contact_by_email_or_phone(contact))
    return checked_contacts


def check_if_contacts_have_email_and_mobile_phone(contacts):
    contacts_with_email_and_mobile_phone = [
        contact for contact in contacts if contact["Email"] and contact["Mobile Phone"]
    ]
    return contacts_with_email_and_mobile_phone


def filter_contacts_not_in_close(contacts_with_close_info):
    # Filter out contacts that are marked as present in Close
    return [
        contact for contact in contacts_with_close_info if not contact["is_in_close"]
    ]


def format_contacts_for_spreadsheet(contacts):
    formatted_contacts = []
    for contact in contacts:
        formatted_contact = {
            "First Name": contact.get("First Name", ""),
            "Last Name": contact.get("Last Name", ""),
            "Mobile Phone": f"'{contact.get('Mobile Phone', '')}",
            "Direct Phone": f"'{contact.get('Direct Phone', '')}",
            "Email Address": contact.get("Email", ""),
            "Company": contact.get("Company", ""),
            "Title": contact.get("Title", ""),
            "Contact LinkedIn URL": contact.get("Person Linkedin Url", ""),
        }
        formatted_contacts.append(formatted_contact)
    return formatted_contacts


def create_csv_from_contacts(contacts):
    csv_output = io.StringIO()
    writer = csv.DictWriter(csv_output, fieldnames=contacts[0].keys())
    writer.writeheader()
    writer.writerows(contacts)
    csv_output.seek(
        0
    )  # Rewind the StringIO object after writing to prepare for reading
    return csv_output.getvalue()  # Return CSV data as a string


def upload_to_bytescale(csv_data):
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"cleaned_{current_time}.csv"
    url = f"https://api.bytescale.com/v2/accounts/{BYTESCALE_ACCOUNT_ID}/uploads/binary"
    headers = {
        "Content-Type": "text/csv",
        "Authorization": f"Bearer {BYTESCALE_API_KEY}",
    }
    params = {"fileName": filename}
    response = requests.request(
        "POST", url, headers=headers, data=csv_data, params=params
    )
    file_url = response.json()["fileUrl"]
    return file_url


@celery.task(name="app.process_contact_list")
def process_contact_list(csv_url):
    # QUESTION FOR RICH: when you are going to loop over a list and perform a few operations do you 1. make a function that
    # takes a list, or 2. a for loop that goes over the list and performs the operations or 3. a function that takes a list
    # and then has sub-functions for each step in the loop?
    contact_list = download_csv_as_list_of_dicts(csv_url)
    contacts_with_email_and_mobile_phone = (
        check_if_contacts_have_email_and_mobile_phone(contact_list)
    )
    contacts_with_close_info = check_if_contacts_present_in_close(
        contacts_with_email_and_mobile_phone
    )
    contacts_not_in_close = filter_contacts_not_in_close(contacts_with_close_info)
    formatted_contacts = format_contacts_for_spreadsheet(contacts_not_in_close)
    csv_data = create_csv_from_contacts(formatted_contacts)
    bytescale_file_url = upload_to_bytescale(csv_data)

    requests.post(
        "https://hooks.zapier.com/hooks/catch/628188/3jtben9/",
        json={
            "file_url": bytescale_file_url,
            "time": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
        },
    )
    logger.info(f"File URL uploaded to Zapier: {bytescale_file_url}")


@flask_app.route("/prepare_contact_list_for_address_verification", methods=["POST"])
def prepare_contact_list_for_address_verification():
    api_key = request.headers.get("X-API-KEY")
    if api_key != WEBHOOK_API_KEY:
        return jsonify({"status": "error", "message": "Unauthorized access"}), 401
    data = request.json
    csv_url = data["webContentLink"]
    process_contact_list.delay(csv_url)
    return jsonify({"status": "success", "message": "Processing started"}), 202


@flask_app.route("/debug/env")
def debug_env():
    """Debug endpoint to check environment variables (only in development)."""
    if os.environ.get("ENV_TYPE", "").lower() not in ["production", "prod"]:
        gmail_info = os.environ.get("GMAIL_SERVICE_ACCOUNT_INFO", "Not found")
        gmail_file = os.environ.get("GMAIL_SERVICE_ACCOUNT_FILE", "Not found")

        # If GMAIL_SERVICE_ACCOUNT_INFO exists, just show first/last few chars
        if gmail_info != "Not found":
            info_len = len(gmail_info)
            gmail_info = (
                f"Found ({info_len} chars): {gmail_info[:20]}...{gmail_info[-20:]}"
            )

        return {
            "env_type": os.environ.get("ENV_TYPE", "Not set"),
            "gmail_service_account_info": gmail_info,
            "gmail_service_account_file": gmail_file,
            "flask_env": os.environ.get("FLASK_ENV", "Not set"),
            "flask_debug": os.environ.get("FLASK_DEBUG", "Not set"),
        }
    return {"error": "Debug endpoints not available in production"}, 403


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    if env_type == "development":
        flask_app.run(debug=True, host="0.0.0.0", port=port)
    else:
        flask_app.run(debug=False, host="0.0.0.0", port=port)
