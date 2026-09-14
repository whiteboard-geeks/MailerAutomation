import os
from typing import Any

import easypost
import structlog

from close_utils import make_close_request


# Configure logging using structlog
logger = structlog.get_logger("easypost")

EASYPOST_PROD_API_KEY = os.environ.get("EASYPOST_PROD_API_KEY")
EASYPOST_TEST_API_KEY = os.environ.get("EASYPOST_TEST_API_KEY")

# Close "Mailer Delivered" custom activity type, and the date_delivered field on it.
# The date field is what scopes duplicate detection to a single delivery so that a
# re-mailed (recycled) lead still gets a new activity.
MAILER_DELIVERED_ACTIVITY_TYPE_ID = "custom.actitype_3KhBfWgjtVfiGYbczbgOWv"
ACTIVITY_DATE_DELIVERED_FIELD = "custom.cf_wS7icPETKthDz764rkbuC1kQYzP0l88CzlMxoJAlOkO"


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


def create_package_delivered_custom_activity_in_close(lead_id, delivery_information) -> dict[str, Any]:
    """Create a custom activity in Close for delivered package."""
    # Only skip if an activity already exists for THIS delivery date. Recycled
    # leads carry a 'Mailer Delivered' activity from their previous mailing; a
    # lead-wide existence check would suppress every re-mail forever.
    if _check_existing_mailer_delivered_activities(
        lead_id, delivery_date=delivery_information.get("delivery_date")
    ):
        logger.info(
            f"Mailer delivered custom activity already exists for lead {lead_id} "
            f"on {delivery_information.get('delivery_date')}, skipping creation"
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


def _check_existing_mailer_delivered_activities(lead_id, delivery_date=None):
    """
    Check for existing 'Mailer Delivered' custom activities on a lead.

    Args:
        lead_id (str): The ID of the lead to check
        delivery_date (date | str | None): When provided, only an activity whose
            own date_delivered matches this date counts as a duplicate. This is
            what makes re-mailing a recycled lead work: the lead keeps the
            activity from its earlier mailing, and the new delivery still gets
            its own activity. When omitted, any existing activity counts
            (legacy lead-wide behaviour).

    Returns:
        bool: True if a matching activity was found, False otherwise
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

            if delivery_date is not None:
                wanted = (
                    delivery_date.isoformat()
                    if hasattr(delivery_date, "isoformat")
                    else str(delivery_date)
                )[:10]
                activities = [
                    a
                    for a in activities
                    if str(a.get(ACTIVITY_DATE_DELIVERED_FIELD) or "")[:10] == wanted
                ]

            has_existing_delivered_activities = len(activities) > 0

            if has_existing_delivered_activities:
                logger.info(
                    f"Found {len(activities)} existing mailer delivered activities for "
                    f"lead {lead_id}"
                    + (f" on {delivery_date}" if delivery_date is not None else "")
                )
            else:
                logger.info(
                    f"No existing mailer delivered activities found for lead {lead_id}"
                    + (f" on {delivery_date}" if delivery_date is not None else "")
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