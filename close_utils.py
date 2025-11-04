"""
Utility functions for interacting with Close CRM.
"""

import logging
import os
import traceback
from base64 import b64encode
from time import sleep
import json
import functools

import requests
from utils.rate_limiter import CloseRateLimiter

# Configure logging
logger = logging.getLogger(__name__)

# Get API key from environment
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = b64encode(f"{CLOSE_API_KEY}:".encode()).decode()

# Initialize global Close rate limiter
_close_rate_limiter = None


def get_close_rate_limiter():
    """
    Get or create the global Close rate limiter instance.

    Returns:
        CloseRateLimiter: Global rate limiter instance
    """
    global _close_rate_limiter
    if _close_rate_limiter is None:
        try:
            import redis
            import os

            # Try to connect to Redis
            redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379/0")
            redis_client = redis.from_url(redis_url)
            redis_client.ping()  # Test connection

            _close_rate_limiter = CloseRateLimiter(
                redis_client=redis_client,
                conservative_default_rps=1.0,  # Conservative 1 req/sec for unknown endpoints
                safety_factor=0.8,  # 80% safety margin
                cache_expiration_seconds=3600,  # 1 hour cache
            )
            logger.info("Close rate limiter initialized with Redis")

        except Exception as e:
            logger.warning(f"Failed to initialize Redis for Close rate limiter: {e}")
            # Fallback to in-memory rate limiter
            _close_rate_limiter = CloseRateLimiter(
                redis_client=None,
                conservative_default_rps=1.0,
                safety_factor=0.8,
                fallback_on_redis_error=True,
            )
            logger.info("Close rate limiter initialized with in-memory fallback")

    return _close_rate_limiter


def close_rate_limit(max_retries=3, initial_delay=1):
    """
    Decorator that adds Close-specific rate limiting and retry logic to a function.

    This decorator:
    1. Applies endpoint-specific rate limiting before making requests
    2. Parses rate limit headers from responses to learn actual limits
    3. Provides retry logic with exponential backoff for non-rate-limit errors
    4. Maintains backward compatibility with existing retry_with_backoff behavior

    Args:
        max_retries (int): Maximum number of retry attempts
        initial_delay (int): Initial delay in seconds before first retry

    Returns:
        function: Decorated function with rate limiting and retry logic
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get the URL from function arguments
            url = None
            if len(args) >= 2:
                url = args[1]  # Second argument is typically the URL
            elif "url" in kwargs:
                url = kwargs["url"]

            rate_limiter = get_close_rate_limiter()
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    # Apply rate limiting before making the request
                    if url and url.startswith("https://api.close.com"):
                        if not rate_limiter.acquire_token_for_endpoint(url):
                            logger.warning(f"Rate limited for endpoint: {url}")
                            # Wait a bit and try again (this counts as an attempt)
                            if attempt < max_retries:
                                sleep(delay)
                                delay *= 2
                                continue
                            else:
                                raise requests.exceptions.RequestException(
                                    "Rate limit exceeded after retries"
                                )

                    # Make the actual request
                    response = func(*args, **kwargs)

                    # Parse rate limit headers from response to learn actual limits
                    if (
                        url
                        and url.startswith("https://api.close.com")
                        and hasattr(response, "headers")
                    ):
                        rate_limiter.update_from_response_headers(url, response)

                    return response

                except requests.exceptions.RequestException as e:
                    last_exception = e

                    # Don't retry on 4xx errors (except 429 rate limit)
                    if hasattr(e, "response") and e.response is not None:
                        status_code = e.response.status_code
                        if 400 <= status_code < 500 and status_code != 429:
                            logger.error(
                                f"Client error {status_code} for {func.__name__}: {str(e)}"
                            )
                            raise last_exception

                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}: {str(e)}"
                        )
                        raise last_exception

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}. "
                        f"Retrying in {delay} seconds. Error: {str(e)}"
                    )
                    sleep(delay)
                    delay *= 2  # Exponential backoff

            raise last_exception

        return wrapper

    return decorator


def load_query(file_name):
    """
    Load a Close query from a JSON file in the close_queries directory.

    Args:
        file_name (str): Name of the JSON file to load

    Returns:
        dict: The loaded query as a dictionary
    """
    # Construct the full path to the file
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "close_queries", file_name)

    # Open and load the JSON data
    with open(file_path, "r") as file:
        return json.load(file)


def retry_with_backoff(max_retries=3, initial_delay=1):
    """
    Decorator that adds retry logic with exponential backoff to a function.

    Args:
        max_retries (int): Maximum number of retry attempts
        initial_delay (int): Initial delay in seconds before first retry

    Returns:
        function: Decorated function with retry logic
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}: {str(e)}"
                        )
                        raise last_exception

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}. "
                        f"Retrying in {delay} seconds. Error: {str(e)}"
                    )
                    sleep(delay)
                    delay *= 2  # Exponential backoff

            raise last_exception

        return wrapper

    return decorator


def get_close_headers():
    """
    Returns headers needed for Close API requests.

    Returns:
        dict: Headers with Content-Type and Authorization.
    """
    return {
        "Content-Type": "application/json",
        "Authorization": f"Basic {CLOSE_ENCODED_KEY}",
    }


@close_rate_limit(max_retries=3, initial_delay=1)
def make_close_request(method, url, **kwargs):
    """
    Make a request to the Close API with dynamic rate limiting and retry logic.

    This function now includes:
    - Endpoint-specific rate limiting before requests
    - Dynamic limit discovery from response headers
    - Retry logic with exponential backoff
    - Backward compatibility with existing functionality

    Args:
        method (str): HTTP method (get, post, put, delete)
        url (str): URL to make the request to
        **kwargs: Additional arguments to pass to requests

    Returns:
        requests.Response: The response from the Close API
    """
    headers = get_close_headers()
    if "headers" in kwargs:
        headers.update(kwargs["headers"])
    kwargs["headers"] = headers

    response = requests.request(method, url, **kwargs)
    response.raise_for_status()
    return response


def create_email_search_query(email):
    """
    Create a Close API query to find leads with a contact that has the given email.

    Args:
        email (str): The email address to search for.

    Returns:
        dict: The Close API query.
    """
    # Load the query template
    query_path = os.path.join(
        os.path.dirname(__file__),
        "close_queries",
        "leads_with_contact_with_email.json",
    )
    with open(query_path, "r") as f:
        query_template = json.load(f)

    # Replace the email value in the query
    query_template["query"]["queries"][1]["queries"][0]["related_query"]["queries"][0][
        "related_query"
    ]["queries"][0]["condition"]["value"] = email

    return query_template


def search_close_leads(query):
    """
    Search for leads in Close using a query.

    Args:
        query (dict): The Close query to execute.

    Returns:
        list: A list of leads matching the query, or empty list if none found or error occurs.
    """
    try:
        data_to_return = []
        cursor = None

        while True:
            if cursor:
                query["cursor"] = cursor

            response = make_close_request(
                "post",
                "https://api.close.com/api/v1/data/search/",
                json=query,
                timeout=30,
            )
            response_data = response.json()

            # Log response data for debugging
            logger.debug(f"Close API Response: {response_data}")

            if "data" not in response_data:
                logger.error(
                    f"Unexpected response format from Close API: {response_data}"
                )
                raise Exception("Invalid response format from Close API")

            number_of_leads_retrieved = len(response_data["data"])
            logger.info(
                f"Number of leads retrieved: {number_of_leads_retrieved}, "
                f"Current cursor: {cursor}"
            )

            data_to_return.extend(response_data["data"])

            # Get next cursor
            cursor = response_data.get("cursor")
            if not cursor:
                logger.info("No more pages to fetch from Close API.")
                break

        if not data_to_return:
            logger.warning("No leads found in Close API search")
            return []  # Return empty list instead of None

        return data_to_return

    except Exception as e:
        logger.error(f"Failed to search Close leads: {e}")
        logger.error(f"Query used: {query}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []  # Return empty list instead of None


def get_lead_by_id(lead_id) -> dict | None:
    """
    Get a lead by its ID from Close.

    Note: The lead data includes contact information, so there's no need
    for a separate call to retrieve contacts.

    Args:
        lead_id (str): The ID of the lead to retrieve.

    Returns:
        dict: The lead data or None if not found or error occurs.
    """
    try:
        url = f"https://api.close.com/api/v1/lead/{lead_id}/"
        response = make_close_request("get", url, timeout=30)

        if response.status_code == 404:
            logger.warning(f"Lead with ID {lead_id} not found")
            return None

        return response.json()

    except Exception as e:
        logger.error(f"Failed to get lead {lead_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def get_lead_email_activities(lead_id):
    """
    Get all email activities for a lead.

    Args:
        lead_id (str): The ID of the lead to get email activities for.

    Returns:
        list: A list of email activities, or empty list if none found or error occurs.
    """
    try:
        url = f"https://api.close.com/api/v1/activity/email/?lead_id={lead_id}"
        response = make_close_request("get", url)
        data = response.json()
        return data.get("data", [])

    except Exception as e:
        logger.error(f"Failed to get email activities for lead {lead_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []


def get_task(task_id):
    """
    Get a task by its ID from Close.

    Args:
        task_id (str): The ID of the task to retrieve.

    Returns:
        dict: The task data or None if not found or error occurs.
    """
    try:
        url = f"https://api.close.com/api/v1/task/{task_id}/"
        response = make_close_request("get", url)
        return response.json()

    except Exception as e:
        logger.error(f"Failed to get task {task_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def create_task(lead_id, text, assigned_to=None, date=None, is_complete=False):
    """
    Create a task in Close CRM.

    Args:
        lead_id (str): The ID of the lead to associate the task with
        text (str): The task text/content
        assigned_to (str, optional): User ID of the assignee. If None, will be assigned to the API user.
        date (str, optional): Due date in ISO format (YYYY-MM-DD). If None, set to today.
        is_complete (bool, optional): Whether the task is already complete. Default is False.

    Returns:
        dict: The created task data or None if an error occurred
    """
    try:
        url = "https://api.close.com/api/v1/task/"

        # Prepare task data
        task_data = {
            "_type": "lead",
            "lead_id": lead_id,
            "text": text,
            "is_complete": is_complete,
        }

        # Add optional fields if provided
        if assigned_to:
            task_data["assigned_to"] = assigned_to

        if date:
            task_data["date"] = date

        # Make the request
        response = make_close_request("post", url, json=task_data, timeout=30)
        return response.json()

    except Exception as e:
        logger.error(f"Failed to create task for lead {lead_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def get_sequence_subscriptions(lead_id=None, contact_id=None, sequence_id=None):
    """
    Get sequence subscriptions for a lead or contact.

    At least one of the parameters must be provided.

    Args:
        lead_id (str, optional): The ID of the lead to get subscriptions for
        contact_id (str, optional): The ID of the contact to get subscriptions for
        sequence_id (str, optional): The ID of the sequence to filter by

    Returns:
        list: A list of sequence subscriptions or empty list if none found
    """
    try:
        # Build params - at least one must be provided
        params = {}
        if lead_id:
            params["lead_id"] = lead_id
        if contact_id:
            params["contact_id"] = contact_id
        if sequence_id:
            params["sequence_id"] = sequence_id

        if not params:
            logger.error(
                "At least one of lead_id, contact_id, or sequence_id must be provided"
            )
            return []

        url = "https://api.close.com/api/v1/sequence_subscription/"
        response = make_close_request("get", url, params=params)
        data = response.json()
        return data.get("data", [])

    except Exception as e:
        logger.error(f"Failed to get sequence subscriptions: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []


def pause_sequence_subscription(subscription_id, status_reason="replied"):
    """
    Pause a sequence subscription.

    Args:
        subscription_id (str): The ID of the subscription to pause
        status_reason (str, optional): Reason for pausing. Default is "replied"

    Returns:
        dict: The updated subscription data or None if an error occurred
    """
    try:
        url = f"https://api.close.com/api/v1/sequence_subscription/{subscription_id}/"

        # Prepare the payload
        payload = {"status": "paused", "status_reason": status_reason}

        response = make_close_request("put", url, json=payload)
        return response.json()

    except Exception as e:
        logger.error(f"Failed to pause sequence subscription {subscription_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def update_delivery_information_for_lead(lead_id, delivery_information) -> None:
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
        raise Exception("Close did not accept the lead update.")
    response_data = response.json()
    data_updated = verify_delivery_information_updated(response_data, lead_update_data)
    if not data_updated:
        raise Exception("Close accepted the lead, but the fields did not update.")
