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

# Configure logging
logger = logging.getLogger(__name__)

# Get API key from environment
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = b64encode(f"{CLOSE_API_KEY}:".encode()).decode()


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


@retry_with_backoff(max_retries=3, initial_delay=1)
def make_close_request(method, url, **kwargs):
    """
    Make a request to the Close API with retry logic.

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


@retry_with_backoff(max_retries=3, initial_delay=1)
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


@retry_with_backoff(max_retries=3, initial_delay=1)
def get_lead_by_id(lead_id):
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


@retry_with_backoff(max_retries=3, initial_delay=1)
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


@retry_with_backoff(max_retries=3, initial_delay=1)
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
