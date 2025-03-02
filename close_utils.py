"""
Utility functions for interacting with Close CRM.
"""

import logging
import os
import traceback
from base64 import b64encode
from time import sleep

import requests

# Configure logging
logger = logging.getLogger(__name__)

# Get API key from environment
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
CLOSE_ENCODED_KEY = b64encode(f"{CLOSE_API_KEY}:".encode()).decode()


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


def search_close_leads(query):
    """
    Search for leads in Close using a query.

    Args:
        query (dict): The Close query to execute.

    Returns:
        list: A list of leads matching the query, or empty list if none found or error occurs.
    """
    try:
        headers = get_close_headers()

        data_to_return = []
        cursor = None
        retry_count = 0
        max_retries = 3

        while True:
            if cursor:
                query["cursor"] = cursor

            # Add retry logic with exponential backoff
            try:
                response = requests.post(
                    "https://api.close.com/api/v1/data/search/",
                    json=query,
                    headers=headers,
                    timeout=30,  # Add timeout
                )
                response.raise_for_status()  # Raise exception for non-200 status codes

            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"Max retries exceeded when querying Close API: {e}")
                    raise

                sleep_time = 2**retry_count  # Exponential backoff
                logger.warning(
                    f"Retrying Close API request in {sleep_time} seconds. Attempt {retry_count} of {max_retries}"
                )
                sleep(sleep_time)
                continue

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
        headers = get_close_headers()

        url = f"https://api.close.com/api/v1/lead/{lead_id}/"

        retry_count = 0
        max_retries = 3

        while retry_count <= max_retries:
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=30,  # Add timeout
                )

                if response.status_code == 404:
                    logger.warning(f"Lead with ID {lead_id} not found")
                    return None

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(
                        f"Max retries exceeded when getting lead {lead_id}: {e}"
                    )
                    break

                sleep_time = 2**retry_count  # Exponential backoff
                logger.warning(
                    f"Retrying Close API request in {sleep_time} seconds. Attempt {retry_count} of {max_retries}"
                )
                sleep(sleep_time)

        return None

    except Exception as e:
        logger.error(f"Failed to get lead {lead_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None
