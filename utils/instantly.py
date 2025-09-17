import json
import os
import re
from pydantic import BaseModel
import structlog
import requests
import time

from utils.redis import get_from_cache, set_to_cache
from utils.rate_limiter import RedisRateLimiter, APIRateConfig

# Configure logging using structlog
logger = structlog.get_logger("instantly")

# Global rate limiter instance
_rate_limiter = None


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


INSTANTLY_API_KEY = os.environ.get("INSTANTLY_API_KEY")


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


class Campaign(BaseModel):
    id: str
    name: str



def search_campaigns_by_lead_email(email: str) -> list[Campaign]:
    """
    Search for campaigns by lead email in Instantly.

    Args:
        email (str): The email address to search for

    Returns:
        list[Campaign]: A list of campaigns the lead is in
    """
    if not INSTANTLY_API_KEY:
        raise Exception("Instantly API key is not configured")

    url = "https://api.instantly.ai/api/v2/campaigns/search-by-contact"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
    }

    params = {
        "search": email,
        "sort_column": "timestamp_created",
        "sort_order": "asc",
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return [Campaign(**item) for item in data.get("items", [])]
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching campaigns from Instantly: {str(e)}")
