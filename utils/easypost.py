import os

import easypost
import structlog


# Configure logging using structlog
logger = structlog.get_logger("easypost")

EASYPOST_PROD_API_KEY = os.environ.get("EASYPOST_PROD_API_KEY")
EASYPOST_TEST_API_KEY = os.environ.get("EASYPOST_TEST_API_KEY")


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
