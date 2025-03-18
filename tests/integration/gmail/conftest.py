"""
Pytest configuration for Gmail integration tests.
"""

import os
import pytest
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Set default values for necessary environment variables
@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables if not already set."""
    # Check required credentials are available
    if not os.environ.get("GMAIL_WEBHOOK_PASSWORD"):
        pytest.skip("GMAIL_WEBHOOK_PASSWORD not set in environment variables")

    if not os.environ.get("GMAIL_SERVICE_ACCOUNT_FILE"):
        # Use a default path but don't create the file
        os.environ["GMAIL_SERVICE_ACCOUNT_FILE"] = os.path.expanduser(
            "~/wbg-email-service-key.json"
        )
        # Skip if the file doesn't exist
        if not os.path.exists(os.environ["GMAIL_SERVICE_ACCOUNT_FILE"]):
            pytest.skip("Gmail service account key file not found")

    if not os.environ.get("BASE_URL"):
        # Default to local development server
        os.environ["BASE_URL"] = "http://localhost:8080"

    if not os.environ.get("ENV_TYPE"):
        os.environ["ENV_TYPE"] = "test_automation"
