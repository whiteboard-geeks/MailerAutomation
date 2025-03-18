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
    # Set default values for required environment variables
    if not os.environ.get("GMAIL_WEBHOOK_PASSWORD"):
        os.environ["GMAIL_WEBHOOK_PASSWORD"] = (
            "kShkgz6-6svDMWBefziRwENK1AeU4AT-K5qfJ4BEmp0"
        )

    if not os.environ.get("GMAIL_SERVICE_ACCOUNT_FILE"):
        os.environ["GMAIL_SERVICE_ACCOUNT_FILE"] = os.path.expanduser(
            "~/wbg-email-service-key.json"
        )

    if not os.environ.get("BASE_URL"):
        # Default to local development server
        os.environ["BASE_URL"] = "http://localhost:8080"

    if not os.environ.get("ENV_TYPE"):
        os.environ["ENV_TYPE"] = "test_automation"
