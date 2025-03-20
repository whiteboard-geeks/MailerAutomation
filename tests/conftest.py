"""
Pytest configuration file.
This file is automatically loaded by pytest.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Get the project root directory
project_root = Path(__file__).parent.parent

# Load environment variables from .env file (if it exists)
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file)
    print(f"Loaded environment variables from: {env_file}")
else:
    print("No .env file found, using environment variables from the system")


def pytest_configure(config):
    """
    Called before pytest collects tests so environment variables
    are properly set for all tests.
    """
    # Verify critical environment variables
    for var in ["GMAIL_SERVICE_ACCOUNT_INFO"]:
        if var in os.environ:
            print(f"✅ Found {var} in environment")
        else:
            print(f"❌ Missing {var} in environment")
