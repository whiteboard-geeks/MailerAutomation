import os


def test_gmail_service_account_info():
    """
    Test that the GMAIL_SERVICE_ACCOUNT_INFO environment variable is set.
    This is required for the Gmail integration to work in the tests.
    """
    gmail_service_account_info = os.environ.get("GMAIL_SERVICE_ACCOUNT_INFO")
    assert gmail_service_account_info is not None, (
        "GMAIL_SERVICE_ACCOUNT_INFO environment variable is not set. "
        "This is required for Gmail functionality in tests. "
        "Please set this environment variable before running the tests."
    )
