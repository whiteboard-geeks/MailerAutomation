"""
Integration test for the EasyPost webhook failure modes.
This test sends actual emails so you can verify the failure notifications.

To run just this test:
pytest tests/integration/easypost/test_webhook_failure_integration.py -v

Note: This requires a working email configuration in your environment.
"""

import json
import pytest
import time
from unittest.mock import patch, MagicMock
from app import flask_app

# Sample Close webhook payload for EasyPost tracker creation
SAMPLE_PAYLOAD = {
    "event": {
        "data": {
            "id": "lead_123456",
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": "1Z999AA10123456789",
            "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": "UPS",
        }
    }
}


@pytest.fixture
def client():
    """Create a test client with the actual Flask app."""
    # Ensure testing mode
    flask_app.config["TESTING"] = True
    # We want to use the actual email functionality
    flask_app.config["MAIL_SUPPRESS_SEND"] = False
    return flask_app.test_client()


@pytest.mark.webhook_failures
def test_no_lead_id_sends_real_email(client):
    """
    Integration test for missing lead ID error.
    This will actually send a real email notification.
    """
    print("\n--- Testing missing lead ID (real email will be sent) ---")
    print("Email will be sent to the hardcoded recipient in the send_email function")

    # Create a payload with missing lead ID
    invalid_payload = {
        "event": {
            "data": {}  # Missing lead ID
        }
    }

    # Send the webhook payload
    start_time = time.time()
    response = client.post(
        "/easypost/create_tracker",
        json=invalid_payload,
        content_type="application/json",
    )
    elapsed = time.time() - start_time

    # Print response details
    print(f"\nResponse received in {elapsed:.2f} seconds")
    print(f"Status code: {response.status_code}")
    response_data = response.json
    print(f"Response body: {json.dumps(response_data, indent=2)}")

    # Assert the response has the expected format
    assert (
        response.status_code == 200
    ), f"Expected status code 200, got {response.status_code}"
    assert (
        response_data.get("status") == "success"
    ), "Response status should be 'success'"
    assert "No lead_id provided" in response_data.get(
        "message", ""
    ), "Message should mention missing lead ID"

    print("\n✅ Test passed - Webhook returned 200 with 'success' status")
    print("Check your email for the notification about missing lead ID.")

    # Print verification prompt
    print("\nVerify that:")
    print("1. You received an email with subject 'EasyPost Tracker Creation Error'")
    print("2. The email contains error details about missing lead ID")
    print("3. The JSON response has status 'success' despite the error")


@pytest.mark.webhook_failures
def test_lead_not_found_sends_real_email(client):
    """
    Integration test for lead not found error.
    This will actually send a real email notification.
    """
    print("\n--- Testing lead not found (real email will be sent) ---")
    print("Email will be sent to the hardcoded recipient in the send_email function")

    # Mock requests.get to simulate lead not found in Close
    with patch("requests.get") as mock_get:
        # Configure the mock to return a 404 status code
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_get.return_value = mock_response

        # Send the webhook payload
        start_time = time.time()
        response = client.post(
            "/easypost/create_tracker",
            json=SAMPLE_PAYLOAD,
            content_type="application/json",
        )
        elapsed = time.time() - start_time

        # Print response details
        print(f"\nResponse received in {elapsed:.2f} seconds")
        print(f"Status code: {response.status_code}")
        response_data = response.json
        print(f"Response body: {json.dumps(response_data, indent=2)}")

        # Assert the response has the expected format
        assert (
            response.status_code == 200
        ), f"Expected status code 200, got {response.status_code}"
        assert (
            response_data.get("status") == "success"
        ), "Response status should be 'success'"
        assert "Failed to fetch lead data" in response_data.get(
            "message", ""
        ), "Message should mention lead not found"

        print("\n✅ Test passed - Webhook returned 200 with 'success' status")
        print("Check your email for the notification about lead not found.")

        # Print verification prompt
        print("\nVerify that:")
        print("1. You received an email with subject 'Close Lead Data Fetch Error'")
        print("2. The email contains error details about the failed fetch")
        print("3. The JSON response has status 'success' despite the error")


@pytest.mark.webhook_failures
def test_missing_tracking_info_sends_real_email(client):
    """
    Integration test for missing tracking number or carrier.
    This will actually send a real email notification.
    """
    print("\n--- Testing missing tracking info (real email will be sent) ---")
    print("Email will be sent to the hardcoded recipient in the send_email function")

    # Mock requests.get to simulate lead with missing tracking info
    with patch("requests.get") as mock_get:
        # Configure the mock to return a lead without tracking number or carrier
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "lead_123456",
            "name": "Test Lead",
            # No tracking info fields
        }
        mock_get.return_value = mock_response

        # Send the webhook payload
        start_time = time.time()
        response = client.post(
            "/easypost/create_tracker",
            json=SAMPLE_PAYLOAD,
            content_type="application/json",
        )
        elapsed = time.time() - start_time

        # Print response details
        print(f"\nResponse received in {elapsed:.2f} seconds")
        print(f"Status code: {response.status_code}")
        response_data = response.json
        print(f"Response body: {json.dumps(response_data, indent=2)}")

        # Assert the response has the expected format
        assert (
            response.status_code == 200
        ), f"Expected status code 200, got {response.status_code}"
        assert (
            response_data.get("status") == "success"
        ), "Response status should be 'success'"
        assert "doesn't have tracking number or carrier" in response_data.get(
            "message", ""
        ), "Message should mention missing tracking info"

        print("\n✅ Test passed - Webhook returned 200 with 'success' status")
        print("Check your email for the notification about missing tracking info.")

        # Print verification prompt
        print("\nVerify that:")
        print("1. You received an email with subject 'EasyPost Tracker Missing Data'")
        print("2. The email contains error details about missing tracking info")
        print("3. The JSON response has status 'success' despite the error")


@pytest.mark.webhook_failures
def test_easypost_api_error_sends_real_email(client):
    """
    Integration test for EasyPost API error.
    This will actually send a real email notification.
    """
    print("\n--- Testing EasyPost API error (real email will be sent) ---")
    print("Email will be sent to the hardcoded recipient in the send_email function")

    # Mock sequence for a lead with tracking info but EasyPost API error
    with patch("requests.get") as mock_get:
        with patch("blueprints.easypost.get_easypost_client") as mock_get_client:
            # Configure the mock to return a lead with tracking info
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "id": "lead_123456",
                "name": "Test Lead",
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": "1Z999AA10123456789",
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": "UPS",
            }
            mock_get.return_value = mock_response

            # Set up the EasyPost client mock to raise an exception
            mock_client = MagicMock()
            mock_client.tracker.create.side_effect = Exception(
                "EasyPost API rate limit exceeded"
            )
            mock_get_client.return_value = mock_client

            # Send the webhook payload
            start_time = time.time()
            response = client.post(
                "/easypost/create_tracker",
                json=SAMPLE_PAYLOAD,
                content_type="application/json",
            )
            elapsed = time.time() - start_time

            # Print response details
            print(f"\nResponse received in {elapsed:.2f} seconds")
            print(f"Status code: {response.status_code}")
            response_data = response.json
            print(f"Response body: {json.dumps(response_data, indent=2)}")

            # Assert the response has the expected format
            assert (
                response.status_code == 200
            ), f"Expected status code 200, got {response.status_code}"
            assert (
                response_data.get("status") == "success"
            ), "Response status should be 'success'"
            assert "Error creating EasyPost tracker" in response_data.get(
                "message", ""
            ), "Message should mention API error"

            print("\n✅ Test passed - Webhook returned 200 with 'success' status")
            print("Check your email for the notification about EasyPost API error.")

            # Print verification prompt
            print("\nVerify that:")
            print(
                "1. You received an email with subject 'EasyPost Tracker Creation Error'"
            )
            print("2. The email contains error details about the API error")
            print("3. The JSON response has status 'success' despite the error")


if __name__ == "__main__":
    # This allows running the test directly if needed
    pytest.main(["-xvs", __file__])
