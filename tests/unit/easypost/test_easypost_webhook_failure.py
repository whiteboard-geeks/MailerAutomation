"""
Unit tests for the EasyPost webhook handler failure modes.
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from flask import Flask
from blueprints.easypost import easypost_bp


@pytest.fixture
def app():
    """Create a Flask test app with the EasyPost blueprint registered."""
    app = Flask(__name__)
    app.register_blueprint(easypost_bp, url_prefix="/easypost")
    return app


@pytest.fixture
def client(app):
    """Create a test client for the app."""
    return app.test_client()


@pytest.fixture
def close_webhook_payload():
    """Return a sample Close webhook payload for tracking number and carrier updates."""
    return {
        "event": {
            "data": {
                "id": "lead_123456",
                "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": "1Z999AA10123456789",
                "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": "UPS",
            }
        }
    }


@patch("blueprints.easypost.send_email")
def test_no_lead_id_returns_200(mock_send_email, client):
    """
    Test that when no lead ID is provided, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Create payload with missing lead ID
    payload = {"event": {"data": {}}}

    # Send the webhook payload
    response = client.post(
        "/easypost/create_tracker", json=payload, content_type="application/json"
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response contains success status
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "No lead_id provided" in response_data["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "EasyPost Tracker Creation Error" in email_subject


@patch("blueprints.easypost.get_close_encoded_key")
@patch("blueprints.easypost.send_email")
def test_lead_not_found_returns_200(
    mock_send_email, mock_get_key, client, close_webhook_payload
):
    """
    Test that when a lead can't be fetched, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Mock the Close API response for a non-existent lead
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Lead not found"
        mock_get.return_value = mock_response

        # Send the webhook payload
        response = client.post(
            "/easypost/create_tracker",
            json=close_webhook_payload,
            content_type="application/json",
        )

        # Check response status code is 200
        assert response.status_code == 200

        # Check response contains success status
        response_data = json.loads(response.data)
        assert response_data["status"] == "success"
        assert "Failed to fetch lead data" in response_data["message"]

        # Verify email notification was sent
        mock_send_email.assert_called_once()
        email_subject = mock_send_email.call_args[1]["subject"]
        assert "Close Lead Data Fetch Error" in email_subject


@patch("blueprints.easypost.get_close_encoded_key")
@patch("blueprints.easypost.send_email")
def test_missing_tracking_info_returns_200(
    mock_send_email, mock_get_key, client, close_webhook_payload
):
    """
    Test that when tracking number or carrier is missing, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Mock the Close API response with missing tracking info
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "lead_123456",
            "name": "Test Lead",
            # Missing tracking number and carrier
        }
        mock_get.return_value = mock_response

        # Send the webhook payload
        response = client.post(
            "/easypost/create_tracker",
            json=close_webhook_payload,
            content_type="application/json",
        )

        # Check response status code is 200
        assert response.status_code == 200

        # Check response contains success status
        response_data = json.loads(response.data)
        assert response_data["status"] == "success"
        assert (
            "Lead doesn't have tracking number or carrier" in response_data["message"]
        )

        # Verify email notification was sent
        mock_send_email.assert_called_once()
        email_subject = mock_send_email.call_args[1]["subject"]
        assert "EasyPost Tracker Missing Data" in email_subject


@patch("blueprints.easypost.get_close_encoded_key")
@patch("blueprints.easypost.get_easypost_client")
@patch("blueprints.easypost.send_email")
def test_easypost_api_error_returns_200(
    mock_send_email, mock_get_client, mock_get_key, client, close_webhook_payload
):
    """
    Test that when the EasyPost API fails, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Mock the Close API response with valid tracking info
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "lead_123456",
            "name": "Test Lead",
            "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii": "1Z999AA10123456789",
            "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l": "UPS",
        }
        mock_get.return_value = mock_response

        # Mock EasyPost client to raise an exception
        mock_client = MagicMock()
        mock_client.tracker.create.side_effect = Exception("API rate limit exceeded")
        mock_get_client.return_value = mock_client

        # Send the webhook payload
        response = client.post(
            "/easypost/create_tracker",
            json=close_webhook_payload,
            content_type="application/json",
        )

        # Check response status code is 200
        assert response.status_code == 200

        # Check response contains success status
        response_data = json.loads(response.data)
        assert response_data["status"] == "success"
        assert "Error creating EasyPost tracker" in response_data["message"]

        # Verify email notification was sent
        mock_send_email.assert_called_once()
        email_subject = mock_send_email.call_args[1]["subject"]
        assert "EasyPost Tracker Creation Error" in email_subject
