"""
Unit tests for the Instantly webhook handler failure modes.
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from flask import Flask
from blueprints.instantly import instantly_bp, campaign_exists, get_lead_by_id


@pytest.fixture
def app():
    """Create a Flask test app with the Instantly blueprint registered."""
    app = Flask(__name__)
    app.register_blueprint(instantly_bp, url_prefix="/instantly")
    return app


@pytest.fixture
def client(app):
    """Create a test client for the app."""
    return app.test_client()


@pytest.fixture
def close_task_created_payload():
    """Return a sample Close webhook payload for a task creation."""
    return {
        "subscription_id": "whsub_7Yqhrb6zEZo1waN6medQzn",
        "event": {
            "id": "ev_4mp5KdF52CVItarzu1kkCi",
            "date_created": "2025-03-18T10:54:52.098000",
            "date_updated": "2025-03-18T10:54:52.098000",
            "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
            "user_id": None,
            "request_id": None,
            "api_key_id": None,
            "oauth_client_id": None,
            "oauth_scope": None,
            "object_type": "task.lead",
            "object_id": "task_07y7VvRV9HXrxDsDCMpZUOdkgKRsCRpmV7fVnSrAhaM",
            "lead_id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
            "action": "created",
            "changed_fields": [],
            "meta": {},
            "data": {
                "date": "2025-03-18",
                "id": "task_07y7VvRV9HXrxDsDCMpZUOdkgKRsCRpmV7fVnSrAhaM",
                "date_created": "2025-03-18T10:54:52.096000+00:00",
                "updated_by_name": None,
                "object_type": None,
                "is_primary_lead_notification": True,
                "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                "sequence_id": "seq_543h6u7YOdAZPJ74I49a0y",
                "lead_name": "Test - Noura M",
                "date_updated": "2025-03-18T10:54:52.096000+00:00",
                "view": None,
                "due_date": "2025-03-18",
                "is_dateless": False,
                "contact_id": None,
                "object_id": None,
                "is_complete": False,
                "created_by_name": "Barbara Pigg",
                "created_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                "assigned_to": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                "lead_id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
                "is_new": True,
                "sequence_subscription_id": "sub_38Qv1oCai2YqDuBYc5vpq4",
                "text": "Instantly: BP_BC_BlindInviteEmail1 [Noura Test]",
                "assigned_to_name": "Barbara Pigg",
                "updated_by": None,
                "_type": "lead",
                "deduplication_key": None,
            },
            "previous_data": {},
        },
    }


@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.send_email")
def test_nonexistent_campaign_returns_200(
    mock_send_email, mock_campaign_exists, client, close_task_created_payload
):
    """
    Test that when a campaign doesn't exist, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Setup mocks
    mock_campaign_exists.return_value = {"exists": False, "error": "Campaign not found"}

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response contains success status
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "does not exist" in response_data["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Campaign Not Found" in email_subject


@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.get_lead_by_id")
@patch("blueprints.instantly.send_email")
def test_lead_not_found_returns_200(
    mock_send_email,
    mock_get_lead,
    mock_campaign_exists,
    client,
    close_task_created_payload,
):
    """
    Test that when a lead can't be found, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Setup mocks
    mock_campaign_exists.return_value = {"exists": True, "campaign_id": "camp_123"}
    mock_get_lead.return_value = None

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response contains success status
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "Could not retrieve lead details" in response_data["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Lead Details Error" in email_subject


@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.get_lead_by_id")
@patch("blueprints.instantly.add_to_instantly_campaign")
@patch("blueprints.instantly.send_email")
def test_api_error_returns_200(
    mock_send_email,
    mock_add_to_campaign,
    mock_get_lead,
    mock_campaign_exists,
    client,
    close_task_created_payload,
):
    """
    Test that when the Instantly API returns an error, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Setup mocks
    mock_campaign_exists.return_value = {"exists": True, "campaign_id": "camp_123"}
    mock_get_lead.return_value = {
        "id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
        "name": "Test Lead",
        "contacts": [{"id": "cont_123", "emails": [{"email": "test@example.com"}]}],
    }
    mock_add_to_campaign.return_value = {
        "status": "error",
        "message": "API rate limit exceeded",
    }

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response contains success status
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "Failed to add lead to Instantly" in response_data["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Instantly API Error" in email_subject


@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.send_email")
def test_exception_returns_200(
    mock_send_email, mock_campaign_exists, client, close_task_created_payload
):
    """
    Test that when an unexpected exception occurs, the webhook handler:
    1. Returns a 200 status code
    2. Has a success status in the response
    3. Sends an email notification
    """
    # Setup mock to raise an exception
    mock_campaign_exists.side_effect = Exception("Unexpected error")

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response contains success status
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "An error occurred" in response_data["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Close Task Webhook Error" in email_subject
