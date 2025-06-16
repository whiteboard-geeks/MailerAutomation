"""
Unit tests for the Instantly webhook handler with async processing.
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from flask import Flask
from blueprints.instantly import instantly_bp


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


@patch("blueprints.instantly.process_lead_batch_task.delay")
def test_valid_instantly_task_queues_async_processing(
    mock_celery_delay, client, close_task_created_payload
):
    """
    Test that a valid Instantly task is queued for async processing.

    This test verifies the new async behavior where the webhook endpoint:
    1. Returns 202 Accepted status code
    2. Queues a Celery task for background processing
    3. Returns success response with task details
    """
    # Setup mock Celery task
    mock_task = MagicMock()
    mock_task.id = "test-celery-task-id"
    mock_celery_delay.return_value = mock_task

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 202 (Accepted)
    assert response.status_code == 202

    # Check response contains success status and async details
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "queued for Instantly campaign" in response_data["message"]
    assert response_data["processing_type"] == "async"
    assert response_data["celery_task_id"] == "test-celery-task-id"
    assert response_data["campaign_name"] == "BP_BC_BlindInviteEmail1"
    assert (
        response_data["lead_id"] == "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc"
    )

    # Verify Celery task was queued with correct payload
    mock_celery_delay.assert_called_once_with(close_task_created_payload)


@patch("blueprints.instantly.process_lead_batch_task.delay")
def test_non_task_creation_event_returns_200(
    mock_celery_delay, client, close_task_created_payload
):
    """
    Test that non-task-creation events are handled correctly without queueing async processing.
    """
    # Modify payload to be a different event type
    close_task_created_payload["event"]["action"] = "updated"

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response message
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "Not a task creation event" in response_data["message"]

    # Verify Celery task was NOT queued
    mock_celery_delay.assert_not_called()


@patch("blueprints.instantly.process_lead_batch_task.delay")
def test_non_instantly_task_returns_200(
    mock_celery_delay, client, close_task_created_payload
):
    """
    Test that tasks not starting with 'Instantly' are handled correctly without queueing.
    """
    # Modify task text to not start with "Instantly"
    close_task_created_payload["event"]["data"]["text"] = "Regular task: Do something"

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response message
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "Not an Instantly task" in response_data["message"]

    # Verify Celery task was NOT queued
    mock_celery_delay.assert_not_called()


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.process_lead_batch_task.delay")
def test_no_campaign_name_sends_email_and_returns_200(
    mock_celery_delay, mock_send_email, client, close_task_created_payload
):
    """
    Test that when campaign name cannot be extracted, an email is sent and 200 is returned.
    """
    # Modify task text to not have a extractable campaign name
    close_task_created_payload["event"]["data"]["text"] = "InstantlyNoSeparator"

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response message
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "No campaign name found" in response_data["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Instantly Campaign Name Error" in email_subject

    # Verify Celery task was NOT queued
    mock_celery_delay.assert_not_called()


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.process_lead_batch_task.delay")
def test_exception_in_webhook_sends_email_and_returns_200(
    mock_celery_delay, mock_send_email, client, close_task_created_payload
):
    """
    Test that when an unexpected exception occurs in the webhook handler,
    an email is sent and 200 is returned.
    """
    # Make process_lead_batch_task.delay raise an exception
    mock_celery_delay.side_effect = Exception("Celery connection error")

    # Send the webhook payload
    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    # Check response status code is 200
    assert response.status_code == 200

    # Check response contains error handling
    response_data = json.loads(response.data)
    assert response_data["status"] == "success"
    assert "An error occurred" in response_data["message"]
    assert "error" in response_data
    assert "Celery connection error" in response_data["error"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Close Task Webhook Error" in email_subject

    # Verify Celery task was attempted to be queued
    mock_celery_delay.assert_called_once()
