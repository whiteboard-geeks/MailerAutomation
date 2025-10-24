"""
Unit tests for the EasyPost webhook handler failure modes.
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock
from blueprints import easypost as easypost_module
from flask import Flask
from blueprints.easypost import easypost_bp


# Shared skip condition for tests that should be skipped when Temporal is enabled
skip_when_temporal_enabled = pytest.mark.skipif(
    os.getenv("USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER", "").lower() in ("true", "1", "yes", "on"),
    reason="Test skipped when USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER is enabled"
)


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


@pytest.fixture(autouse=True)
def disable_webhook_tracker_redis(monkeypatch):
    """Prevent tests from attempting to write to an actual Redis instance."""
    tracker = easypost_module._webhook_tracker
    monkeypatch.setattr(tracker, "redis", None)
    if not hasattr(tracker, "webhooks"):
        tracker.webhooks = {}


@skip_when_temporal_enabled
@patch("blueprints.easypost.send_email")
def test_no_lead_id_returns_400(mock_send_email, client):
    """
    Test that when no lead ID is provided, the webhook handler:
    1. Returns a 400 status code (Bad Request)
    2. Has an error status in the response
    3. Does not send email notification (validation error handled immediately)
    """
    # Create payload with missing lead ID
    payload = {"event": {"data": {}}}

    # Send the webhook payload
    response = client.post(
        "/easypost/create_tracker", json=payload, content_type="application/json"
    )

    # Check response status code is 400 (Bad Request)
    assert response.status_code == 400

    # Check response contains error status
    response_data = json.loads(response.data)
    assert response_data["status"] == "error"
    assert "No lead_id provided" in response_data["message"]

    # Verify no email notification was sent (immediate validation error)
    mock_send_email.assert_not_called()


@skip_when_temporal_enabled
@patch("blueprints.easypost.create_tracker_task")
@patch("blueprints.easypost.send_email")
def test_lead_not_found_returns_202(
    mock_send_email, mock_task, client, close_webhook_payload
):
    """
    Test that when a lead can't be fetched, the webhook handler:
    1. Returns a 202 status code (task queued successfully)
    2. Has an accepted status in the response
    3. Includes celery_task_id in response
    4. Does not send immediate email notification (error handling happens in background task)
    """
    # Mock the Celery task to return a task ID
    mock_task_result = MagicMock()
    mock_task_result.id = "test-task-id-123"
    mock_task.delay.return_value = mock_task_result

    # Send the webhook payload
    response = client.post(
        "/easypost/create_tracker",
        json=close_webhook_payload,
        content_type="application/json",
    )

    # Check response status code is 202 (Accepted)
    assert response.status_code == 202

    # Check response contains accepted status
    response_data = json.loads(response.data)
    assert response_data["status"] == "accepted"
    assert "task queued for background processing" in response_data["message"]
    assert response_data["celery_task_id"] == "test-task-id-123"
    assert response_data["lead_id"] == "lead_123456"

    # Verify task was queued
    mock_task.delay.assert_called_once_with(close_webhook_payload)

    # Verify no immediate email notification was sent (handled by background task)
    mock_send_email.assert_not_called()


@skip_when_temporal_enabled
@patch("blueprints.easypost.create_tracker_task")
@patch("blueprints.easypost.send_email")
def test_missing_tracking_info_returns_202(
    mock_send_email, mock_task, client, close_webhook_payload
):
    """
    Test that when tracking number or carrier is missing, the webhook handler:
    1. Returns a 202 status code (task queued successfully)
    2. Has an accepted status in the response
    3. Includes celery_task_id in response
    4. Does not send immediate email notification (error handling happens in background task)
    """
    # Mock the Celery task to return a task ID
    mock_task_result = MagicMock()
    mock_task_result.id = "test-task-id-456"
    mock_task.delay.return_value = mock_task_result

    # Send the webhook payload
    response = client.post(
        "/easypost/create_tracker",
        json=close_webhook_payload,
        content_type="application/json",
    )

    # Check response status code is 202 (Accepted)
    assert response.status_code == 202

    # Check response contains accepted status
    response_data = json.loads(response.data)
    assert response_data["status"] == "accepted"
    assert "task queued for background processing" in response_data["message"]
    assert response_data["celery_task_id"] == "test-task-id-456"
    assert response_data["lead_id"] == "lead_123456"

    # Verify task was queued
    mock_task.delay.assert_called_once_with(close_webhook_payload)

    # Verify no immediate email notification was sent (handled by background task)
    mock_send_email.assert_not_called()


@skip_when_temporal_enabled
@patch("blueprints.easypost.create_tracker_task")
@patch("blueprints.easypost.send_email")
def test_easypost_api_error_returns_202(
    mock_send_email, mock_task, client, close_webhook_payload
):
    """
    Test that when the EasyPost API fails, the webhook handler:
    1. Returns a 202 status code (task queued successfully)
    2. Has an accepted status in the response
    3. Includes celery_task_id in response
    4. Does not send immediate email notification (error handling happens in background task)
    """
    # Mock the Celery task to return a task ID
    mock_task_result = MagicMock()
    mock_task_result.id = "test-task-id-789"
    mock_task.delay.return_value = mock_task_result

    # Send the webhook payload
    response = client.post(
        "/easypost/create_tracker",
        json=close_webhook_payload,
        content_type="application/json",
    )

    # Check response status code is 202 (Accepted)
    assert response.status_code == 202

    # Check response contains accepted status
    response_data = json.loads(response.data)
    assert response_data["status"] == "accepted"
    assert "task queued for background processing" in response_data["message"]
    assert response_data["celery_task_id"] == "test-task-id-789"
    assert response_data["lead_id"] == "lead_123456"

    # Verify task was queued
    mock_task.delay.assert_called_once_with(close_webhook_payload)

    # Verify no immediate email notification was sent (handled by background task)
    mock_send_email.assert_not_called()


def test_temporal_feature_flag_dispatches_workflow(
    client, close_webhook_payload, monkeypatch
):
    temporal_mock = MagicMock()
    temporal_mock.client.start_workflow.return_value = "mock-start-coro"
    monkeypatch.setattr(
        easypost_module, "USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER", True
    )
    monkeypatch.setattr(easypost_module, "temporal", temporal_mock)

    task_mock = MagicMock()
    monkeypatch.setattr(easypost_module, "create_tracker_task", task_mock)

    response = client.post(
        "/easypost/create_tracker",
        json=close_webhook_payload,
        content_type="application/json",
    )

    assert response.status_code == 202
    response_data = response.get_json()
    assert response_data["status"] == "accepted"
    assert "workflow_id" in response_data

    temporal_mock.ensure_started.assert_called_once()
    temporal_mock.client.start_workflow.assert_called_once()
    temporal_mock.run.assert_called_once_with("mock-start-coro")
    task_mock.delay.assert_not_called()


def test_temporal_feature_flag_handles_start_failure(
    client, close_webhook_payload, monkeypatch
):
    temporal_mock = MagicMock()
    temporal_mock.client.start_workflow.side_effect = RuntimeError("temporal error")
    monkeypatch.setattr(
        easypost_module, "USE_TEMPORAL_FOR_EASYPOST_CREATE_TRACKER", True
    )
    monkeypatch.setattr(easypost_module, "temporal", temporal_mock)

    response = client.post(
        "/easypost/create_tracker",
        json=close_webhook_payload,
        content_type="application/json",
    )

    assert response.status_code == 500
    response_data = response.get_json()
    assert response_data["status"] == "error"
    assert "Error enqueuing Temporal tracker workflow" in response_data["message"]

    temporal_mock.ensure_started.assert_called_once()
    temporal_mock.client.start_workflow.assert_called_once()
    temporal_mock.run.assert_not_called()
