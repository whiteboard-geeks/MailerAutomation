"""
Unit tests for the EasyPost webhook handler failure modes.
"""

import pytest
from unittest.mock import MagicMock
from blueprints import easypost as easypost_module
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


@pytest.fixture(autouse=True)
def disable_webhook_tracker_redis(monkeypatch):
    """Prevent tests from attempting to write to an actual Redis instance."""
    tracker = easypost_module._webhook_tracker
    monkeypatch.setattr(tracker, "redis", None)
    if not hasattr(tracker, "webhooks"):
        tracker.webhooks = {}


def test_temporal_feature_flag_dispatches_workflow(
    client, close_webhook_payload, monkeypatch
):
    temporal_mock = MagicMock()
    temporal_mock.client.start_workflow.return_value = "mock-start-coro"
    monkeypatch.setattr(easypost_module, "temporal", temporal_mock)

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


def test_temporal_feature_flag_handles_start_failure(
    client, close_webhook_payload, monkeypatch
):
    temporal_mock = MagicMock()
    temporal_mock.client.start_workflow.side_effect = RuntimeError("temporal error")
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
