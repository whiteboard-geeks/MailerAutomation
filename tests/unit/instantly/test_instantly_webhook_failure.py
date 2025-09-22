"""Unit tests for the Instantly add lead webhook endpoint."""

import pytest
from unittest.mock import MagicMock, patch
from flask import Flask

from blueprints.instantly import instantly_bp
from temporal.shared import TASK_QUEUE_NAME
from temporal.workflows.instantly.webhook_add_lead_workflow import (
    WebhookAddLeadPayload,
    WebhookAddLeadWorkflow,
)


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
            "object_type": "task.lead",
            "lead_id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
            "action": "created",
            "data": {
                "id": "task_07y7VvRV9HXrxDsDCMpZUOdkgKRsCRpmV7fVnSrAhaM",
                "lead_id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
                "text": "Instantly: BP_BC_BlindInviteEmail1 [Noura Test]",
            },
        },
    }


@patch("blueprints.instantly.temporal")
def test_valid_webhook_starts_temporal_workflow(mock_temporal, client, close_task_created_payload):
    """Ensure the route starts the Temporal workflow with the expected payload."""
    mock_temporal.client = MagicMock()
    workflow_handle = MagicMock()
    mock_temporal.client.start_workflow.return_value = workflow_handle

    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    assert response.status_code == 200
    response_data = response.get_json()
    assert response_data["status"] == "success"
    assert response_data["message"] == "Webhook received"

    mock_temporal.ensure_started.assert_called_once()
    mock_temporal.client.start_workflow.assert_called_once()
    mock_temporal.run.assert_called_once_with(workflow_handle)

    args, kwargs = mock_temporal.client.start_workflow.call_args
    assert args[0] == WebhookAddLeadWorkflow.run
    assert isinstance(args[1], WebhookAddLeadPayload)
    assert args[1].json_payload == close_task_created_payload
    assert kwargs["task_queue"] == TASK_QUEUE_NAME
    assert kwargs["id"]


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.temporal")
def test_temporal_failure_sends_email_and_returns_200(
    mock_temporal,
    mock_send_email,
    client,
    close_task_created_payload,
):
    """Verify Temporal failures are reported via email and return a 200 response."""
    mock_temporal.client = MagicMock()
    workflow_handle = MagicMock()
    mock_temporal.client.start_workflow.return_value = workflow_handle
    mock_temporal.run.side_effect = RuntimeError("Temporal failure")

    response = client.post(
        "/instantly/add_lead",
        json=close_task_created_payload,
        content_type="application/json",
    )

    assert response.status_code == 200
    response_data = response.get_json()
    assert response_data["status"] == "success"
    assert "Temporal failure" in response_data["error"]

    mock_temporal.ensure_started.assert_called_once()
    mock_temporal.run.assert_called_once_with(workflow_handle)
    mock_send_email.assert_called_once()


def test_non_json_payload_returns_400(client):
    """Non-JSON payloads should be rejected with a 400 status."""
    response = client.post(
        "/instantly/add_lead",
        data="not json",
        content_type="text/plain",
    )

    assert response.status_code == 400
    response_data = response.get_json()
    assert response_data["status"] == "error"
