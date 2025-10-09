from typing import Any, Dict

import pytest
from flask import Flask

from blueprints import instantly
from temporal.workflows.instantly.webhook_reply_received_workflow import (
    WebhookReplyReceivedPayload,
    WebhookReplyReceivedWorkflow,
)
from temporalio.exceptions import ApplicationError
from temporal.activities.instantly import webhook_reply_received as activities


@pytest.fixture()
def flask_app():
    app = Flask(__name__)
    app.register_blueprint(instantly.instantly_bp, url_prefix="/instantly")
    return app


def _make_request(client, json_payload: Dict[str, Any]):
    return client.post("/instantly/reply_received", json=json_payload)


def test_reply_received_route_invokes_temporal_handler(monkeypatch, flask_app):
    monkeypatch.setattr(
        instantly,
        "handle_instantly_reply_received_temporal",
        lambda: ("temporal", 202),
    )
    with flask_app.test_client() as client:
        response = _make_request(client, {})

    assert response.status_code == 202
    assert response.get_data(as_text=True) == "temporal"


def test_handle_reply_received_temporal_enqueues_workflow(monkeypatch, flask_app):
    start_args: Dict[str, Any] = {}

    class FakeClient:
        def start_workflow(self, *args, **kwargs):
            start_args["args"] = args
            start_args["kwargs"] = kwargs
            return "fake-coro"

    def fake_run(coro):
        start_args["ran"] = coro
        return None

    monkeypatch.setattr(instantly.temporal, "ensure_started", lambda: None)
    monkeypatch.setattr(instantly.temporal, "client", FakeClient(), raising=False)
    monkeypatch.setattr(instantly.temporal, "run", fake_run, raising=False)

    payload = {
        "event_type": "reply_received",
        "lead_email": "lead@example.com",
        "campaign_name": "Test Campaign",
        "reply_subject": "Re: Hi",
        "reply_text": "Hello",
        "timestamp": "2023-09-01T12:00:00Z",
        "email_account": "consultant@example.com",
    }

    with flask_app.test_client() as client:
        response = _make_request(client, payload)

    assert response.status_code == 202
    body = response.get_json()
    assert body["status"] == "accepted"
    assert start_args["args"][0] == WebhookReplyReceivedWorkflow.run
    workflow_input = start_args["args"][1]
    assert isinstance(workflow_input, WebhookReplyReceivedPayload)
    assert workflow_input.json_payload == payload


def test_handle_reply_received_temporal_invalid_payload_returns_400(flask_app):
    with flask_app.test_client() as client:
        response = client.post("/instantly/reply_received", data="not-json")

    assert response.status_code == 400


def test_workflow_validation_requires_reply_body():
    payload = WebhookReplyReceivedPayload(
        json_payload={
            "event_type": "reply_received",
            "lead_email": "lead@example.com",
            "campaign_name": "Test",
            "reply_subject": "Subject",
            "reply_text": None,
            "reply_html": None,
            "timestamp": "2023-09-01T12:00:00Z",
            "email_account": "consultant@example.com",
        }
    )

    with pytest.raises(ApplicationError):
        WebhookReplyReceivedWorkflow._validate_input(payload)


def test_send_notification_email_uses_custom_recipients(monkeypatch):
    args = activities.SendNotificationEmailArgs(
        lead_id="lead123",
        lead_email="lead@example.com",
        lead_name="Lead",
        campaign_name="Campaign",
        reply_subject="Subject",
        reply_text="Body",
        reply_html=None,
        env_type="production",
        paused_subscriptions=[],
        lead_details={"id": "lead123"},
        email_activity_id="email456",
    )

    monkeypatch.setattr(
        activities,
        "determine_notification_recipients",
        lambda *_: ("consultant@example.com", None),
    )

    recorded_kwargs: Dict[str, Any] = {}

    def fake_send_email(**kwargs):
        recorded_kwargs.update(kwargs)
        return {"status": "sent", "message_id": "msg-123"}

    monkeypatch.setattr(activities, "send_email", fake_send_email)

    result = activities.send_notification_email(args)

    assert result.notification_status == "sent"
    assert result.custom_recipients_used is True
    assert recorded_kwargs["recipients"] == "consultant@example.com"


def test_send_notification_email_raises_for_consultant_errors(monkeypatch):
    args = activities.SendNotificationEmailArgs(
        lead_id="lead123",
        lead_email="lead@example.com",
        lead_name="Lead",
        campaign_name="Campaign",
        reply_subject="Subject",
        reply_text="Body",
        reply_html=None,
        env_type="production",
        paused_subscriptions=[],
        lead_details={"id": "lead123"},
        email_activity_id="email456",
    )

    monkeypatch.setattr(
        activities,
        "determine_notification_recipients",
        lambda *_: (None, "bad consultant"),
    )

    with pytest.raises(ValueError):
        activities.send_notification_email(args)


def test_add_email_activity_to_lead_returns_metadata(monkeypatch):
    payload = activities.WebhookReplyReceivedPayloadValidated(
        event_type="reply_received",
        lead_email="lead@example.com",
        campaign_name="Campaign",
        reply_subject="Subject",
        reply_text="Body",
        reply_html=None,
        timestamp="2023-09-01T12:00:00Z",
        email_account="consultant@example.com",
    )

    monkeypatch.setattr(activities, "create_email_search_query", lambda _: "query")
    monkeypatch.setattr(
        activities,
        "search_close_leads",
        lambda _: [{"id": "lead123"}],
    )
    monkeypatch.setattr(
        activities,
        "get_lead_by_id",
        lambda _: {
            "id": "lead123",
            "name": "Lead",
            "contacts": [
                {
                    "id": "contact123",
                    "emails": [{"email": "lead@example.com"}],
                }
            ],
        },
    )

    class FakeResponse:
        def json(self):
            return {"id": "email456"}

    monkeypatch.setattr(
        activities,
        "make_close_request",
        lambda *_, **__: FakeResponse(),
    )

    result = activities.add_email_activity_to_lead(
        activities.AddEmailActivityToLeadArgs(payload=payload)
    )

    assert result.lead_id == "lead123"
    assert result.email_activity_id == "email456"
    assert result.lead_details["id"] == "lead123"


def test_add_email_activity_to_lead_raises_when_no_leads(monkeypatch):
    payload = activities.WebhookReplyReceivedPayloadValidated(
        event_type="reply_received",
        lead_email="missing@example.com",
        campaign_name="Campaign",
        reply_subject="Subject",
        reply_text="Body",
        reply_html=None,
        timestamp="2023-09-01T12:00:00Z",
        email_account="consultant@example.com",
    )

    monkeypatch.setattr(activities, "create_email_search_query", lambda _: "query")
    monkeypatch.setattr(activities, "search_close_leads", lambda _: [])

    with pytest.raises(ValueError):
        activities.add_email_activity_to_lead(
            activities.AddEmailActivityToLeadArgs(payload=payload)
        )
