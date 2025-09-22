"""Integration tests for Instantly webhook failure notifications."""

import copy
import os
import time
import uuid

import pytest

from app import flask_app
from blueprints.gmail import check_for_emails
from utils import email as email_module
from utils.instantly import get_instantly_campaigns


BASE_PAYLOAD = {
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
            "date": "2025-03-18",
            "id": "task_07y7VvRV9HXrxDsDCMpZUOdkgKRsCRpmV7fVnSrAhaM",
            "date_created": "2025-03-18T10:54:52.096000+00:00",
            "lead_id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
            "text": "Instantly: Placeholder",
        },
    },
}


def _ensure_email_enabled():
    if email_module.env_type.lower() != "production":
        pytest.skip("utils.email.send_email disabled outside production environment")
    if not os.environ.get("GMAIL_SERVICE_ACCOUNT_INFO"):
        pytest.skip("GMAIL_SERVICE_ACCOUNT_INFO not configured")


@pytest.fixture
def client():
    _ensure_email_enabled()
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _build_payload(task_text: str, lead_id: str) -> dict:
    payload = copy.deepcopy(BASE_PAYLOAD)
    payload["event"]["data"]["text"] = task_text
    payload["event"]["data"]["lead_id"] = lead_id
    payload["event"]["lead_id"] = lead_id
    payload["event"]["id"] = f"ev_{uuid.uuid4()}"
    payload["event"]["data"]["id"] = f"task_{uuid.uuid4()}"
    return payload


def _wait_for_email(recipient: str, subject: str, token: str, timeout: int = 180, poll_interval: int = 10):
    deadline = time.time() + timeout
    query = f'subject:"{subject}"'

    while time.time() < deadline:
        result = check_for_emails(
            user_email=recipient,
            query=query,
            max_results=5,
            include_content=True,
        )

        if result.get("status") == "success":
            for message in result.get("messages", []):
                snippet = message.get("snippet") or ""
                body = message.get("body", {}) or {}
                text_body = body.get("text") or ""
                html_body = body.get("html") or ""

                if any(token in content for content in (snippet, text_body, html_body)):
                    return message

        time.sleep(poll_interval)

    raise AssertionError(
        f"Email with subject '{subject}' containing token '{token}' not found within {timeout}s"
    )


def _recipient() -> str:
    return os.environ.get("TEST_EMAIL_RECIPIENT", "lance@whiteboardgeeks.com")


@pytest.mark.webhook_failures
def test_campaign_not_found_sends_real_email(client):
    token = str(uuid.uuid4())
    campaign_name = f"Integration Missing Campaign {token}"
    task_text = f"Instantly: {campaign_name}"
    lead_id = f"lead_campaign_missing_{token}"

    response = client.post("/instantly/add_lead", json=_build_payload(task_text, lead_id))
    assert response.status_code == 200
    response_data = response.get_json()
    assert response_data == {"status": "success", "message": "Webhook received"}

    subject = f"Instantly Campaign Not Found: {campaign_name}"
    message = _wait_for_email(_recipient(), subject, token)
    body = message.get("body") or {}
    assert token in (message.get("snippet") or "")
    if body:
        assert token in body.get("text", "") or token in body.get("html", "")


@pytest.mark.webhook_failures
def test_lead_not_found_sends_real_email(client):
    token = str(uuid.uuid4())
    lead_id = f"lead_missing_{token}"

    campaigns = get_instantly_campaigns(limit=1)
    campaigns_list = campaigns.get("campaigns", []) if campaigns.get("status") == "success" else []
    if not campaigns_list:
        pytest.skip("No Instantly campaigns available to execute integration test")
    campaign_name = campaigns_list[0].get("name")
    task_text = f"Instantly: {campaign_name}"

    response = client.post("/instantly/add_lead", json=_build_payload(task_text, lead_id))
    assert response.status_code == 200
    response_data = response.get_json()
    assert response_data == {"status": "success", "message": "Webhook received"}

    subject = "Close Lead Details Error (Async)"
    message = _wait_for_email(_recipient(), subject, token)
    body = message.get("body") or {}
    assert token in (message.get("snippet") or "") or lead_id in (message.get("snippet") or "")
    if body:
        text_content = body.get("text", "")
        html_content = body.get("html", "")
        assert lead_id in text_content or lead_id in html_content


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
