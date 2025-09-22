"""Integration tests for the `utils.email.send_email` helper."""

import os
import time
import uuid

import pytest

from blueprints.gmail import check_for_emails
from utils import email as email_module


def _wait_for_email(
    recipient: str,
    subject: str,
    token: str,
    timeout: int = 60,
    poll_interval: int = 5,
):
    """Poll Gmail until an email with the given subject/token arrives."""
    deadline = time.time() + timeout
    query = f"subject:{subject}"

    while time.time() < deadline:
        result = check_for_emails(
            user_email=recipient,
            query=query,
            max_results=5,
            include_content=True,
        )

        if result.get("status") != "success":
            time.sleep(poll_interval)
            continue

        for message in result.get("messages", []):
            snippet = message.get("snippet", "") or ""
            body = message.get("body", {}) or {}
            text_body = body.get("text") or ""
            html_body = body.get("html") or ""

            if token in snippet or token in text_body or token in html_body:
                return message

        time.sleep(poll_interval)

    raise AssertionError(
        f"Email with subject '{subject}' containing token '{token}' not found within {timeout}s"
    )


def test_send_email_with_real_gmail_api(monkeypatch):
    """Send a real email via the helper and verify it arrives in Gmail."""
    if not os.environ.get("GMAIL_SERVICE_ACCOUNT_INFO"):
        pytest.skip("GMAIL_SERVICE_ACCOUNT_INFO not configured")

    monkeypatch.setattr(email_module, "env_type", "production")

    test_token = str(uuid.uuid4())
    subject = f"Integration Email {test_token}"
    html_body = f"<h1>Integration Test Email</h1><p>Token: {test_token}</p>"
    text_body = f"Integration Test Email\nToken: {test_token}"
    recipient = os.environ.get("TEST_EMAIL_RECIPIENT", "lance@whiteboardgeeks.com")

    result = email_module.send_email(
        subject,
        html_body,
        text_content=text_body,
        recipients=recipient,
    )

    if result.get("status") != "success":
        if os.environ.get("CI") != "true":
            pytest.skip(f"Real Gmail send skipped: {result.get('message')}")
        pytest.fail(f"Real Gmail send failed: {result.get('message')}")

    _wait_for_email(recipient=recipient, subject=subject, token=test_token)
