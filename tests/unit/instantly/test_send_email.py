"""Unit tests for `utils.email.send_email`."""

from unittest.mock import patch

import pytest

from utils import email as email_module


@pytest.fixture
def subject():
    return "Test Subject"


@pytest.fixture
def html_body():
    return "<h1>Example</h1>"


def test_send_email_non_production(monkeypatch, subject, html_body):
    """In non-production environments the helper should no-op."""
    monkeypatch.setattr(email_module, "env_type", "development")

    with patch("blueprints.gmail.send_gmail") as mock_send_gmail:
        result = email_module.send_email(subject, html_body)

    assert result == {"status": "success", "message": "Email not sent in non-production env"}
    mock_send_gmail.assert_not_called()


def test_send_email_production_invokes_gmail(monkeypatch, subject, html_body):
    """In production the helper should enrich email content and call Gmail."""
    monkeypatch.setattr(email_module, "env_type", "production")

    with patch("blueprints.gmail.send_gmail") as mock_send_gmail:
        mock_send_gmail.return_value = {"status": "success", "message_id": "mock_id"}

        result = email_module.send_email(
            subject,
            html_body,
            text_content="Plain text",
            recipients="custom@example.com",
        )

    assert result == {"status": "success", "message_id": "mock_id"}

    mock_send_gmail.assert_called_once()
    call_kwargs = mock_send_gmail.call_args.kwargs

    assert call_kwargs["sender"] == "lance@whiteboardgeeks.com"
    assert call_kwargs["to"] == "custom@example.com"
    assert call_kwargs["html_content"].startswith("<p><strong>Environment:</strong> production</p>" + html_body)
    assert call_kwargs["text_content"].startswith("Environment: production\n\nPlain text")
    assert call_kwargs["subject"].startswith(f"[MailerAutomation] [production] {subject}")
