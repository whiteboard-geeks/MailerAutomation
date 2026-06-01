"""Tests for excluding 'Lance' campaigns from Instantly webhook automation.

Campaigns with 'lance' in the name (e.g. capital-raise outreach) must bypass the
mailer automation entirely — no Close task completion, no reply routing — so they
never enqueue a Temporal workflow.
"""

from typing import Any, Dict

import pytest
from flask import Flask

from blueprints import instantly


@pytest.fixture()
def flask_app():
    app = Flask(__name__)
    app.register_blueprint(instantly.instantly_bp, url_prefix="/instantly")
    return app


def _fail_if_temporal_used(monkeypatch):
    """Make any Temporal enqueue blow up, so excluded campaigns are proven to skip it."""
    def boom(*args, **kwargs):
        raise AssertionError("Temporal must not be invoked for excluded campaigns")

    monkeypatch.setattr(instantly.temporal, "ensure_started", lambda: None, raising=False)
    monkeypatch.setattr(instantly.temporal, "run", boom, raising=False)


@pytest.mark.parametrize("name", ["Lance Conroe 2026-06-01", "lance_test", "MY LANCE RAISE"])
def test_campaign_name_excluded_true(name):
    assert instantly._campaign_name_excluded(name) is True


@pytest.mark.parametrize("name", ["BP_BC_BlindInviteEmail1", "APRIL_BC_NoShowEmail1", "", None])
def test_campaign_name_excluded_false(name):
    assert instantly._campaign_name_excluded(name) is False


def test_reply_received_excluded_campaign_skips_temporal(monkeypatch, flask_app):
    _fail_if_temporal_used(monkeypatch)
    payload: Dict[str, Any] = {
        "event_type": "reply_received",
        "lead_email": "lead@example.com",
        "campaign_name": "Lance Conroe 2026-06-01",
        "reply_subject": "Re: Hi",
        "reply_text": "Hello",
        "timestamp": "2023-09-01T12:00:00Z",
        "email_account": "consultant@example.com",
    }
    with flask_app.test_client() as client:
        response = client.post("/instantly/reply_received", json=payload)

    assert response.status_code == 200
    assert response.get_json()["status"] == "ignored"


def test_email_sent_excluded_campaign_skips_temporal(monkeypatch, flask_app):
    _fail_if_temporal_used(monkeypatch)
    payload = {"campaign_name": "Lance Conroe 2026-06-01", "lead_email": "lead@example.com"}
    with flask_app.test_client() as client:
        response = client.post("/instantly/email_sent", json=payload)

    assert response.status_code == 200
    assert response.get_json()["status"] == "ignored"
