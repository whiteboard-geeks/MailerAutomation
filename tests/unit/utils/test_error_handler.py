"""The global error handler emails real errors, not client errors like 404s."""

import os
from typing import Any
from unittest.mock import MagicMock, patch

os.environ.setdefault("TEMPORAL_WORKFLOW_UI_BASE_URL", "http://temporal.test")
os.environ.setdefault("CLOSE_API_KEY", "dummy")

import app as app_module  # noqa: E402

flask_app = app_module.flask_app
if "boom_test" not in flask_app.view_functions:
    flask_app.add_url_rule("/__boom_test", "boom_test", lambda: 1 / 0)
    flask_app.add_url_rule(
        "/__post_only_test", "post_only_test", lambda: "ok", methods=["POST"]
    )


def _get(path: str) -> tuple[Any, MagicMock]:
    with patch.object(app_module, "send_email") as send:
        resp = flask_app.test_client().get(path)
    return resp, send


def test_scanner_404_sends_no_email() -> None:
    resp, send = _get("/.env")
    assert resp.status_code == 404
    send.assert_not_called()


def test_wrong_method_405_sends_no_email() -> None:
    resp, send = _get("/__post_only_test")
    assert resp.status_code == 405
    send.assert_not_called()


def test_real_exception_still_emails() -> None:
    resp, send = _get("/__boom_test")
    assert resp.status_code == 500
    send.assert_called_once()


def test_debug_env_closed_in_production() -> None:
    with patch.object(app_module, "env_type", "production"):
        resp, _ = _get("/debug/env")
    assert resp.status_code == 403


def test_debug_env_shows_no_credential_text() -> None:
    secret = '{"type":"service_account","private_key":"xyz"}'
    with patch.object(app_module, "env_type", "staging"), patch.dict(
        os.environ, {"GMAIL_SERVICE_ACCOUNT_INFO": secret}
    ):
        resp, _ = _get("/debug/env")
    body = resp.get_json()
    assert body["gmail_service_account_info"] == f"Found ({len(secret)} chars)"
    assert "xyz" not in resp.get_data(as_text=True)
