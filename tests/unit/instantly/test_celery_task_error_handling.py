"""
Unit tests for error handling in the Instantly Celery task.

These tests focus on the error conditions that happen during async processing
in the process_lead_batch_task Celery task.
"""

import pytest
from unittest.mock import patch
from blueprints.instantly import process_lead_batch_task


@pytest.fixture
def close_task_payload():
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


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.get_instantly_campaign_name")
def test_celery_task_no_campaign_name_sends_email(
    mock_get_campaign_name, mock_send_email, close_task_payload
):
    """
    Test that when campaign name cannot be extracted in Celery task,
    an error email is sent and error status is returned.
    """
    # Mock campaign name extraction to return empty string
    mock_get_campaign_name.return_value = ""

    # Call the Celery task function directly
    result = process_lead_batch_task(close_task_payload)

    # Check that error status is returned
    assert result["status"] == "error"
    assert "Could not extract campaign name" in result["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Instantly Campaign Name Extraction Error" in email_subject


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.get_instantly_campaign_name")
def test_celery_task_campaign_not_found_sends_email(
    mock_get_campaign_name, mock_campaign_exists, mock_send_email, close_task_payload
):
    """
    Test that when a campaign doesn't exist, the Celery task:
    1. Sends an email notification
    2. Returns error status
    """
    # Setup mocks
    mock_get_campaign_name.return_value = "BP_BC_BlindInviteEmail1"
    mock_campaign_exists.return_value = {"exists": False, "error": "Campaign not found"}

    # Call the Celery task function directly
    result = process_lead_batch_task(close_task_payload)

    # Check that error status is returned
    assert result["status"] == "error"
    assert "does not exist in Instantly" in result["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Instantly Campaign Not Found" in email_subject

    # Check email body contains relevant details
    email_body = mock_send_email.call_args[1]["body"]
    assert "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc" in email_body
    assert "BP_BC_BlindInviteEmail1" in email_body


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.get_lead_by_id")
@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.get_instantly_campaign_name")
def test_celery_task_lead_not_found_sends_email(
    mock_get_campaign_name,
    mock_campaign_exists,
    mock_get_lead,
    mock_send_email,
    close_task_payload,
):
    """
    Test that when a lead can't be found, the Celery task:
    1. Sends an email notification
    2. Returns error status
    """
    # Setup mocks
    mock_get_campaign_name.return_value = "BP_BC_BlindInviteEmail1"
    mock_campaign_exists.return_value = {"exists": True, "campaign_id": "camp_123"}
    mock_get_lead.return_value = None

    # Call the Celery task function directly
    result = process_lead_batch_task(close_task_payload)

    # Check that error status is returned
    assert result["status"] == "error"
    assert "Could not retrieve lead details" in result["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Close Lead Details Error" in email_subject


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.get_lead_by_id")
@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.get_instantly_campaign_name")
def test_celery_task_lead_no_email_sends_notification(
    mock_get_campaign_name,
    mock_campaign_exists,
    mock_get_lead,
    mock_send_email,
    close_task_payload,
):
    """
    Test that when a lead has no email address, the Celery task:
    1. Sends an email notification
    2. Returns error status
    """
    # Setup mocks
    mock_get_campaign_name.return_value = "BP_BC_BlindInviteEmail1"
    mock_campaign_exists.return_value = {"exists": True, "campaign_id": "camp_123"}
    mock_get_lead.return_value = {
        "id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
        "name": "Test Lead",
        "contacts": [{"id": "cont_123", "emails": []}],  # No emails
    }

    # Call the Celery task function directly
    result = process_lead_batch_task(close_task_payload)

    # Check that error status is returned
    assert result["status"] == "error"
    assert "No email found for lead ID" in result["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Close Lead Email Error" in email_subject


@patch("blueprints.instantly._webhook_tracker")
@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.add_to_instantly_campaign")
@patch("blueprints.instantly.get_lead_by_id")
@patch("blueprints.instantly.campaign_exists")
@patch("blueprints.instantly.get_instantly_campaign_name")
def test_celery_task_instantly_api_error_sends_email(
    mock_get_campaign_name,
    mock_campaign_exists,
    mock_get_lead,
    mock_add_to_campaign,
    mock_send_email,
    mock_webhook_tracker,
    close_task_payload,
):
    """
    Test that when the Instantly API returns an error, the Celery task:
    1. Sends an email notification
    2. Returns error status
    """
    # Setup mocks
    mock_get_campaign_name.return_value = "BP_BC_BlindInviteEmail1"
    mock_campaign_exists.return_value = {"exists": True, "campaign_id": "camp_123"}
    mock_get_lead.return_value = {
        "id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
        "name": "Test Lead",
        "contacts": [{"id": "cont_123", "emails": [{"email": "test@example.com"}]}],
        "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o": "Test Company",
        "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9": "New York, 2025-01-01",
    }
    mock_add_to_campaign.return_value = {
        "status": "error",
        "message": "API rate limit exceeded",
    }

    # Call the Celery task function directly
    result = process_lead_batch_task(close_task_payload)

    # Check that error status is returned
    assert result["status"] == "error"
    assert "Failed to add lead to Instantly" in result["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Instantly API Error" in email_subject


@patch("blueprints.instantly.send_email")
@patch("blueprints.instantly.get_instantly_campaign_name")
def test_celery_task_exception_sends_email(
    mock_get_campaign_name, mock_send_email, close_task_payload
):
    """
    Test that when an unexpected exception occurs in the Celery task,
    an error email is sent and error status is returned.
    """
    # Make get_instantly_campaign_name raise an exception
    mock_get_campaign_name.side_effect = Exception("Unexpected error")

    # Call the Celery task function directly
    result = process_lead_batch_task(close_task_payload)

    # Check that error status is returned
    assert result["status"] == "error"
    assert "Unexpected error" in result["message"]

    # Verify email notification was sent
    mock_send_email.assert_called_once()
    email_subject = mock_send_email.call_args[1]["subject"]
    assert "Instantly Async Processing Error" in email_subject
