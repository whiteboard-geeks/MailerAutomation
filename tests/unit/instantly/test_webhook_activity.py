"""Unit tests for the Instantly add lead Temporal activity."""

import pytest
from unittest.mock import patch

from temporal.activities.instantly.webhook_add_lead import (
    add_lead_to_instantly_campaign,
    AddLeadToInstantlyCampaignArgs,
    LeadDetails,
)


@patch("temporal.activities.instantly.webhook_add_lead.send_email")
@patch("temporal.activities.instantly.webhook_add_lead.add_to_instantly_campaign")
@patch("temporal.activities.instantly.webhook_add_lead._get_lead_details_from_close")
@patch("temporal.activities.instantly.webhook_add_lead.campaign_exists")
def test_add_lead_to_instantly_campaign_api_error(
    mock_campaign_exists,
    mock_get_lead_details,
    mock_add_to_campaign,
    mock_send_email,
):
    """Activity should email and raise when Instantly API returns an error."""
    args = AddLeadToInstantlyCampaignArgs(
        lead_id="lead-123",
        campaign_name="Existing Campaign",
        task_text="Instantly: Existing Campaign",
    )

    mock_campaign_exists.return_value = {"exists": True, "campaign_id": "camp-123"}
    mock_get_lead_details.return_value = LeadDetails(
        email="test@example.com",
        first_name="Test",
        last_name="User",
        company_name="ACME",
        date_location="",
    )
    mock_add_to_campaign.return_value = {
        "status": "error",
        "message": "Instantly API rate limit exceeded",
    }

    with pytest.raises(ValueError) as exc:
        add_lead_to_instantly_campaign(args)

    assert "Failed to add lead to Instantly" in str(exc.value)
    mock_send_email.assert_called_once()
    assert mock_send_email.call_args.kwargs["subject"] == "Instantly API Error (Async)"
    assert "Instantly API rate limit exceeded" in mock_send_email.call_args.kwargs["body"]
