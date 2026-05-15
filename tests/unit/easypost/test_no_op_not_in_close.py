"""Unit tests for the cross-tenant 'tracking number not in Close' soft-skip.

Background: Onspring (a sibling client) shares this EasyPost account via the
onspring-mailer fork. EasyPost fires every configured webhook for every tracker
event on the account, so MailerAutomation receives delivered events for trackers
it never created. Previously those raised "No leads found" and paged the team
after 3 retries. Now the activity returns `not_found=True` so the workflow
short-circuits silently.
"""

from unittest.mock import patch

from temporal.activities.easypost.webhook_delivery_status import (
    TrackingDetail,
    UpdateDeliveryInfoInput,
    UpdateDeliveryInfoResult,
    update_delivery_info_for_lead_activity,
)


def _minimal_query_template() -> dict:
    """Mock structure that survives the activity's nested mutation:
    close_query["query"]["queries"][1]["queries"][0]["queries"][0]["condition"]["value"] = ...
    """
    return {
        "query": {
            "queries": [
                {},
                {"queries": [{"queries": [{"condition": {"value": ""}}]}]},
            ]
        }
    }


def _input(tracking_code: str = "EZ_unknown_tracker") -> UpdateDeliveryInfoInput:
    return UpdateDeliveryInfoInput(
        tracking_code=tracking_code,
        last_tracking_detail=TrackingDetail.new(
            city="Houston", state="TX", datetime="2026-05-14T15:30:00Z"
        ),
    )


def test_returns_not_found_when_search_yields_no_leads():
    with patch(
        "temporal.activities.easypost.webhook_delivery_status.load_query",
        return_value=_minimal_query_template(),
    ), patch(
        "temporal.activities.easypost.webhook_delivery_status.search_close_leads",
        return_value=[],
    ), patch(
        "temporal.activities.easypost.webhook_delivery_status.send_email"
    ) as mock_send_email:
        result = update_delivery_info_for_lead_activity(_input())

    assert isinstance(result, UpdateDeliveryInfoResult)
    assert result.not_found is True
    assert result.lead_id == ""
    # No team email — this is the whole point of the soft-skip
    mock_send_email.assert_not_called()


def test_default_not_found_is_false_for_successful_lookups():
    """Existing call sites that construct UpdateDeliveryInfoResult without the new field
    must continue to work as 'found'. Guards against accidental contract drift."""
    r = UpdateDeliveryInfoResult(lead_id="lead_abc123")
    assert r.not_found is False
    assert r.lead_id == "lead_abc123"
