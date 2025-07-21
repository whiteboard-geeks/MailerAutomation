from datetime import datetime
from unittest.mock import ANY, patch

import pytest
from blueprints.easypost import process_delivery_status_task


@patch("blueprints.easypost._webhook_tracker.add")
@patch("blueprints.easypost.search_close_leads", return_value=[])
def test_no_leads_found(mock_search_close_leads, mock_webhook_tracker_add):
    """Test the behavior when no leads are found with the tracking number."""
    # GIVEN
    tracker_id = "trk_test123"
    tracking_code = "1Z999AA10123456789"
    payload_data = create_payload_data(tracker_id=tracker_id, tracking_code=tracking_code)
    
    # WHEN
    result = process_delivery_status_task(payload_data)
    
    # THEN
    # Verify search_close_leads was called with a query containing the tracking number
    mock_search_close_leads.assert_called_once()
    query_arg = mock_search_close_leads.call_args[0][0]
    assert query_arg["query"]["queries"][1]["queries"][0]["queries"][0]["condition"]["value"] == tracking_code
    
    # Verify the webhook tracker was updated with "No leads found"
    mock_webhook_tracker_add.assert_called_once()
    args, _ = mock_webhook_tracker_add.call_args
    assert args == (tracker_id, {'processed': True, 'result': 'No leads found', 'timestamp': ANY})

    # Verify the return value
    assert result["status"] == "success"
    assert "No leads found" in result["message"]


@patch("blueprints.easypost._webhook_tracker.add")
@patch("blueprints.easypost.get_lead_by_id", return_value=None)
@patch("blueprints.easypost.search_close_leads")
def test_single_lead_found_but_not_valid(mock_search_close_leads, mock_get_lead_by_id, mock_webhook_tracker_add):
    """Test the behavior when a single lead is found but it's not valid."""
    # GIVEN
    tracking_code_dont_care = "1Z999AA10123456789"
    tracker_id = "trk_test123"
    payload_data = create_payload_data(tracker_id=tracker_id, tracking_code=tracking_code_dont_care)
    
    # Mock search_close_leads to return a single lead
    lead_id = "lead_123456"
    mock_search_close_leads.return_value = [
        {
            "id": lead_id,
            "name": "Test Lead"
        }
    ]
    
    # WHEN
    # Call the process_delivery_status_task function
    result = process_delivery_status_task(payload_data)
    
    # THEN
    # Verify search_close_leads was called
    mock_search_close_leads.assert_called_once()
    
    # Verify get_lead_by_id was called with the correct lead ID
    mock_get_lead_by_id.assert_called_once_with(lead_id)
    
    # Verify the webhook tracker was updated with "Lead not found"
    mock_webhook_tracker_add.assert_called_once()
    args, _ = mock_webhook_tracker_add.call_args
    assert args == (tracker_id, {'processed': True, 'result': 'Lead not found', 'timestamp': ANY})
    
    # Verify the return value
    assert result["status"] == "success"
    assert "returned 404 or error" in result["message"]

def test_single_lead_found_and_valid():
    pass


def test_multiple_leads_found_but_none_valid():
    pass


def test_multiple_leads_found_and_exactly_one_valid():
    pass


def test_multiple_leads_found_and_more_than_one_valid():
    pass


def create_payload_data(tracker_id="", tracking_code=""):
    return {
        "id": "evt_test123",
        "result": {
            "id": tracker_id,
            "tracking_code": tracking_code,
            "carrier": "UPS",
            "status": "delivered",
            "tracking_details": [
                {
                    "tracking_location": {
                        "city": "austin",
                        "state": "tx"
                    },
                    "datetime": "2023-12-18T12:00:00Z"
                }
            ]
        }
    }
