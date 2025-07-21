from unittest.mock import patch
from blueprints.easypost import process_delivery_status_task


@patch("blueprints.easypost._webhook_tracker.add")
@patch("blueprints.easypost.search_close_leads", return_value=[])
def test_no_leads_found(mock_search_close_leads, mock_webhook_tracker_add):
    """Test the behavior when no leads are found with the tracking number."""
    # GIVEN
    payload_data = {
        "id": "evt_test123",
        "result": {
            "id": "trk_test123",
            "tracking_code": "1Z999AA10123456789",
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
    
    # WHEN
    result = process_delivery_status_task(payload_data)
    
    # THEN
    # Verify search_close_leads was called with a query containing the tracking number
    mock_search_close_leads.assert_called_once()
    query_arg = mock_search_close_leads.call_args[0][0]
    assert query_arg["query"]["queries"][1]["queries"][0]["queries"][0]["condition"]["value"] == "1Z999AA10123456789"
    
    # Verify the webhook tracker was updated with "No leads found"
    mock_webhook_tracker_add.assert_called_once()
    args, _ = mock_webhook_tracker_add.call_args
    assert args[0] == "trk_test123"  # tracker_id
    assert args[1]["processed"] is True
    assert args[1]["result"] == "No leads found"
    
    # Verify the return value
    assert result["status"] == "success"
    assert "No leads found" in result["message"]


def test_single_lead_found_but_not_valid():
    pass


def test_single_lead_found_and_valid():
    pass


def test_multiple_leads_found_but_none_valid():
    pass


def test_multiple_leads_found_and_exactly_one_valid():
    pass


def test_multiple_leads_found_and_more_than_one_valid():
    pass
