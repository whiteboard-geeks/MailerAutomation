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

@patch("blueprints.easypost._webhook_tracker.add")
@patch("blueprints.easypost.create_package_delivered_custom_activity_in_close")
@patch("blueprints.easypost.update_delivery_information_for_lead")
@patch("blueprints.easypost.get_lead_by_id")
@patch("blueprints.easypost.search_close_leads")
def test_single_lead_found_and_valid(
    mock_search_close_leads,
    mock_get_lead_by_id,
    mock_update_delivery,
    mock_create_activity,
    mock_webhook_tracker_add
):
    """Test the behavior when a single lead is found and it's valid."""
    # GIVEN
    tracking_code = "1Z999AA10123456789"
    tracker_id = "trk_test123"
    payload_data = create_payload_data(tracker_id=tracker_id, tracking_code=tracking_code)
    
    # Mock search_close_leads to return a single lead
    lead_id = "lead_123456"
    mock_search_close_leads.return_value = [
        {
            "id": lead_id,
            "name": "Test Lead"
        }
    ]
    
    # Mock get_lead_by_id to return a valid lead
    mock_get_lead_by_id.return_value = {
        "id": lead_id,
        "name": "Test Lead",
        "custom": {
            "some_field": "some_value"
        }
    }
    
    # WHEN
    result = process_delivery_status_task(payload_data)
    
    # THEN
    # Verify search_close_leads was called
    mock_search_close_leads.assert_called_once()
    
    # Verify get_lead_by_id was called with the correct lead ID
    mock_get_lead_by_id.assert_called_once_with(lead_id)
    
    # Verify update_delivery_information_for_lead was called with the correct parameters
    mock_update_delivery.assert_called_once()
    assert mock_update_delivery.call_args[0][0] == lead_id
    
    # Verify create_package_delivered_custom_activity_in_close was called with the correct parameters
    mock_create_activity.assert_called_once()
    assert mock_create_activity.call_args[0][0] == lead_id
    
    # Verify the webhook tracker was updated with "Success"
    mock_webhook_tracker_add.assert_called_once()
    args, _ = mock_webhook_tracker_add.call_args
    assert args == (tracker_id, {
        'processed': True,
        'result': 'Success',
        'lead_id': lead_id,
        'delivery_information': ANY,
        'timestamp': ANY
    })
    
    # Verify the return value
    assert result == {
        "status": "success",
        "lead_id": lead_id,
        "delivery_information": ANY
    }


@patch("blueprints.easypost._webhook_tracker.add")
@patch("blueprints.easypost.get_lead_by_id", return_value=None)
@patch("blueprints.easypost.search_close_leads")
def test_multiple_leads_found_but_none_valid(mock_search_close_leads, mock_get_lead_by_id, mock_webhook_tracker_add):
    """Test the behavior when multiple leads are found but none are valid."""
    # GIVEN
    tracker_id = "trk_test123"
    payload_data = create_payload_data(tracker_id=tracker_id)
    
    # Mock search_close_leads to return multiple leads
    lead_id_1 = "lead_123456"
    lead_id_2 = "lead_789012"
    mock_search_close_leads.return_value = [
        {
            "id": lead_id_1,
            "name": "Test Lead 1"
        },
        {
            "id": lead_id_2,
            "name": "Test Lead 2"
        }
    ]
    
    # WHEN
    result = process_delivery_status_task(payload_data)
    
    # THEN
    # Verify search_close_leads was called
    mock_search_close_leads.assert_called_once()
    
    # Verify get_lead_by_id was called for each lead
    assert mock_get_lead_by_id.call_count == 2
    mock_get_lead_by_id.assert_any_call(lead_id_1)
    mock_get_lead_by_id.assert_any_call(lead_id_2)
    
    # Verify the webhook tracker was updated with "No valid leads found"
    mock_webhook_tracker_add.assert_called_once()
    args, _ = mock_webhook_tracker_add.call_args
    assert args == (tracker_id, {'processed': True, 'result': 'No valid leads found', 'timestamp': ANY})
    
    # Verify the return value
    assert result["status"] == "success"
    assert "No valid leads found" in result["message"]


@patch("blueprints.easypost._webhook_tracker.add")
@patch("blueprints.easypost.create_package_delivered_custom_activity_in_close")
@patch("blueprints.easypost.update_delivery_information_for_lead")
@patch("blueprints.easypost.get_lead_by_id")
@patch("blueprints.easypost.search_close_leads")
def test_multiple_leads_found_and_exactly_one_valid(
    mock_search_close_leads,
    mock_get_lead_by_id,
    mock_update_delivery,
    mock_create_activity,
    mock_webhook_tracker_add
):
    """Test the behavior when multiple leads are found but only one is valid."""
    # GIVEN
    tracker_id = "trk_test123"
    payload_data = create_payload_data(tracker_id=tracker_id)
    
    # Mock search_close_leads to return multiple leads
    lead_id_1 = "lead_123456"
    lead_id_2 = "lead_789012"
    mock_search_close_leads.return_value = [
        {
            "id": lead_id_1,
            "name": "Test Lead 1"
        },
        {
            "id": lead_id_2,
            "name": "Test Lead 2"
        }
    ]
    
    # Mock get_lead_by_id to return a valid lead for only one of the leads
    # Using side_effect to return different values based on input
    def get_lead_side_effect(lead_id):
        if lead_id == lead_id_1:
            return None  # First lead is not valid
        elif lead_id == lead_id_2:
            return {  # Second lead is valid
                "id": lead_id_2,
                "name": "Test Lead 2",
                "custom": {
                    "some_field": "some_value"
                }
            }
        return None
    
    mock_get_lead_by_id.side_effect = get_lead_side_effect
    
    # WHEN
    result = process_delivery_status_task(payload_data)
    
    # THEN
    # Verify search_close_leads was called
    mock_search_close_leads.assert_called_once()
    
    # Verify get_lead_by_id was called for each lead
    assert mock_get_lead_by_id.call_count == 2
    mock_get_lead_by_id.assert_any_call(lead_id_1)
    mock_get_lead_by_id.assert_any_call(lead_id_2)
    
    # Verify update_delivery_information_for_lead was called with the correct parameters
    mock_update_delivery.assert_called_once()
    assert mock_update_delivery.call_args[0][0] == lead_id_2
    
    # Verify create_package_delivered_custom_activity_in_close was called with the correct parameters
    mock_create_activity.assert_called_once()
    assert mock_create_activity.call_args[0][0] == lead_id_2
    
    # Verify the webhook tracker was updated with "Success"
    mock_webhook_tracker_add.assert_called_once()
    args, _ = mock_webhook_tracker_add.call_args
    assert args == (tracker_id, {
        'processed': True,
        'result': 'Success',
        'lead_id': lead_id_2,
        'delivery_information': ANY,
        'timestamp': ANY
    })
    
    # Verify the return value
    assert result == {
        "status": "success",
        "lead_id": lead_id_2,
        "delivery_information": ANY
    }

@patch("blueprints.easypost._webhook_tracker.add")
@patch("blueprints.easypost.get_lead_by_id")
@patch("blueprints.easypost.search_close_leads")
def test_multiple_leads_found_and_more_than_one_valid(
    mock_search_close_leads,
    mock_get_lead_by_id,
    mock_webhook_tracker_add
):
    """Test the behavior when multiple leads are found and more than one is valid."""
    # GIVEN
    tracking_code = "1Z999AA10123456789"
    tracker_id = "trk_test123"
    payload_data = create_payload_data(tracker_id=tracker_id, tracking_code=tracking_code)
    
    # Mock search_close_leads to return multiple leads
    lead_id_1 = "lead_123456"
    lead_id_2 = "lead_789012"
    lead_id_3 = "lead_345678"
    mock_search_close_leads.return_value = [
        {
            "id": lead_id_1,
            "name": "Test Lead 1"
        },
        {
            "id": lead_id_2,
            "name": "Test Lead 2"
        },
        {
            "id": lead_id_3,
            "name": "Test Lead 3"
        }
    ]
    
    # Mock get_lead_by_id to return valid leads for more than one of the leads
    # Using side_effect to return different values based on input
    def get_lead_side_effect(lead_id):
        if lead_id == lead_id_1:
            return {  # First lead is valid
                "id": lead_id_1,
                "name": "Test Lead 1",
                "custom": {
                    "some_field": "some_value"
                }
            }
        elif lead_id == lead_id_2:
            return None  # Second lead is not valid
        elif lead_id == lead_id_3:
            return {  # Third lead is valid
                "id": lead_id_3,
                "name": "Test Lead 3",
                "custom": {
                    "some_field": "some_value"
                }
            }
        return None
    
    mock_get_lead_by_id.side_effect = get_lead_side_effect
    
    # WHEN
    result = process_delivery_status_task(payload_data)
    
    # THEN
    # Verify search_close_leads was called
    mock_search_close_leads.assert_called_once()
    
    # Verify get_lead_by_id was called for each lead
    assert mock_get_lead_by_id.call_count == 3
    mock_get_lead_by_id.assert_any_call(lead_id_1)
    mock_get_lead_by_id.assert_any_call(lead_id_2)
    mock_get_lead_by_id.assert_any_call(lead_id_3)
    
    # Verify the webhook tracker was updated with "Multiple valid leads found"
    mock_webhook_tracker_add.assert_called_once()
    args, _ = mock_webhook_tracker_add.call_args
    assert args == (tracker_id, {'processed': True, 'result': 'Multiple valid leads found', 'timestamp': ANY})
    
    # Verify the return value
    assert result["status"] == "success"
    assert "Multiple valid leads found" in result["message"]

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
