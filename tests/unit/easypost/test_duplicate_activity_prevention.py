"""
Unit tests for duplicate mailer delivered custom activity prevention.
"""

from unittest.mock import Mock, patch
from datetime import datetime
import requests

from utils.easypost import _check_existing_mailer_delivered_activities


class TestDuplicateActivityPrevention:
    """Test cases for preventing duplicate mailer delivered custom activities."""

    def setup_method(self):
        """Setup test data before each test."""
        self.test_lead_id = "lead_test123"
        self.test_delivery_information = {
            "date_and_location_of_mailer_delivered": "Mon 12/18 to Austin, TX",
            "delivery_state": "TX",
            "delivery_city": "Austin",
            "delivery_date": datetime.strptime("2023-12-18", "%Y-%m-%d").date(),
            "delivery_date_readable": "Mon 12/18",
            "location_delivered": "Austin, TX",
        }

    @patch("utils.easypost.make_close_request")
    def test_check_existing_activities_api_call(self, mock_make_request):
        """Test that check_existing_mailer_delivered_activities makes correct API call."""
        # Setup mocks
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_make_request.return_value = mock_response

        # Call the function
        result = _check_existing_mailer_delivered_activities(self.test_lead_id)

        # Verify API call was made correctly
        expected_url = "https://api.close.com/api/v1/activity/custom/"
        expected_params = {
            "lead_id": self.test_lead_id,
            "custom_activity_type_id": "custom.actitype_3KhBfWgjtVfiGYbczbgOWv",
        }

        mock_make_request.assert_called_once_with(
            "get", expected_url, params=expected_params
        )

        # Should return False when no activities found
        assert result is False

    @patch("utils.easypost.make_close_request")
    def test_activity_matching_logic_no_existing_activities(self, mock_make_request):
        """Test the logic when no existing activities are found."""
        # Setup mocks
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_make_request.return_value = mock_response

        # Call the function
        result = _check_existing_mailer_delivered_activities(self.test_lead_id)

        # Should return False when no activities found
        assert result is False

    @patch("utils.easypost.make_close_request")
    def test_activity_matching_logic_existing_activities_found(self, mock_make_request):
        """Test the logic when existing activities are found."""
        # Setup mocks
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {
                    "id": "activity_123",
                    "custom_activity_type_id": "custom.actitype_3KhBfWgjtVfiGYbczbgOWv",
                    "lead_id": self.test_lead_id,
                }
            ]
        }
        mock_make_request.return_value = mock_response

        # Call the function
        result = _check_existing_mailer_delivered_activities(self.test_lead_id)

        # Should return True when activities found
        assert result is True

    @patch("utils.easypost.make_close_request")
    @patch("utils.easypost.logger")
    def test_check_existing_activities_api_failure_fallback(
        self, mock_logger, mock_make_request
    ):
        """Test that function handles API failure gracefully and falls back to False."""
        # Setup mocks
        mock_make_request.side_effect = requests.exceptions.RequestException(
            "API Error"
        )

        # Call the function
        result = _check_existing_mailer_delivered_activities(self.test_lead_id)

        # Should return False (fail-safe approach) when API call fails
        assert result is False

        # Verify error was logged
        mock_logger.error.assert_called()
