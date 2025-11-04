"""
Unit tests for duplicate mailer delivered custom activity prevention.
"""

from unittest.mock import Mock, patch
from datetime import datetime
import requests
from blueprints.easypost import (
    create_package_delivered_custom_activity_in_close,
)

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

    @patch("utils.easypost._check_existing_mailer_delivered_activities")
    @patch("utils.easypost.make_close_request")
    def test_create_activity_when_none_exists(
        self, mock_make_request, mock_check_existing
    ):
        """Test that activity is created when no existing activity is found."""
        # Setup mocks
        mock_check_existing.return_value = False  # No existing activities
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "new_activity_123"}
        mock_make_request.return_value = mock_response

        # Call the function
        result = create_package_delivered_custom_activity_in_close(
            self.test_lead_id, self.test_delivery_information
        )

        # Verify that check for existing activities was called
        mock_check_existing.assert_called_once_with(self.test_lead_id)

        # Verify that POST request was made to create activity
        mock_make_request.assert_called_once()
        # Verify it was a POST request
        assert mock_make_request.call_args[0][0] == "post"

        # Verify the result
        assert result == {"id": "new_activity_123"}

    @patch("utils.easypost._check_existing_mailer_delivered_activities")
    @patch("utils.easypost.make_close_request")
    @patch("utils.easypost.logger")
    def test_skip_activity_when_duplicate_exists(
        self, mock_logger, mock_make_request, mock_check_existing
    ):
        """Test that activity is NOT created when duplicate exists."""
        # Setup mocks
        mock_check_existing.return_value = True  # Existing activity found

        # Call the function
        result = create_package_delivered_custom_activity_in_close(
            self.test_lead_id, self.test_delivery_information
        )

        # Verify that check for existing activities was called
        mock_check_existing.assert_called_once_with(self.test_lead_id)

        # Verify that NO POST request was made (activity not created)
        mock_make_request.assert_not_called()

        # Verify that appropriate log message was written
        mock_logger.info.assert_called_with(
            f"Mailer delivered custom activity already exists for lead {self.test_lead_id}, skipping creation"
        )

        # Verify the result indicates skipping
        assert result == {"status": "skipped", "reason": "duplicate_activity_exists"}

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
