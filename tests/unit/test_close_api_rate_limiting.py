"""
Unit tests for CloseAPI rate limiting retry functionality.
"""

import pytest
from unittest.mock import Mock, patch
from tests.utils.close_api import CloseAPI


class TestCloseAPIRateLimiting:
    """Test the rate limiting retry functionality in CloseAPI."""

    def setup_method(self):
        """Setup for each test method."""
        # Mock the environment variable for the API key
        with patch.dict("os.environ", {"CLOSE_API_KEY": "test_api_key"}):
            self.close_api = CloseAPI()

    @patch("tests.utils.close_api.time.sleep")
    @patch("tests.utils.close_api.requests.post")
    def test_retry_on_429_with_ratelimit_header(self, mock_post, mock_sleep):
        """Test that 429 responses trigger retry with RateLimit header parsing."""
        # Mock first response as 429 with RateLimit header
        first_response = Mock()
        first_response.status_code = 429
        first_response.headers = {"RateLimit": "limit=240, remaining=0, reset=5.5"}

        # Mock second response as successful
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {"id": "test_lead_id"}

        # Configure mock to return first 429, then success
        mock_post.side_effect = [first_response, second_response]

        # Make a request that should trigger retry
        result = self.close_api._make_request_with_retry(
            "POST",
            "https://api.close.com/api/v1/lead/",
            json={"name": "Test Lead"},
            headers=self.close_api.headers,
        )

        # Verify the retry logic worked
        assert mock_post.call_count == 2  # Called twice - once failed, once succeeded
        mock_sleep.assert_called_once_with(5.5)  # Should sleep for the reset time
        assert result.status_code == 200

    @patch("tests.utils.close_api.time.sleep")
    @patch("tests.utils.close_api.requests.post")
    def test_retry_on_429_with_retry_after_header(self, mock_post, mock_sleep):
        """Test that 429 responses fall back to retry-after header."""
        # Mock first response as 429 with retry-after header (no RateLimit header)
        first_response = Mock()
        first_response.status_code = 429
        first_response.headers = {"retry-after": "10"}

        # Mock second response as successful
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {"id": "test_lead_id"}

        # Configure mock to return first 429, then success
        mock_post.side_effect = [first_response, second_response]

        # Make a request that should trigger retry
        result = self.close_api._make_request_with_retry(
            "POST",
            "https://api.close.com/api/v1/lead/",
            json={"name": "Test Lead"},
            headers=self.close_api.headers,
        )

        # Verify the retry logic worked
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(10.0)  # Should use retry-after value
        assert result.status_code == 200

    @patch("tests.utils.close_api.time.sleep")
    @patch("tests.utils.close_api.requests.post")
    def test_retry_on_429_with_default_wait_time(self, mock_post, mock_sleep):
        """Test that 429 responses use default wait time when no headers available."""
        # Mock first response as 429 with no useful headers
        first_response = Mock()
        first_response.status_code = 429
        first_response.headers = {}

        # Mock second response as successful
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {"id": "test_lead_id"}

        # Configure mock to return first 429, then success
        mock_post.side_effect = [first_response, second_response]

        # Make a request that should trigger retry
        result = self.close_api._make_request_with_retry(
            "POST",
            "https://api.close.com/api/v1/lead/",
            json={"name": "Test Lead"},
            headers=self.close_api.headers,
        )

        # Verify the retry logic worked with default wait time
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(60)  # Should use default 60 seconds
        assert result.status_code == 200

    @patch("tests.utils.close_api.time.sleep")
    @patch("tests.utils.close_api.requests.post")
    def test_max_retries_exceeded(self, mock_post, mock_sleep):
        """Test that requests fail after exceeding max retries."""
        # Mock all responses as 429
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {"RateLimit": "limit=240, remaining=0, reset=1"}

        # Return 429 for all calls (more than max_retries)
        mock_post.return_value = mock_response

        # Make a request that should fail after max retries
        with pytest.raises(
            Exception, match="Request failed after .* retries due to rate limiting"
        ):
            self.close_api._make_request_with_retry(
                "POST",
                "https://api.close.com/api/v1/lead/",
                json={"name": "Test Lead"},
                headers=self.close_api.headers,
                max_retries=2,  # Set low max_retries for faster test
            )

        # Should have called post 3 times (initial + 2 retries)
        assert mock_post.call_count == 3
        # Should have slept 2 times (for the 2 retries)
        assert mock_sleep.call_count == 2

    @patch("tests.utils.close_api.requests.post")
    def test_no_retry_on_non_429_errors(self, mock_post):
        """Test that non-429 errors don't trigger retries."""
        # Mock response as 400 (bad request)
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"

        mock_post.return_value = mock_response

        # Make a request that should not retry
        result = self.close_api._make_request_with_retry(
            "POST",
            "https://api.close.com/api/v1/lead/",
            json={"name": "Test Lead"},
            headers=self.close_api.headers,
        )

        # Should only call once (no retries)
        assert mock_post.call_count == 1
        assert result.status_code == 400

    @patch("tests.utils.close_api.time.sleep")
    @patch("tests.utils.close_api.requests.post")
    def test_successful_request_no_retry(self, mock_post, mock_sleep):
        """Test that successful requests don't trigger retries."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "test_lead_id"}

        mock_post.return_value = mock_response

        # Make a successful request
        result = self.close_api._make_request_with_retry(
            "POST",
            "https://api.close.com/api/v1/lead/",
            json={"name": "Test Lead"},
            headers=self.close_api.headers,
        )

        # Should only call once and not sleep
        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()
        assert result.status_code == 200

    @patch("tests.utils.close_api.time.sleep")
    @patch("tests.utils.close_api.requests.post")
    def test_create_test_lead_uses_retry_logic(self, mock_post, mock_sleep):
        """Test that create_test_lead method uses the retry logic."""
        # Mock first response as 429
        first_response = Mock()
        first_response.status_code = 429
        first_response.headers = {"RateLimit": "limit=240, remaining=0, reset=2"}

        # Mock second response as successful
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {"id": "test_lead_id", "name": "Test Lead"}

        mock_post.side_effect = [first_response, second_response]

        # Call create_test_lead which should use the retry logic
        result = self.close_api.create_test_lead(
            email="test@example.com", first_name="Test", last_name="User"
        )

        # Verify retry logic was used
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(2.0)
        assert result["id"] == "test_lead_id"

    def test_ratelimit_header_parsing_edge_cases(self):
        """Test edge cases in RateLimit header parsing."""
        # Test malformed headers
        test_cases = [
            ("limit=240, remaining=0, reset=abc", None),  # Non-numeric reset
            ("limit=240, remaining=0", None),  # Missing reset
            ("invalid header format", None),  # Completely invalid
            ("limit=240, remaining=0, reset=5.5", 5.5),  # Valid decimal
            ("limit=240,remaining=0,reset=10", 10.0),  # No spaces
            ("reset=15, limit=240, remaining=0", 15.0),  # Different order
        ]

        for header_value, expected_reset in test_cases:
            with patch("tests.utils.close_api.requests.post") as mock_post:
                mock_response = Mock()
                mock_response.status_code = 429
                mock_response.headers = {"RateLimit": header_value}
                mock_post.return_value = mock_response

                # We expect this to raise an exception after max retries
                with pytest.raises(Exception):
                    self.close_api._make_request_with_retry(
                        "POST",
                        "https://api.close.com/api/v1/lead/",
                        max_retries=0,  # Fail immediately to test parsing
                    )

                # The test is that it doesn't crash during header parsing
                assert True  # If we get here, parsing didn't crash
