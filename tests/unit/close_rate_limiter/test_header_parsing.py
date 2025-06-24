"""
Unit tests for Close.com rate limit header parsing functionality.

Tests the parse_close_ratelimit_header() function that extracts rate limit
information from Close API response headers.
"""

import pytest
from utils.rate_limiter import parse_close_ratelimit_header


class TestCloseRateLimitHeaderParsing:
    """Test cases for parsing Close.com ratelimit headers."""

    def test_parse_valid_header_format(self):
        """Test parsing a valid Close ratelimit header."""
        header_value = "limit=160; remaining=159; reset=8"
        expected = {"limit": 160, "remaining": 159, "reset": 8}

        result = parse_close_ratelimit_header(header_value)
        assert result == expected

    def test_parse_header_with_different_order(self):
        """Test parsing header with parameters in different order."""
        header_value = "remaining=14; limit=16; reset=1"
        expected = {"limit": 16, "remaining": 14, "reset": 1}

        result = parse_close_ratelimit_header(header_value)
        assert result == expected

    def test_parse_header_with_zero_remaining(self):
        """Test parsing header when remaining tokens is zero."""
        header_value = "limit=240; remaining=0; reset=60"
        expected = {"limit": 240, "remaining": 0, "reset": 60}

        result = parse_close_ratelimit_header(header_value)
        assert result == expected

    def test_parse_header_with_extra_whitespace(self):
        """Test parsing header with extra whitespace around values."""
        header_value = "limit = 160 ; remaining = 159 ; reset = 8"
        expected = {"limit": 160, "remaining": 159, "reset": 8}

        result = parse_close_ratelimit_header(header_value)
        assert result == expected

    def test_parse_malformed_header_missing_values(self):
        """Test parsing malformed header with missing values."""
        header_value = "limit=; remaining=159; reset=8"

        with pytest.raises(ValueError, match="Invalid ratelimit header format"):
            parse_close_ratelimit_header(header_value)

    def test_parse_malformed_header_invalid_format(self):
        """Test parsing header with invalid format."""
        header_value = "invalid header format"

        with pytest.raises(ValueError, match="Invalid ratelimit header format"):
            parse_close_ratelimit_header(header_value)

    def test_parse_header_with_non_numeric_values(self):
        """Test parsing header with non-numeric values."""
        header_value = "limit=abc; remaining=159; reset=8"

        with pytest.raises(ValueError, match="Invalid ratelimit header format"):
            parse_close_ratelimit_header(header_value)

    def test_parse_empty_header(self):
        """Test parsing empty header string."""
        header_value = ""

        with pytest.raises(ValueError, match="Invalid ratelimit header format"):
            parse_close_ratelimit_header(header_value)

    def test_parse_none_header(self):
        """Test parsing None header value."""
        header_value = None

        with pytest.raises(ValueError, match="Invalid ratelimit header format"):
            parse_close_ratelimit_header(header_value)

    def test_parse_header_missing_required_fields(self):
        """Test parsing header missing required fields."""
        header_value = "limit=160; remaining=159"  # Missing reset

        with pytest.raises(ValueError, match="Missing required fields"):
            parse_close_ratelimit_header(header_value)

    def test_parse_header_with_additional_fields(self):
        """Test parsing header with additional unknown fields (should ignore them)."""
        header_value = "limit=160; remaining=159; reset=8; window=60; policy=sliding"
        expected = {"limit": 160, "remaining": 159, "reset": 8}

        result = parse_close_ratelimit_header(header_value)
        assert result == expected

    def test_parse_header_with_float_values(self):
        """Test parsing header with float values (should convert to int)."""
        header_value = "limit=160.0; remaining=159.5; reset=8.2"
        expected = {"limit": 160, "remaining": 159, "reset": 8}

        result = parse_close_ratelimit_header(header_value)
        assert result == expected

    def test_parse_header_case_insensitive(self):
        """Test parsing header with different case (should be case insensitive)."""
        header_value = "LIMIT=160; REMAINING=159; RESET=8"
        expected = {"limit": 160, "remaining": 159, "reset": 8}

        result = parse_close_ratelimit_header(header_value)
        assert result == expected
