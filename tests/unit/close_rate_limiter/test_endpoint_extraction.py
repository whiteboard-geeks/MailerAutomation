"""
Unit tests for Close API endpoint extraction functionality.

Tests the extract_endpoint_key() function that converts Close API URLs
into consistent endpoint keys for rate limiting purposes.

Following TDD approach - these tests will initially fail until the
extract_endpoint_key() function is implemented in utils/rate_limiter.py.
"""

import pytest
from utils.rate_limiter import extract_endpoint_key


class TestExtractEndpointKey:
    """Test cases for extract_endpoint_key() function."""

    def test_root_endpoint_extraction_lead(self):
        """Test that all lead endpoints map to /api/v1/lead/ root."""
        # Base lead endpoint
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/")
            == "/api/v1/lead/"
        )

        # Specific lead endpoints - all should map to root
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_123/")
            == "/api/v1/lead/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_abc456/")
            == "/api/v1/lead/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_xyz789/")
            == "/api/v1/lead/"
        )

        # Lead sub-resources should also map to lead root
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_123/activity/")
            == "/api/v1/lead/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_456/contact/")
            == "/api/v1/lead/"
        )

    def test_root_endpoint_extraction_task(self):
        """Test that all task endpoints map to /api/v1/task/ root."""
        # Base task endpoint
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/task/")
            == "/api/v1/task/"
        )

        # Specific task endpoints
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/task/task_123/")
            == "/api/v1/task/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/task/task_456/")
            == "/api/v1/task/"
        )

    def test_root_endpoint_extraction_contact(self):
        """Test that all contact endpoints map to /api/v1/contact/ root."""
        # Base contact endpoint
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/contact/")
            == "/api/v1/contact/"
        )

        # Specific contact endpoints
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/contact/cont_123/")
            == "/api/v1/contact/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/contact/cont_456/")
            == "/api/v1/contact/"
        )

    def test_root_endpoint_extraction_activity(self):
        """Test that all activity endpoints map to /api/v1/activity/ root."""
        # Base activity endpoint
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/activity/")
            == "/api/v1/activity/"
        )

        # Specific activity endpoints
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/activity/acti_123/")
            == "/api/v1/activity/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/activity/acti_456/")
            == "/api/v1/activity/"
        )

    def test_static_endpoints_unchanged(self):
        """Test that static endpoints without resource IDs remain unchanged."""
        # These endpoints don't have resource IDs, so they stay as-is
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/data/search/")
            == "/api/v1/data/search/"
        )
        assert extract_endpoint_key("https://api.close.com/api/v1/me/") == "/api/v1/me/"
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/status/")
            == "/api/v1/status/"
        )

    def test_url_variations(self):
        """Test handling of different URL variations."""
        # With query parameters (should be ignored)
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/data/search/?limit=10")
            == "/api/v1/data/search/"
        )
        assert (
            extract_endpoint_key(
                "https://api.close.com/api/v1/lead/lead_123/?include=contacts"
            )
            == "/api/v1/lead/"
        )

        # With fragments (should be ignored)
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_123/#section")
            == "/api/v1/lead/"
        )

        # HTTP vs HTTPS (both should work)
        assert extract_endpoint_key("http://api.close.com/api/v1/me/") == "/api/v1/me/"
        assert extract_endpoint_key("https://api.close.com/api/v1/me/") == "/api/v1/me/"

    def test_trailing_slash_normalization(self):
        """Test that trailing slashes are properly normalized."""
        # Missing trailing slash should be added
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/data/search")
            == "/api/v1/data/search/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_123")
            == "/api/v1/lead/"
        )
        assert extract_endpoint_key("https://api.close.com/api/v1/me") == "/api/v1/me/"

    def test_invalid_urls(self):
        """Test error handling for invalid URLs."""
        # None/empty URLs
        with pytest.raises(ValueError, match="Invalid URL"):
            extract_endpoint_key(None)

        with pytest.raises(ValueError, match="Invalid URL"):
            extract_endpoint_key("")

        with pytest.raises(ValueError, match="Invalid URL"):
            extract_endpoint_key("   ")

    def test_non_string_input(self):
        """Test error handling for non-string input."""
        with pytest.raises(ValueError, match="URL must be a string"):
            extract_endpoint_key(123)

        with pytest.raises(ValueError, match="URL must be a string"):
            extract_endpoint_key(["not", "a", "string"])

    def test_malformed_urls(self):
        """Test error handling for malformed URLs."""
        with pytest.raises(ValueError, match="Invalid URL format"):
            extract_endpoint_key("not-a-url")

        with pytest.raises(ValueError, match="Invalid URL format"):
            extract_endpoint_key("ftp://invalid-protocol.com")

    def test_non_close_api_urls(self):
        """Test error handling for non-Close API URLs."""
        with pytest.raises(ValueError, match="Not a Close API URL"):
            extract_endpoint_key("https://api.other.com/api/v1/data/")

        with pytest.raises(ValueError, match="Not a Close API URL"):
            extract_endpoint_key(
                "https://close.com/api/v1/data/"
            )  # Missing api subdomain

        with pytest.raises(ValueError, match="Not a Close API URL"):
            extract_endpoint_key("https://api.close.io/api/v1/data/")  # Wrong domain

    def test_non_api_close_paths(self):
        """Test error handling for Close URLs that aren't API endpoints."""
        with pytest.raises(ValueError, match="Not a Close API endpoint"):
            extract_endpoint_key("https://api.close.com/")

        with pytest.raises(ValueError, match="Not a Close API endpoint"):
            extract_endpoint_key("https://api.close.com/docs/")

        with pytest.raises(ValueError, match="Not a Close API endpoint"):
            extract_endpoint_key("https://api.close.com/api/")  # Missing version

    def test_unsupported_api_versions(self):
        """Test handling of different API versions."""
        # v1 should work
        assert extract_endpoint_key("https://api.close.com/api/v1/me/") == "/api/v1/me/"

        # Other versions should raise error for now
        with pytest.raises(ValueError, match="Unsupported API version"):
            extract_endpoint_key("https://api.close.com/api/v2/me/")

    def test_complex_nested_resources(self):
        """Test handling of complex nested resource URLs."""
        # All should map to the root resource endpoint
        assert (
            extract_endpoint_key(
                "https://api.close.com/api/v1/lead/lead_123/activity/acti_456/"
            )
            == "/api/v1/lead/"
        )
        assert (
            extract_endpoint_key(
                "https://api.close.com/api/v1/contact/cont_123/activity/acti_789/"
            )
            == "/api/v1/contact/"
        )
        assert (
            extract_endpoint_key(
                "https://api.close.com/api/v1/lead/lead_456/contact/cont_789/activity/"
            )
            == "/api/v1/lead/"
        )

    def test_edge_case_resource_patterns(self):
        """Test edge cases with different resource ID patterns."""
        # Different ID formats should all map to root
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_123abc/")
            == "/api/v1/lead/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/lead/lead_456_def/")
            == "/api/v1/lead/"
        )
        assert (
            extract_endpoint_key("https://api.close.com/api/v1/task/task_xyz123/")
            == "/api/v1/task/"
        )

    def test_case_sensitivity(self):
        """Test that URL parsing is case-insensitive where appropriate."""
        # Domain should be case-insensitive
        assert extract_endpoint_key("https://API.CLOSE.COM/api/v1/me/") == "/api/v1/me/"

        # Path should preserve case but still work
        assert extract_endpoint_key("https://api.close.com/API/V1/me/") == "/API/V1/me/"
