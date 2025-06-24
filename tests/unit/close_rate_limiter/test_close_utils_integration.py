"""
Unit tests for Close rate limiter integration with close_utils.py

Tests the integration between the CloseRateLimiter and the close_utils functions,
ensuring that:
1. Rate limiting is applied before requests
2. Headers are parsed after responses
3. Limits are updated correctly
4. Existing functionality remains unchanged
5. Backward compatibility is maintained
"""

import pytest
from unittest.mock import Mock, patch
import requests

# Import the functions we're testing
from close_utils import (
    make_close_request,
    get_close_rate_limiter,
    close_rate_limit,
    search_close_leads,
    get_lead_by_id,
)


class TestCloseUtilsIntegration:
    """Test integration between Close rate limiter and close_utils functions."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset the global rate limiter for each test
        import close_utils

        close_utils._close_rate_limiter = None

    @patch("redis.from_url")
    def test_get_close_rate_limiter_with_redis(self, mock_redis_from_url):
        """Test rate limiter initialization with Redis."""
        # Mock Redis client
        mock_redis_client = Mock()
        mock_redis_client.ping.return_value = True
        mock_redis_from_url.return_value = mock_redis_client

        # Get rate limiter
        rate_limiter = get_close_rate_limiter()

        # Verify initialization
        assert rate_limiter is not None
        assert rate_limiter.redis_client == mock_redis_client
        assert rate_limiter.conservative_default_rps == 1.0
        assert rate_limiter.safety_factor == 0.8
        assert rate_limiter.cache_expiration_seconds == 3600

        # Verify Redis connection was attempted
        mock_redis_from_url.assert_called_once()
        mock_redis_client.ping.assert_called_once()

    @patch("redis.Redis")
    @patch("redis.from_url")
    def test_get_close_rate_limiter_redis_fallback(
        self, mock_redis_from_url, mock_redis_class
    ):
        """Test rate limiter fallback when Redis is unavailable."""
        # Mock Redis connection failures for both from_url and Redis class
        mock_redis_from_url.side_effect = Exception("Redis connection failed")
        mock_redis_class.side_effect = Exception("Default Redis connection failed")

        # Get rate limiter
        rate_limiter = get_close_rate_limiter()

        # Verify fallback initialization
        assert rate_limiter is not None
        assert rate_limiter.redis_client is None
        assert rate_limiter.conservative_default_rps == 1.0
        assert rate_limiter.safety_factor == 0.8

    def test_get_close_rate_limiter_singleton(self):
        """Test that rate limiter is a singleton."""
        rate_limiter1 = get_close_rate_limiter()
        rate_limiter2 = get_close_rate_limiter()

        # Should be the same instance
        assert rate_limiter1 is rate_limiter2

    @patch("close_utils.get_close_rate_limiter")
    @patch("requests.request")
    def test_close_rate_limit_decorator_applies_rate_limiting(
        self, mock_request, mock_get_limiter
    ):
        """Test that the decorator applies rate limiting before requests."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire_token_for_endpoint.return_value = True
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock successful response
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}
        mock_request.return_value = mock_response

        # Create a test function with the decorator
        @close_rate_limit()
        def test_function(method, url, **kwargs):
            return requests.request(method, url, **kwargs)

        # Call the function
        url = "https://api.close.com/api/v1/me/"
        result = test_function("GET", url)

        # Verify rate limiting was applied
        mock_rate_limiter.acquire_token_for_endpoint.assert_called_once_with(url)

        # Verify header parsing was called
        mock_rate_limiter.update_from_response_headers.assert_called_once_with(
            url, mock_response
        )

        # Verify request was made
        mock_request.assert_called_once_with("GET", url)
        assert result == mock_response

    @patch("close_utils.get_close_rate_limiter")
    @patch("requests.request")
    def test_close_rate_limit_decorator_handles_rate_limit_denial(
        self, mock_request, mock_get_limiter
    ):
        """Test decorator behavior when rate limit is exceeded."""
        # Mock rate limiter that denies tokens
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire_token_for_endpoint.return_value = False
        mock_get_limiter.return_value = mock_rate_limiter

        # Create a test function with the decorator
        @close_rate_limit(max_retries=1, initial_delay=0.01)
        def test_function(method, url, **kwargs):
            return requests.request(method, url, **kwargs)

        # Call the function and expect rate limit exception
        url = "https://api.close.com/api/v1/me/"
        with pytest.raises(
            requests.exceptions.RequestException, match="Rate limit exceeded"
        ):
            test_function("GET", url)

        # Verify rate limiting was attempted multiple times
        assert (
            mock_rate_limiter.acquire_token_for_endpoint.call_count == 2
        )  # Initial + 1 retry

        # Verify no actual request was made
        mock_request.assert_not_called()

    @patch("close_utils.get_close_rate_limiter")
    @patch("requests.request")
    def test_close_rate_limit_decorator_non_close_url(
        self, mock_request, mock_get_limiter
    ):
        """Test decorator behavior with non-Close URLs."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock successful response
        mock_response = Mock()
        mock_request.return_value = mock_response

        # Create a test function with the decorator
        @close_rate_limit()
        def test_function(method, url, **kwargs):
            return requests.request(method, url, **kwargs)

        # Call with non-Close URL
        url = "https://api.example.com/test"
        result = test_function("GET", url)

        # Verify rate limiting was NOT applied for non-Close URLs
        mock_rate_limiter.acquire_token_for_endpoint.assert_not_called()
        mock_rate_limiter.update_from_response_headers.assert_not_called()

        # Verify request was still made
        mock_request.assert_called_once_with("GET", url)
        assert result == mock_response

    @patch("close_utils.get_close_rate_limiter")
    @patch("requests.request")
    def test_close_rate_limit_decorator_retry_logic(
        self, mock_request, mock_get_limiter
    ):
        """Test decorator retry logic for non-rate-limit errors."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire_token_for_endpoint.return_value = True
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock request that fails then succeeds
        success_response = Mock(headers={})
        mock_request.side_effect = [
            requests.exceptions.ConnectionError("Connection failed"),
            success_response,
        ]

        # Create a test function with the decorator
        @close_rate_limit(max_retries=2, initial_delay=0.01)
        def test_function(method, url, **kwargs):
            return requests.request(method, url, **kwargs)

        # Call the function
        url = "https://api.close.com/api/v1/me/"
        result = test_function("GET", url)

        # Verify retry logic worked
        assert mock_request.call_count == 2
        assert result == success_response

    @patch("close_utils.get_close_rate_limiter")
    @patch("requests.request")
    def test_close_rate_limit_decorator_no_retry_on_4xx(
        self, mock_request, mock_get_limiter
    ):
        """Test decorator doesn't retry on 4xx errors (except 429)."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire_token_for_endpoint.return_value = True
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock 404 error
        mock_response = Mock()
        mock_response.status_code = 404
        mock_error = requests.exceptions.HTTPError("404 Not Found")
        mock_error.response = mock_response
        mock_request.side_effect = mock_error

        # Create a test function with the decorator
        @close_rate_limit(max_retries=2, initial_delay=0.01)
        def test_function(method, url, **kwargs):
            return requests.request(method, url, **kwargs)

        # Call the function and expect immediate failure
        url = "https://api.close.com/api/v1/me/"
        with pytest.raises(requests.exceptions.HTTPError):
            test_function("GET", url)

        # Verify no retries for 4xx errors
        assert mock_request.call_count == 1

    @patch("close_utils.make_close_request")
    def test_make_close_request_integration(self, mock_make_request):
        """Test that make_close_request uses the new decorator."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"id": "lead_123"}
        mock_make_request.return_value = mock_response

        # Call a function that uses make_close_request
        result = get_lead_by_id("lead_123")

        # Verify the function was called
        mock_make_request.assert_called_once_with(
            "get", "https://api.close.com/api/v1/lead/lead_123/", timeout=30
        )

        # Verify result
        assert result == {"id": "lead_123"}

    @patch("close_utils.make_close_request")
    def test_search_close_leads_integration(self, mock_make_request):
        """Test search_close_leads integration with rate limiting."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"data": [{"id": "lead_123"}], "cursor": None}
        mock_make_request.return_value = mock_response

        # Test query
        query = {"query": {"queries": []}}

        # Call the function
        result = search_close_leads(query)

        # Verify the function was called with rate limiting
        mock_make_request.assert_called_once_with(
            "post", "https://api.close.com/api/v1/data/search/", json=query, timeout=30
        )

        # Verify result
        assert result == [{"id": "lead_123"}]

    @patch("close_utils.get_close_rate_limiter")
    def test_rate_limiter_header_parsing_integration(self, mock_get_limiter):
        """Test that response headers are parsed and cached."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire_token_for_endpoint.return_value = True
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock response with rate limit headers
        with patch("requests.request") as mock_request:
            mock_response = Mock()
            mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            # Call make_close_request
            url = "https://api.close.com/api/v1/me/"
            result = make_close_request("GET", url)

            # Verify header parsing was called
            mock_rate_limiter.update_from_response_headers.assert_called_once_with(
                url, mock_response
            )

            # Verify result
            assert result == mock_response

    def test_backward_compatibility(self):
        """Test that existing functionality remains unchanged."""
        # Test that all original functions still exist and are callable
        from close_utils import (
            load_query,
            retry_with_backoff,
            get_close_headers,
            create_email_search_query,
            search_close_leads,
            get_lead_by_id,
            get_lead_email_activities,
            get_task,
            create_task,
            get_sequence_subscriptions,
            pause_sequence_subscription,
        )

        # Verify functions are callable
        assert callable(load_query)
        assert callable(retry_with_backoff)
        assert callable(get_close_headers)
        assert callable(create_email_search_query)
        assert callable(search_close_leads)
        assert callable(get_lead_by_id)
        assert callable(get_lead_email_activities)
        assert callable(get_task)
        assert callable(create_task)
        assert callable(get_sequence_subscriptions)
        assert callable(pause_sequence_subscription)

        # Test that get_close_headers still works
        headers = get_close_headers()
        assert "Content-Type" in headers
        assert "Authorization" in headers

    @patch("close_utils.get_close_rate_limiter")
    def test_decorator_url_extraction_from_kwargs(self, mock_get_limiter):
        """Test that decorator can extract URL from kwargs."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire_token_for_endpoint.return_value = True
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock response
        with patch("requests.request") as mock_request:
            mock_response = Mock()
            mock_response.headers = {}
            mock_request.return_value = mock_response

            # Create a test function with the decorator
            @close_rate_limit()
            def test_function(method, **kwargs):
                return requests.request(method, **kwargs)

            # Call with URL in kwargs
            url = "https://api.close.com/api/v1/me/"
            result = test_function("GET", url=url)

            # Verify rate limiting was applied
            mock_rate_limiter.acquire_token_for_endpoint.assert_called_once_with(url)

            # Verify result
            assert result == mock_response

    @patch("close_utils.get_close_rate_limiter")
    def test_decorator_handles_missing_url(self, mock_get_limiter):
        """Test decorator behavior when URL cannot be extracted."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock response
        with patch("requests.request") as mock_request:
            mock_response = Mock()
            mock_request.return_value = mock_response

            # Create a test function with the decorator
            @close_rate_limit()
            def test_function(method):
                return requests.request(method, "http://example.com")

            # Call without URL parameter
            result = test_function("GET")

            # Verify rate limiting was NOT applied (no URL to extract)
            mock_rate_limiter.acquire_token_for_endpoint.assert_not_called()
            mock_rate_limiter.update_from_response_headers.assert_not_called()

            # Verify request was still made
            mock_request.assert_called_once()
            assert result == mock_response

    @patch("close_utils.get_close_rate_limiter")
    def test_decorator_handles_response_without_headers(self, mock_get_limiter):
        """Test decorator behavior when response has no headers."""
        # Mock rate limiter
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire_token_for_endpoint.return_value = True
        mock_get_limiter.return_value = mock_rate_limiter

        # Mock response without headers attribute
        with patch("requests.request") as mock_request:
            mock_response = Mock(spec=[])  # No headers attribute
            mock_request.return_value = mock_response

            # Create a test function with the decorator
            @close_rate_limit()
            def test_function(method, url, **kwargs):
                return requests.request(method, url, **kwargs)

            # Call the function
            url = "https://api.close.com/api/v1/me/"
            result = test_function("GET", url)

            # Verify rate limiting was applied
            mock_rate_limiter.acquire_token_for_endpoint.assert_called_once_with(url)

            # Verify header parsing was NOT called (no headers)
            mock_rate_limiter.update_from_response_headers.assert_not_called()

            # Verify result
            assert result == mock_response
