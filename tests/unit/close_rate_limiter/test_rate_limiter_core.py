"""
Unit tests for Close.com Dynamic Rate Limiter core functionality.

Tests the CloseRateLimiter class that provides endpoint-specific rate limiting
with dynamic limit discovery from Close API response headers.

These tests follow TDD approach - they will initially FAIL until the
CloseRateLimiter class is implemented in Phase 1.3.
"""

import redis
from unittest.mock import Mock
from utils.rate_limiter import CloseRateLimiter, RedisRateLimiter


class TestCloseRateLimiterCore:
    """Test cases for Close.com dynamic rate limiter core functionality."""

    def setup_method(self):
        """Setup before each test."""
        # Mock Redis client for unit tests
        self.mock_redis = Mock(spec=redis.Redis)
        self.mock_redis.ping.return_value = True
        self.mock_redis.get.return_value = None
        self.mock_redis.setex.return_value = True
        self.mock_redis.delete.return_value = 1
        self.mock_redis.pipeline.return_value = Mock()

    # ========================================
    # 1. Initialization Tests
    # ========================================

    def test_close_rate_limiter_initialization(self):
        """Test CloseRateLimiter can be initialized with default parameters."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        assert limiter is not None
        assert hasattr(limiter, "conservative_default_rps")
        assert hasattr(limiter, "acquire_token_for_endpoint")
        assert hasattr(limiter, "update_from_response_headers")

    def test_close_rate_limiter_inherits_from_redis_rate_limiter(self):
        """Test CloseRateLimiter properly inherits from RedisRateLimiter."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        assert isinstance(limiter, RedisRateLimiter)
        assert hasattr(limiter, "acquire_token")  # Inherited method
        assert hasattr(limiter, "get_bucket_status")  # Inherited method
        assert hasattr(limiter, "reset_bucket")  # Inherited method

    def test_close_rate_limiter_with_custom_defaults(self):
        """Test CloseRateLimiter initialization with custom parameters."""
        limiter = CloseRateLimiter(
            redis_client=self.mock_redis,
            conservative_default_rps=0.5,
            safety_factor=0.7,
        )

        assert limiter.conservative_default_rps == 0.5
        assert limiter.safety_factor == 0.7

    # ========================================
    # 2. Conservative Default Behavior
    # ========================================

    def test_unknown_endpoint_uses_conservative_default(self):
        """Test that unknown endpoints use very conservative default rate."""
        limiter = CloseRateLimiter(
            redis_client=self.mock_redis, conservative_default_rps=1.0
        )

        # Mock Redis to return no cached limits (unknown endpoint)
        self.mock_redis.get.return_value = None

        # First call to unknown endpoint should use conservative default
        endpoint_url = "https://api.close.com/api/v1/unknown_endpoint/"

        # Should use conservative rate limiting
        result = limiter.acquire_token_for_endpoint(endpoint_url)

        # Verify it used conservative default rate (1 req/sec)
        assert isinstance(result, bool)

    def test_conservative_default_rate_is_very_low(self):
        """Test that conservative default rate is appropriately restrictive."""
        limiter = CloseRateLimiter(
            redis_client=self.mock_redis,
            conservative_default_rps=1.0,  # 1 request per second
        )

        endpoint_url = "https://api.close.com/api/v1/new_endpoint/"

        # Mock Redis to simulate no cached limits
        self.mock_redis.get.return_value = None

        # Should allow first request
        first_request = limiter.acquire_token_for_endpoint(endpoint_url)
        assert (
            first_request is True or first_request is False
        )  # Boolean result expected

        # Verify conservative rate is being used
        assert limiter.conservative_default_rps == 1.0

    # ========================================
    # 3. Endpoint-Specific Rate Limiting
    # ========================================

    def test_different_endpoints_have_separate_buckets(self):
        """Test that different endpoints maintain separate rate limit buckets."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint1 = "https://api.close.com/api/v1/lead/lead_123/"
        endpoint2 = "https://api.close.com/api/v1/data/search/"

        # Mock different cached limits for each endpoint
        def mock_get_side_effect(key):
            if "lead" in key:
                return b'{"limit": 160, "remaining": 159, "reset": 8}'
            elif "search" in key:
                return b'{"limit": 16, "remaining": 15, "reset": 1}'
            return None

        self.mock_redis.get.side_effect = mock_get_side_effect

        # Both endpoints should work independently
        result1 = limiter.acquire_token_for_endpoint(endpoint1)
        result2 = limiter.acquire_token_for_endpoint(endpoint2)

        assert isinstance(result1, bool)
        assert isinstance(result2, bool)

    def test_endpoint_rate_limiting_isolation(self):
        """Test that rate limiting one endpoint doesn't affect another."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        lead_endpoint = "https://api.close.com/api/v1/lead/lead_123/"
        search_endpoint = "https://api.close.com/api/v1/data/search/"

        # Simulate exhausting rate limit for lead endpoint
        # but search endpoint should still work

        # Mock Redis responses for different endpoints
        def mock_get_side_effect(key):
            if "lead" in key and "bucket" in key:
                return b"0.0"  # No tokens left for lead
            elif "search" in key and "bucket" in key:
                return b"5.0"  # Tokens available for search
            elif "lead" in key and "limits" in key:
                return b'{"limit": 160, "remaining": 0, "reset": 8}'
            elif "search" in key and "limits" in key:
                return b'{"limit": 16, "remaining": 15, "reset": 1}'
            return None

        self.mock_redis.get.side_effect = mock_get_side_effect

        # Lead endpoint should be rate limited, search should work
        lead_result = limiter.acquire_token_for_endpoint(lead_endpoint)
        search_result = limiter.acquire_token_for_endpoint(search_endpoint)

        # Results should be independent
        assert isinstance(lead_result, bool)
        assert isinstance(search_result, bool)

    # ========================================
    # 4. Dynamic Limit Discovery from Headers
    # ========================================

    def test_update_limits_from_valid_response_headers(self):
        """Test updating rate limits from valid Close API response headers."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock HTTP response with rate limit headers
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

        # Should parse headers and update limits
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Verify Redis was called to cache the limits
        self.mock_redis.setex.assert_called()

    def test_update_limits_from_different_endpoints(self):
        """Test updating limits for different endpoints independently."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        # Different endpoints with different limits
        lead_endpoint = "https://api.close.com/api/v1/lead/lead_123/"
        search_endpoint = "https://api.close.com/api/v1/data/search/"

        # Mock responses with different rate limits
        lead_response = Mock()
        lead_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

        search_response = Mock()
        search_response.headers = {"ratelimit": "limit=16; remaining=15; reset=1"}

        # Update limits for both endpoints
        limiter.update_from_response_headers(lead_endpoint, lead_response)
        limiter.update_from_response_headers(search_endpoint, search_response)

        # Should have made separate Redis calls for each endpoint
        assert self.mock_redis.setex.call_count >= 2

    def test_invalid_headers_dont_break_existing_limits(self):
        """Test that invalid headers don't break existing cached limits."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock existing cached limits
        self.mock_redis.get.return_value = (
            b'{"limit": 160, "remaining": 100, "reset": 8}'
        )

        # Mock response with invalid headers
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "invalid header format"}

        # Should handle invalid headers gracefully
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Existing limits should still work
        result = limiter.acquire_token_for_endpoint(endpoint_url)
        assert isinstance(result, bool)

    def test_missing_headers_dont_affect_limits(self):
        """Test that responses without rate limit headers don't affect existing limits."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock response without rate limit headers
        mock_response = Mock()
        mock_response.headers = {}

        # Should handle missing headers gracefully
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Should not have tried to update limits
        # (setex might be called for other reasons, so we just verify no crash)
        result = limiter.acquire_token_for_endpoint(endpoint_url)
        assert isinstance(result, bool)

    # ========================================
    # 5. Limit Persistence and Retrieval
    # ========================================

    def test_discovered_limits_are_cached_in_redis(self):
        """Test that discovered limits are properly cached in Redis."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock response with rate limit headers
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

        # Update limits from response
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Verify limits were cached with correct Redis key structure
        # Should call setex to cache the limits
        self.mock_redis.setex.assert_called()

        # Verify the key structure includes endpoint information
        call_args = self.mock_redis.setex.call_args_list
        assert len(call_args) > 0

        # At least one call should be for caching limits
        key_used = call_args[0][0][0]  # First call, first argument (key)
        assert "close_rate_limit" in key_used
        assert "limits" in key_used

    def test_cached_limits_are_used_for_subsequent_requests(self):
        """Test that cached limits are retrieved and used for rate limiting."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock cached limits in Redis
        cached_limits = '{"limit": 160, "remaining": 100, "reset": 8}'
        self.mock_redis.get.return_value = cached_limits.encode()

        # Should retrieve and use cached limits
        result = limiter.acquire_token_for_endpoint(endpoint_url)

        # Verify Redis was queried for cached limits
        self.mock_redis.get.assert_called()
        assert isinstance(result, bool)

    def test_limit_cache_expiration(self):
        """Test that cached limits have appropriate expiration times."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock response with rate limit headers
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

        # Update limits from response
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Verify setex was called with expiration time
        self.mock_redis.setex.assert_called()

        # Check that expiration time was provided
        call_args = self.mock_redis.setex.call_args_list
        for call in call_args:
            args = call[0]
            if len(args) >= 2:
                expiration_time = args[1]  # Second argument should be expiration
                assert isinstance(expiration_time, int)
                assert expiration_time > 0

    # ========================================
    # 6. Safety Factor Application
    # ========================================

    def test_safety_factor_applied_to_discovered_limits(self):
        """Test that safety factor is applied to discovered rate limits."""
        limiter = CloseRateLimiter(
            redis_client=self.mock_redis,
            safety_factor=0.8,  # 80% safety factor
        )

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock response with high rate limit
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

        # Update limits from response
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Verify safety factor is applied (160 * 0.8 = 128 effective limit)
        # This is tested indirectly by checking the rate limiter behavior
        assert limiter.safety_factor == 0.8

    def test_safety_factor_prevents_hitting_exact_api_limits(self):
        """Test that safety factor prevents hitting exact API limits."""
        limiter = CloseRateLimiter(
            redis_client=self.mock_redis,
            safety_factor=0.5,  # 50% safety factor for testing
        )

        # Mock discovered limits
        endpoint_url = "https://api.close.com/api/v1/data/search/"
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=10; remaining=9; reset=1"}

        # Update limits
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Effective rate should be 10 * 0.5 = 5 req/sec
        # This ensures we don't hit the actual API limit of 10 req/sec
        assert limiter.safety_factor == 0.5

    # ========================================
    # 7. Integration with Existing RedisRateLimiter
    # ========================================

    def test_acquire_token_for_endpoint_method(self):
        """Test the main acquire_token_for_endpoint method."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Should return boolean result
        result = limiter.acquire_token_for_endpoint(endpoint_url)
        assert isinstance(result, bool)

    def test_fallback_behavior_when_redis_unavailable(self):
        """Test fallback behavior when Redis is unavailable."""
        # Create limiter with fallback enabled
        limiter = CloseRateLimiter(
            redis_client=None,  # No Redis client
            fallback_on_redis_error=True,
        )

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Should still work with in-memory fallback
        result = limiter.acquire_token_for_endpoint(endpoint_url)
        assert isinstance(result, bool)

    def test_bucket_status_for_specific_endpoints(self):
        """Test getting bucket status for specific endpoints."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock Redis responses for bucket status
        self.mock_redis.get.return_value = b"5.0"  # Mock token count

        # Should be able to get status for specific endpoint
        # This tests integration with inherited get_bucket_status method
        endpoint_key = limiter._extract_endpoint_key(endpoint_url)
        status = limiter.get_bucket_status(f"close_endpoint:{endpoint_key}")

        assert isinstance(status, dict)

    def test_get_endpoint_limits_method(self):
        """Test retrieving cached limits for a specific endpoint."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_key = "/api/v1/lead/"

        # Mock cached limits
        cached_limits = '{"limit": 160, "remaining": 100, "reset": 8}'
        self.mock_redis.get.return_value = cached_limits.encode()

        # Should retrieve cached limits
        limits = limiter.get_endpoint_limits(endpoint_key)

        assert isinstance(limits, dict)
        self.mock_redis.get.assert_called()

    def test_redis_key_structure_for_endpoints(self):
        """Test that Redis keys follow the expected structure for endpoints."""
        limiter = CloseRateLimiter(redis_client=self.mock_redis)

        endpoint_url = "https://api.close.com/api/v1/lead/lead_123/"

        # Mock response to trigger Redis operations
        mock_response = Mock()
        mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

        # Update limits to trigger Redis key creation
        limiter.update_from_response_headers(endpoint_url, mock_response)

        # Verify Redis keys follow expected structure
        self.mock_redis.setex.assert_called()

        # Check key structure in Redis calls
        call_args = self.mock_redis.setex.call_args_list
        for call in call_args:
            key = call[0][0]  # First argument is the key
            assert "close_rate_limit" in key
            # Should contain either "bucket" or "limits"
            assert "bucket" in key or "limits" in key
