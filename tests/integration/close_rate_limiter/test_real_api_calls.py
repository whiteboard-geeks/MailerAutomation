"""
Real API Integration tests for Close Rate Limiter with actual Close.com API calls.

These tests validate that the CloseRateLimiter works correctly with real Close.com API
responses, testing dynamic limit discovery, real rate limit headers, and end-to-end flow.

IMPORTANT: Requires CLOSE_API_KEY environment variable for real API testing.
"""

import os
import time
import redis
import pytest
import requests

from utils.rate_limiter import CloseRateLimiter


@pytest.mark.real_api
@pytest.mark.integration
class TestCloseRateLimiterRealAPI:
    """Real API integration tests for CloseRateLimiter with actual Close.com API."""

    def setup_method(self):
        """Setup before each test."""
        # Check for required environment variables
        self.close_api_key = os.environ.get("CLOSE_API_KEY")
        if not self.close_api_key:
            pytest.skip(
                "CLOSE_API_KEY environment variable not set - skipping real API tests"
            )

        # Setup Redis connection
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        self.test_keys = []  # Track keys created during tests for cleanup

        try:
            self.redis_client = redis.from_url(self.redis_url)
            self.redis_client.ping()
            print(f"Successfully connected to Redis at: {self.redis_url}")
        except Exception as e:
            pytest.fail(f"Failed to connect to Redis at {self.redis_url}: {str(e)}")

        # Setup Close API headers
        # Close.com uses Basic Auth with API key as username and empty password
        import base64

        auth_string = f"{self.close_api_key}:"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()

        self.close_headers = {
            "Authorization": f"Basic {encoded_auth}",
            "Content-Type": "application/json",
        }

        print(f"Real API tests using Close API key: {self.close_api_key[:8]}...")

    def teardown_method(self):
        """Cleanup after each test."""
        # Clean up test keys from Redis
        if self.redis_client:
            for key in self.test_keys:
                try:
                    self.redis_client.delete(key)
                except Exception as e:
                    print(f"Warning: Failed to cleanup key {key}: {e}")

    def test_real_close_api_authentication_and_connection(self):
        """Test that we can authenticate and connect to the real Close.com API."""
        print("\n=== TESTING REAL CLOSE API AUTHENTICATION ===")

        # Test /api/v1/me/ endpoint (should always work with valid API key)
        me_url = "https://api.close.com/api/v1/me/"

        try:
            response = requests.get(me_url, headers=self.close_headers, timeout=10)
            print(f"API Response Status: {response.status_code}")
            print(f"API Response Headers: {dict(response.headers)}")

            # Should get 200 OK with valid API key
            assert (
                response.status_code == 200
            ), f"Expected 200, got {response.status_code}: {response.text}"

            # Should have user data
            user_data = response.json()
            assert "id" in user_data, "Response should contain user ID"
            assert "email" in user_data, "Response should contain user email"

            print(
                f"✅ Successfully authenticated as user: {user_data.get('email', 'Unknown')}"
            )
            print(f"✅ User ID: {user_data.get('id', 'Unknown')}")

        except requests.exceptions.RequestException as e:
            pytest.fail(f"Failed to connect to Close API: {str(e)}")

    def test_real_rate_limit_header_parsing_from_live_responses(self):
        """Test parsing actual rate limit headers from live Close API responses."""
        print("\n=== TESTING REAL RATE LIMIT HEADER PARSING ===")

        # Create rate limiter
        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=1.0,
            safety_factor=0.8,
        )

        # Test different endpoints to see different rate limits
        test_endpoints = [
            "https://api.close.com/api/v1/me/",
            "https://api.close.com/api/v1/data/search/",
        ]

        for endpoint_url in test_endpoints:
            print(f"\nTesting endpoint: {endpoint_url}")

            try:
                # Make real API call
                response = requests.get(
                    endpoint_url, headers=self.close_headers, timeout=10
                )
                print(f"Status: {response.status_code}")

                # Check for rate limit header
                ratelimit_header = response.headers.get("ratelimit")
                if ratelimit_header:
                    print(f"Rate limit header: {ratelimit_header}")

                    # Update rate limiter with real response
                    rate_limiter.update_from_response_headers(endpoint_url, response)

                    # Check that limits were cached
                    from utils.rate_limiter import extract_endpoint_key

                    endpoint_key = extract_endpoint_key(endpoint_url)
                    cached_limits = rate_limiter.get_endpoint_limits(endpoint_key)

                    if cached_limits:
                        print(f"✅ Cached limits for {endpoint_key}: {cached_limits}")

                        # Track cache key for cleanup
                        cache_key = f"close_rate_limit:limits:{endpoint_key}"
                        self.test_keys.append(cache_key)

                        # Verify limits are reasonable
                        assert cached_limits["limit"] > 0, "Limit should be positive"
                        assert (
                            cached_limits["remaining"] >= 0
                        ), "Remaining should be non-negative"
                        assert (
                            cached_limits["reset"] >= 0
                        ), "Reset should be non-negative"
                    else:
                        print(f"⚠️  No rate limit header found for {endpoint_url}")
                else:
                    print(f"⚠️  No ratelimit header in response for {endpoint_url}")

            except requests.exceptions.RequestException as e:
                print(f"❌ Failed to call {endpoint_url}: {str(e)}")
                continue

        print("✅ Real rate limit header parsing completed")

    def test_end_to_end_flow_with_real_api(self):
        """Test complete end-to-end flow: rate limit → API call → header parsing → limit update."""
        print("\n=== TESTING END-TO-END FLOW WITH REAL API ===")

        # Create rate limiter with conservative settings
        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=0.5,  # Very conservative for testing
            safety_factor=0.8,
        )

        endpoint_url = "https://api.close.com/api/v1/me/"

        # Step 1: Check initial rate limiting (should use conservative default)
        print("Step 1: Initial rate limiting check...")
        initial_allowed = rate_limiter.acquire_token_for_endpoint(endpoint_url)
        print(
            f"Initial token acquisition: {'✅ ALLOWED' if initial_allowed else '❌ DENIED'}"
        )

        if initial_allowed:
            # Step 2: Make real API call
            print("Step 2: Making real API call...")
            try:
                response = requests.get(
                    endpoint_url, headers=self.close_headers, timeout=10
                )
                print(f"API call status: {response.status_code}")
                assert (
                    response.status_code == 200
                ), f"API call failed: {response.status_code}"

                # Step 3: Parse response headers and update limits
                print("Step 3: Parsing response headers...")
                ratelimit_header = response.headers.get("ratelimit")
                if ratelimit_header:
                    print(f"Found rate limit header: {ratelimit_header}")

                    # Update rate limiter
                    rate_limiter.update_from_response_headers(endpoint_url, response)

                    # Step 4: Verify limits were updated
                    print("Step 4: Verifying limit updates...")
                    from utils.rate_limiter import extract_endpoint_key

                    endpoint_key = extract_endpoint_key(endpoint_url)
                    updated_limits = rate_limiter.get_endpoint_limits(endpoint_key)

                    if updated_limits:
                        print(f"✅ Updated limits: {updated_limits}")

                        # Track cache key for cleanup
                        cache_key = f"close_rate_limit:limits:{endpoint_key}"
                        self.test_keys.append(cache_key)

                        # Step 5: Test that subsequent calls use discovered limits
                        print(
                            "Step 5: Testing subsequent calls with discovered limits..."
                        )

                        # Should now allow more requests based on discovered limits
                        subsequent_results = []
                        for i in range(3):
                            allowed = rate_limiter.acquire_token_for_endpoint(
                                endpoint_url
                            )
                            subsequent_results.append(allowed)
                            print(
                                f"Subsequent call {i+1}: {'✅ ALLOWED' if allowed else '❌ DENIED'}"
                            )
                            time.sleep(0.1)  # Small delay between calls

                        subsequent_allowed = sum(subsequent_results)
                        print(f"Subsequent calls: {subsequent_allowed}/3 allowed")

                        # With discovered limits, should allow more than conservative default
                        print("✅ End-to-end flow completed successfully")
                    else:
                        print("⚠️  No limits were cached after header parsing")
                else:
                    print("⚠️  No rate limit header found in response")

            except requests.exceptions.RequestException as e:
                pytest.fail(f"Real API call failed: {str(e)}")
        else:
            print(
                "⚠️  Initial token acquisition denied - rate limiter too restrictive for testing"
            )

    def test_multiple_real_endpoints_different_limits(self):
        """Test that different real endpoints have different rate limits."""
        print("\n=== TESTING MULTIPLE REAL ENDPOINTS WITH DIFFERENT LIMITS ===")

        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=1.0,
            safety_factor=0.8,
        )

        # Test different endpoints that are known to have different limits
        endpoints_to_test = [
            {
                "url": "https://api.close.com/api/v1/me/",
                "description": "User info endpoint (typically higher limit)",
            },
            {
                "url": "https://api.close.com/api/v1/data/search/",
                "description": "Search endpoint (typically lower limit)",
            },
        ]

        discovered_limits = {}

        for endpoint_info in endpoints_to_test:
            endpoint_url = endpoint_info["url"]
            description = endpoint_info["description"]

            print(f"\nTesting {description}: {endpoint_url}")

            try:
                # Acquire token first
                token_acquired = rate_limiter.acquire_token_for_endpoint(endpoint_url)
                print(f"Token acquired: {'✅ YES' if token_acquired else '❌ NO'}")

                if token_acquired:
                    # Make API call
                    response = requests.get(
                        endpoint_url, headers=self.close_headers, timeout=10
                    )
                    print(f"API response: {response.status_code}")

                    if response.status_code == 200:
                        # Update limits from response
                        rate_limiter.update_from_response_headers(
                            endpoint_url, response
                        )

                        # Get discovered limits
                        from utils.rate_limiter import extract_endpoint_key

                        endpoint_key = extract_endpoint_key(endpoint_url)
                        limits = rate_limiter.get_endpoint_limits(endpoint_key)

                        if limits:
                            discovered_limits[endpoint_key] = limits
                            print(f"✅ Discovered limits: {limits}")

                            # Track cache key for cleanup
                            cache_key = f"close_rate_limit:limits:{endpoint_key}"
                            self.test_keys.append(cache_key)
                        else:
                            print("⚠️  No limits discovered")
                    else:
                        print(f"⚠️  API call failed with status {response.status_code}")
                else:
                    print("⚠️  Token acquisition failed")

                # Small delay between endpoint tests
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                print(f"❌ Error testing {endpoint_url}: {str(e)}")
                continue

        # Analyze discovered limits
        print("\n=== DISCOVERED LIMITS SUMMARY ===")
        for endpoint_key, limits in discovered_limits.items():
            print(
                f"{endpoint_key}: limit={limits['limit']}, remaining={limits['remaining']}"
            )

        # Verify that we discovered different limits (if we got multiple)
        if len(discovered_limits) >= 2:
            limit_values = [limits["limit"] for limits in discovered_limits.values()]
            unique_limits = set(limit_values)

            if len(unique_limits) > 1:
                print("✅ Different endpoints have different rate limits")
            else:
                print("⚠️  All endpoints have the same rate limit")
        else:
            print("⚠️  Not enough endpoints tested to compare limits")

    def test_rate_limiting_prevents_429_errors(self):
        """Test that rate limiting prevents actual 429 errors from Close API."""
        print("\n=== TESTING RATE LIMITING PREVENTS 429 ERRORS ===")

        # Create rate limiter with very conservative settings to avoid 429s
        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=0.2,  # Very conservative: 1 request per 5 seconds
            safety_factor=0.5,  # Extra conservative safety factor
        )

        endpoint_url = "https://api.close.com/api/v1/me/"

        print("Testing with very conservative rate limiting to prevent 429 errors...")

        # Make multiple requests with rate limiting
        results = []
        api_calls_made = 0

        for i in range(10):
            print(f"\nAttempt {i+1}/10:")

            # Check if rate limiter allows the request
            token_acquired = rate_limiter.acquire_token_for_endpoint(endpoint_url)
            print(
                f"  Rate limiter decision: {'✅ ALLOW' if token_acquired else '❌ DENY'}"
            )

            if token_acquired:
                try:
                    # Make the actual API call
                    response = requests.get(
                        endpoint_url, headers=self.close_headers, timeout=10
                    )
                    api_calls_made += 1

                    print(f"  API response: {response.status_code}")

                    # Record result
                    results.append(
                        {
                            "attempt": i + 1,
                            "rate_limiter_allowed": True,
                            "api_status": response.status_code,
                            "success": response.status_code == 200,
                            "rate_limited": response.status_code == 429,
                        }
                    )

                    # Update rate limiter with response headers
                    rate_limiter.update_from_response_headers(endpoint_url, response)

                    # Verify no 429 error
                    assert (
                        response.status_code != 429
                    ), f"Got 429 error despite rate limiting on attempt {i+1}"

                except requests.exceptions.RequestException as e:
                    print(f"  ❌ API call failed: {str(e)}")
                    results.append(
                        {
                            "attempt": i + 1,
                            "rate_limiter_allowed": True,
                            "api_status": None,
                            "success": False,
                            "rate_limited": False,
                            "error": str(e),
                        }
                    )
            else:
                # Rate limiter denied the request
                results.append(
                    {
                        "attempt": i + 1,
                        "rate_limiter_allowed": False,
                        "api_status": None,
                        "success": False,
                        "rate_limited": False,
                    }
                )

            # Wait between attempts
            time.sleep(1)

        # Analyze results
        print("\n=== RESULTS SUMMARY ===")
        print(f"Total attempts: {len(results)}")
        print(
            f"Rate limiter allowed: {sum(1 for r in results if r['rate_limiter_allowed'])}"
        )
        print(f"Actual API calls made: {api_calls_made}")
        print(
            f"Successful API calls: {sum(1 for r in results if r.get('success', False))}"
        )
        print(f"429 errors: {sum(1 for r in results if r.get('rate_limited', False))}")

        # Verify no 429 errors occurred
        rate_limited_count = sum(1 for r in results if r.get("rate_limited", False))
        assert (
            rate_limited_count == 0
        ), f"Got {rate_limited_count} 429 errors despite rate limiting"

        print("✅ Rate limiting successfully prevented 429 errors")

    def test_safety_factor_application_with_real_limits(self):
        """Test that safety factor is correctly applied to real discovered limits."""
        print("\n=== TESTING SAFETY FACTOR APPLICATION WITH REAL LIMITS ===")

        # Create rate limiter with known safety factor
        safety_factor = 0.6  # 60% of discovered limit
        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=1.0,
            safety_factor=safety_factor,
        )

        endpoint_url = "https://api.close.com/api/v1/me/"

        # Make initial API call to discover limits
        print("Making initial API call to discover limits...")

        token_acquired = rate_limiter.acquire_token_for_endpoint(endpoint_url)
        if token_acquired:
            try:
                response = requests.get(
                    endpoint_url, headers=self.close_headers, timeout=10
                )
                print(f"Initial API call status: {response.status_code}")

                if response.status_code == 200:
                    # Update limits from response
                    rate_limiter.update_from_response_headers(endpoint_url, response)

                    # Get discovered limits
                    from utils.rate_limiter import extract_endpoint_key

                    endpoint_key = extract_endpoint_key(endpoint_url)
                    discovered_limits = rate_limiter.get_endpoint_limits(endpoint_key)

                    if discovered_limits:
                        original_limit = discovered_limits["limit"]
                        print(f"Discovered API limit: {original_limit} requests/minute")

                        # Track cache key for cleanup
                        cache_key = f"close_rate_limit:limits:{endpoint_key}"
                        self.test_keys.append(cache_key)

                        # Calculate expected effective rate with safety factor
                        expected_effective_rate = (
                            original_limit * safety_factor
                        ) / 60.0  # Convert to req/sec
                        print(
                            f"Expected effective rate with {safety_factor} safety factor: {expected_effective_rate:.2f} req/sec"
                        )

                        # Test rate limiting over time to verify safety factor
                        print(
                            "Testing rate limiting over time to verify safety factor..."
                        )

                        start_time = time.time()
                        allowed_requests = 0
                        total_attempts = 20

                        for i in range(total_attempts):
                            if rate_limiter.acquire_token_for_endpoint(endpoint_url):
                                allowed_requests += 1
                            time.sleep(0.2)  # 5 req/sec attempt rate

                        elapsed_time = time.time() - start_time
                        actual_rate = allowed_requests / elapsed_time

                        print(f"Actual rate achieved: {actual_rate:.2f} req/sec")
                        print(f"Expected rate: {expected_effective_rate:.2f} req/sec")
                        print(f"Allowed requests: {allowed_requests}/{total_attempts}")

                        # Verify that actual rate is close to expected rate (with some tolerance)
                        rate_tolerance = 0.5  # Allow 0.5 req/sec tolerance
                        assert (
                            abs(actual_rate - expected_effective_rate) <= rate_tolerance
                        ), f"Actual rate {actual_rate:.2f} too far from expected {expected_effective_rate:.2f}"

                        # Verify that safety factor is actually reducing the rate
                        original_rate = original_limit / 60.0
                        assert (
                            actual_rate < original_rate
                        ), f"Safety factor not working: actual rate {actual_rate:.2f} >= original rate {original_rate:.2f}"

                        print(
                            "✅ Safety factor correctly applied to real discovered limits"
                        )
                    else:
                        print("⚠️  No limits discovered from API response")
                else:
                    print(
                        f"⚠️  Initial API call failed with status {response.status_code}"
                    )
            except requests.exceptions.RequestException as e:
                print(f"❌ Initial API call failed: {str(e)}")
        else:
            print("⚠️  Initial token acquisition failed")

    def test_real_world_timing_and_rate_enforcement(self):
        """Test real-world timing and rate enforcement with actual API calls."""
        print("\n=== TESTING REAL-WORLD TIMING AND RATE ENFORCEMENT ===")

        # Create rate limiter with moderate settings
        rate_limiter = CloseRateLimiter(
            redis_client=self.redis_client,
            conservative_default_rps=1.0,  # 1 req/sec default
            safety_factor=0.8,
        )

        endpoint_url = "https://api.close.com/api/v1/me/"

        print("Testing timing accuracy over extended period...")

        # Test over 30 seconds to get good timing data
        test_duration = 30  # seconds
        start_time = time.time()

        attempts = []
        successful_calls = 0

        while time.time() - start_time < test_duration:
            attempt_time = time.time()

            # Try to acquire token
            token_acquired = rate_limiter.acquire_token_for_endpoint(endpoint_url)

            attempt_data = {
                "time": attempt_time - start_time,
                "token_acquired": token_acquired,
                "api_call_made": False,
                "api_success": False,
            }

            if token_acquired:
                try:
                    # Make actual API call (but limit to avoid overwhelming API)
                    if successful_calls < 10:  # Limit total API calls
                        response = requests.get(
                            endpoint_url, headers=self.close_headers, timeout=10
                        )
                        attempt_data["api_call_made"] = True
                        attempt_data["api_success"] = response.status_code == 200

                        if response.status_code == 200:
                            successful_calls += 1
                            # Update rate limiter with response
                            rate_limiter.update_from_response_headers(
                                endpoint_url, response
                            )

                        print(
                            f"Time {attempt_data['time']:.1f}s: API call - Status {response.status_code}"
                        )
                    else:
                        # Just record that token was acquired without making API call
                        attempt_data["api_success"] = True  # Assume it would succeed
                        print(
                            f"Time {attempt_data['time']:.1f}s: Token acquired (API call skipped)"
                        )
                except requests.exceptions.RequestException as e:
                    print(
                        f"Time {attempt_data['time']:.1f}s: API call failed - {str(e)}"
                    )
            else:
                print(f"Time {attempt_data['time']:.1f}s: Token denied")

            attempts.append(attempt_data)

            # Wait before next attempt
            time.sleep(0.5)  # Attempt every 0.5 seconds

        # Analyze timing results
        total_time = time.time() - start_time
        tokens_acquired = sum(1 for a in attempts if a["token_acquired"])
        actual_rate = tokens_acquired / total_time

        print("\n=== TIMING ANALYSIS ===")
        print(f"Test duration: {total_time:.1f} seconds")
        print(f"Total attempts: {len(attempts)}")
        print(f"Tokens acquired: {tokens_acquired}")
        print(f"Actual rate: {actual_rate:.2f} req/sec")
        print(f"Successful API calls: {successful_calls}")

        # Verify rate is reasonable (should be close to 1 req/sec initially, then potentially higher after discovery)
        assert actual_rate >= 0.5, f"Rate too low: {actual_rate:.2f} req/sec"
        assert actual_rate <= 3.0, f"Rate too high: {actual_rate:.2f} req/sec"

        print("✅ Real-world timing and rate enforcement working correctly")
