"""
Integration test to reproduce the timeout issue with the Instantly add_lead endpoint.

This test is designed to FAIL initially to prove we can reproduce the timeout problem
before implementing any fixes. It generates test leads to trigger Heroku's 30-second timeout.

Stage 1: Concurrent testing with 20 leads for rapid iteration and fail-fast behavior
Stage 2: Comprehensive testing with 200 leads for full reproduction
Stage 3: Rate limiting integration testing with 700 leads (Step 2.3)
"""

import os
import time
import json
import requests
import redis
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.utils.close_api import CloseAPI
from utils.rate_limiter import RedisRateLimiter, APIRateConfig
import pytest


class TestInstantlyTimeoutReproduction:
    # Configure number of leads for timeout testing (easily adjustable)
    # Start with 40 for rapid iteration, scale up to 200+ for comprehensive testing
    TIMEOUT_TEST_LEAD_COUNT = 1000

    # New configuration for Step 2.3 rate limiting integration test
    RATE_LIMITING_TEST_LEAD_COUNT = 700

    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Set up Redis for rate limiter testing
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        try:
            self.redis_client = redis.from_url(self.redis_url)
            self.redis_client.ping()
            print(f"Successfully connected to Redis at: {self.redis_url}")
        except Exception as e:
            print(f"Warning: Failed to connect to Redis at {self.redis_url}: {e}")
            self.redis_client = None

        # Initialize rate limiter for testing (if Redis is available)
        self.rate_limiter = None
        if self.redis_client:
            self.rate_limiter = RedisRateLimiter(
                redis_client=self.redis_client,
                api_config=APIRateConfig.instantly(),  # 600 req/min = 10 req/sec
                safety_factor=0.8,  # 80% of limit = 8 req/sec
            )
            print(f"Rate limiter initialized: {self.rate_limiter}")

        # Generate timestamp for unique campaign name
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.campaign_name = "TimeoutTest"  # Manually created in Instantly

        # Track rate limiter keys for cleanup
        self.rate_limiter_keys = []

        # Base payload structure for Close webhook
        self.base_payload = {
            "subscription_id": "whsub_1vT2aEze4uUzQlqLIBExYl",
            "event": {
                "id": "ev_34bKnJcMX9UnRJmuGH5Jtr",
                "date_created": "2025-02-28T19:20:45.507000",
                "date_updated": "2025-02-28T19:20:45.507000",
                "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                "user_id": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                "request_id": "req_5SPmoSjkZBMkMkOAaxz7o7",
                "api_key_id": "api_3fw37yHasQmGs00Nnybzq5",
                "oauth_client_id": None,
                "oauth_scope": None,
                "object_type": "task.lead",
                "object_id": "task_CIRBr39mOsTfWAc3ErihkSt4cX0PlVBpTovHGNj939w",
                "lead_id": "lead_mtonPqjLkC0X93AW6evKVa1Sbpq7l8opyuaV5olT2Cf",
                "action": "created",
                "changed_fields": [],
                "meta": {"request_path": "/api/v1/task/", "request_method": "POST"},
                "data": {
                    "_type": "lead",
                    "object_type": None,
                    "contact_id": None,
                    "is_complete": False,
                    "assigned_to_name": "Barbara Pigg",
                    "id": "task_CIRBr39mOsTfWAc3ErihkSt4cX0PlVBpTovHGNj939w",
                    "sequence_id": None,
                    "is_new": True,
                    "created_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "date": "2025-03-01",
                    "deduplication_key": None,
                    "created_by_name": "Barbara Pigg",
                    "date_updated": "2025-02-28T19:20:45.505000+00:00",
                    "is_dateless": False,
                    "sequence_subscription_id": None,
                    "lead_id": "lead_mtonPqjLkC0X93AW6evKVa1Sbpq7l8opyuaV5olT2Cf",
                    "object_id": None,
                    "updated_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "due_date": "2025-03-01",
                    "is_primary_lead_notification": True,
                    "updated_by_name": "Barbara Pigg",
                    "assigned_to": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "text": f"Instantly: {self.campaign_name}",
                    "lead_name": "Test Lead",
                    "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                    "view": None,
                    "date_created": "2025-02-28T19:20:45.505000+00:00",
                },
                "previous_data": {},
            },
        }

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete test leads if they were created
        for lead_id in self.test_data.get("lead_ids", []):
            try:
                self.close_api.delete_lead(lead_id)
            except Exception as e:
                print(f"Warning: Could not delete test lead {lead_id}: {e}")

        # Clean up rate limiter keys from Redis
        if self.redis_client:
            for key in self.rate_limiter_keys:
                try:
                    self.redis_client.delete(key)
                except Exception as e:
                    print(f"Warning: Could not cleanup rate limiter key {key}: {e}")

    def generate_test_leads(self, count=None):
        """
        Generate the specified number of test leads in Close.

        Args:
            count (int): Number of test leads to create (default: uses TIMEOUT_TEST_LEAD_COUNT)

        Returns:
            list: List of created lead data
        """
        if count is None:
            count = self.TIMEOUT_TEST_LEAD_COUNT

        print(f"\n=== Generating {count} test leads ===")
        created_leads = []
        self.test_data["lead_ids"] = []

        for i in range(count):
            # Generate unique email with timestamp and index
            email = f"lance+{self.timestamp}+{i}@whiteboardgeeks.com"

            try:
                lead_data = self.close_api.create_test_lead(
                    email=email,
                    first_name="TestLead",
                    last_name=str(i),
                    custom_fields={
                        "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o": f"Test Company {i}",  # Company
                        "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9": f"Test Location {self.timestamp}",  # Date & Location
                    },
                    include_date_location=False,  # We're setting it manually above
                )
                created_leads.append(lead_data)
                self.test_data["lead_ids"].append(lead_data["id"])

                if (i + 1) % 25 == 0:  # Progress indicator every 25 leads
                    print(f"Created {i + 1}/{count} test leads")

            except Exception as e:
                print(f"Failed to create lead {i}: {e}")
                # Continue with other leads even if one fails

        print(f"Successfully created {len(created_leads)} test leads")
        return created_leads

    def send_webhook_request(self, lead, index):
        """
        Send a single webhook request for the given lead.

        Args:
            lead: Lead data dictionary
            index: Lead index for unique identification

        Returns:
            dict: Result containing success/timeout/error information
        """
        # Create unique payload for each lead
        payload = self.base_payload.copy()
        payload["event"]["data"]["lead_id"] = lead["id"]
        payload["event"]["data"]["text"] = f"Instantly: {self.campaign_name}"
        payload["event"]["data"]["id"] = f"task_timeout_test_{self.timestamp}_{index}"

        result = {
            "index": index,
            "lead_id": lead["id"],
            "status": None,
            "error": None,
            "response_code": None,
        }

        try:
            # Send webhook with 30-second timeout (Heroku's limit)
            response = requests.post(
                f"{self.base_url}/instantly/add_lead",
                json=payload,
                timeout=30,
            )

            result["response_code"] = response.status_code

            # Parse response JSON to check for hidden errors
            try:
                response_json = response.json()
                result["response_json"] = response_json

                # Check if this is actually an error disguised as HTTP 200
                if response.status_code == 200:
                    # Look for error indicators in the response
                    message = response_json.get("message", "")
                    instantly_result = response_json.get("instantly_result", {})

                    if (
                        "Failed to add lead to Instantly" in message
                        or instantly_result.get("status") == "error"
                        or "rate limit" in message.lower()
                        or "429" in message
                    ):
                        result["status"] = "rate_limited"
                        result["error"] = f"Rate limited: {message}"
                        print(f"DEBUG Lead {index}: RATE LIMITED - {message}")
                    else:
                        result["status"] = "success"
                else:
                    result["status"] = "error"
                    result["error"] = f"HTTP {response.status_code}"
                    print(
                        f"DEBUG Lead {index}: HTTP {response.status_code} - {response.text[:100]}..."
                    )

            except (ValueError, json.JSONDecodeError):
                # If we can't parse JSON, treat as error
                result["status"] = "error"
                result["error"] = f"HTTP {response.status_code} - Invalid JSON"
                print(f"DEBUG Lead {index}: Invalid JSON response")

        except requests.exceptions.Timeout:
            result["status"] = "timeout"
            result["error"] = "TIMEOUT after 30 seconds"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def send_rate_limited_webhook_request(self, lead, index):
        """
        Send a webhook request with rate limiting applied.

        Args:
            lead: Lead data dictionary
            index: Lead index for unique identification

        Returns:
            dict: Result containing success/timeout/error information plus rate limiting stats
        """
        # Create unique payload for each lead
        payload = self.base_payload.copy()
        payload["event"]["data"]["lead_id"] = lead["id"]
        payload["event"]["data"]["text"] = f"Instantly: {self.campaign_name}"
        payload["event"]["data"]["id"] = (
            f"task_rate_limit_test_{self.timestamp}_{index}"
        )

        result = {
            "index": index,
            "lead_id": lead["id"],
            "status": None,
            "error": None,
            "response_code": None,
            "rate_limited_by_test": False,
            "wait_time": 0.0,
        }

        # Apply rate limiting if available
        rate_limiter_key = f"test_rate_limit:{self.timestamp}"
        self.rate_limiter_keys.append(rate_limiter_key)  # Track for cleanup

        start_time = time.time()

        if self.rate_limiter:
            # Check rate limiter before making request
            while not self.rate_limiter.acquire_token(rate_limiter_key):
                # Rate limited - wait and try again
                result["rate_limited_by_test"] = True
                time.sleep(0.1)  # Wait 100ms before retrying

                # Prevent infinite waiting (safety check)
                if time.time() - start_time > 30:
                    result["status"] = "timeout"
                    result["error"] = "Rate limiter timeout after 30 seconds"
                    return result

        result["wait_time"] = time.time() - start_time

        try:
            # Send webhook with 30-second timeout (Heroku's limit)
            response = requests.post(
                f"{self.base_url}/instantly/add_lead",
                json=payload,
                timeout=30,
            )

            result["response_code"] = response.status_code

            # Parse response JSON to check for hidden errors
            try:
                response_json = response.json()
                result["response_json"] = response_json

                # Check if this is actually an error disguised as HTTP 200
                if response.status_code == 200:
                    # Look for error indicators in the response
                    message = response_json.get("message", "")
                    instantly_result = response_json.get("instantly_result", {})

                    if (
                        "Failed to add lead to Instantly" in message
                        or instantly_result.get("status") == "error"
                        or "rate limit" in message.lower()
                        or "429" in message
                    ):
                        result["status"] = "rate_limited"
                        result["error"] = f"Rate limited: {message}"
                        print(f"DEBUG Lead {index}: RATE LIMITED - {message}")
                    else:
                        result["status"] = "success"
                else:
                    result["status"] = "error"
                    result["error"] = f"HTTP {response.status_code}"
                    print(
                        f"DEBUG Lead {index}: HTTP {response.status_code} - {response.text[:100]}..."
                    )

            except (ValueError, json.JSONDecodeError):
                # If we can't parse JSON, treat as error
                result["status"] = "error"
                result["error"] = f"HTTP {response.status_code} - Invalid JSON"
                print(f"DEBUG Lead {index}: Invalid JSON response")

        except requests.exceptions.Timeout:
            result["status"] = "timeout"
            result["error"] = "TIMEOUT after 30 seconds"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def test_timeout_reproduction(self):
        """
        Test that reproduces timeout issues using concurrent requests.

        This test uses ThreadPoolExecutor to send all webhook requests simultaneously
        to trigger timeouts faster. It implements fail-fast behavior to stop immediately
        when the first timeout occurs, providing faster feedback during development.

        The number of leads is configurable via TIMEOUT_TEST_LEAD_COUNT class constant.
        Start with smaller numbers for rapid iteration, scale up for comprehensive testing.

        This test is EXPECTED TO FAIL initially with timeout errors to prove we can
        reproduce the timeout issue before implementing fixes.
        """
        # Use the centralized lead count configuration
        num_leads = self.TIMEOUT_TEST_LEAD_COUNT

        print(f"\n=== STARTING CONCURRENT TIMEOUT REPRODUCTION ({num_leads} leads) ===")
        print(f"Campaign: {self.campaign_name}")
        print(f"Timestamp: {self.timestamp}")

        # Generate test leads using centralized configuration
        leads = self.generate_test_leads()
        assert (
            len(leads) >= num_leads
        ), f"Failed to generate enough test leads. Got {len(leads)}, need {num_leads}"

        print(
            f"\nSending {len(leads)} concurrent webhook calls to trigger faster timeouts..."
        )
        print(
            "Using fail-fast approach - will stop immediately when first timeout occurs..."
        )

        # Track results
        results = {
            "timeouts": 0,
            "successes": 0,
            "errors": 0,
            "rate_limited": 0,
            "completed": 0,
        }
        response_codes = {}  # Track distribution of response codes
        start_time = time.time()

        # Send all webhook calls concurrently using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=50) as executor:
            # Submit all requests concurrently
            future_to_lead = {
                executor.submit(self.send_webhook_request, lead, i): (lead, i)
                for i, lead in enumerate(leads)
            }

            print(f"Submitted {len(future_to_lead)} concurrent requests...")

            # Process results as they complete (fail-fast behavior)
            for future in as_completed(future_to_lead):
                lead, index = future_to_lead[future]

                try:
                    result = future.result()
                    results["completed"] += 1

                    # Track response code distribution
                    code = result.get("response_code", "None")
                    response_codes[code] = response_codes.get(code, 0) + 1

                    if result["status"] == "timeout":
                        results["timeouts"] += 1
                        print(f"Lead {result['index']}: {result['error']}")

                        # FAIL-FAST: Cancel all remaining futures when first timeout occurs
                        print(
                            f"\n🔥 FAIL-FAST: First timeout detected after {results['completed']} requests!"
                        )
                        print(
                            "Cancelling all remaining requests for faster iteration..."
                        )

                        for remaining_future in future_to_lead:
                            if not remaining_future.done():
                                remaining_future.cancel()

                        # Break out of the loop to stop immediately
                        break

                    elif result["status"] == "success":
                        results["successes"] += 1

                    elif result["status"] == "rate_limited":
                        results["rate_limited"] += 1
                        print(f"Lead {result['index']}: {result['error']}")

                    else:  # error
                        results["errors"] += 1
                        print(f"Lead {result['index']}: {result['error']}")

                    # Progress indicator every 5 requests for smaller batch
                    if results["completed"] % 5 == 0:
                        elapsed = time.time() - start_time
                        print(
                            f"Progress: {results['completed']}/{len(leads)} | "
                            f"Timeouts: {results['timeouts']} | "
                            f"Successes: {results['successes']} | "
                            f"Rate Limited: {results['rate_limited']} | "
                            f"Errors: {results['errors']} | "
                            f"Time: {elapsed:.1f}s"
                        )

                except Exception as e:
                    results["errors"] += 1
                    print(f"Lead {index}: Exception processing result - {e}")

        total_time = time.time() - start_time
        print("\n=== STAGE 1 FINAL RESULTS ===")
        print(f"Total webhooks submitted: {len(leads)}")
        print(f"Requests completed: {results['completed']}")
        print(f"Successes: {results['successes']}")
        print(f"Timeouts: {results['timeouts']}")
        print(f"Rate Limited: {results['rate_limited']}")
        print(f"Errors: {results['errors']}")
        print(f"Total time: {total_time:.1f} seconds")
        print(f"Response codes: {response_codes}")

        if results["completed"] > 0:
            print(f"Rate: {results['completed']/total_time:.1f} webhooks/second")

        # The test should demonstrate timeout or rate limiting issues
        if results["timeouts"] == 0 and results["rate_limited"] == 0:
            raise AssertionError(
                f"Expected timeout or rate limiting with {len(leads)} concurrent webhook calls, but got none. "
                f"Completed: {results['completed']}, Successes: {results['successes']}, "
                f"Rate Limited: {results['rate_limited']}, Errors: {results['errors']}. "
                "This test is designed to fail initially to prove timeout/rate limiting reproduction. "
                "Try increasing concurrent load or check if rate limiting is working properly."
            )
        else:
            # This is the expected outcome - timeout or rate limiting occurred
            issue_type = "timeout" if results["timeouts"] > 0 else "rate limiting"
            issue_count = (
                results["timeouts"]
                if results["timeouts"] > 0
                else results["rate_limited"]
            )

            print(
                f"\n✅ SUCCESS: Reproduced {issue_type} after {results['completed']} concurrent requests in {total_time:.1f}s"
            )
            print(
                "Fail-fast approach provided rapid feedback for development iteration!"
            )

            # Fail the test as intended to prove the issue exists
            raise AssertionError(
                f"ISSUE REPRODUCED: {issue_type.upper()} occurred {issue_count} times after {results['completed']} concurrent requests. "
                "This proves the timeout/rate limiting issue exists and needs to be fixed with async processing. "
                f"Concurrent approach triggered {issue_type} in {total_time:.1f} seconds for rapid iteration."
            )

    def test_rate_limiting_integration(self):
        """
        Step 2.3 Integration Test: Verify rate limiting works with controlled request rate.

        This test modifies the timeout reproduction to integrate Redis rate limiting.
        It should:
        1. Use rate limiter to control requests to ≤8/second (80% of 10/second Instantly limit)
        2. Test with 700 leads to demonstrate controlled processing
        3. HTTP request should still timeout (proving we need async processing in next steps)

        Expected behavior:
        - Requests are rate-limited to controlled rate
        - Processing takes longer due to rate limiting
        - HTTP timeout still occurs, proving more fixes are needed
        """
        if not self.redis_client or not self.rate_limiter:
            pytest.skip("Redis or rate limiter not available for this test")

        # Use configuration for rate limiting test
        num_leads = self.RATE_LIMITING_TEST_LEAD_COUNT

        print(f"\n=== STEP 2.3: RATE LIMITING INTEGRATION TEST ({num_leads} leads) ===")
        print(f"Campaign: {self.campaign_name}")
        print(f"Timestamp: {self.timestamp}")
        print(f"Rate limiter config: {self.rate_limiter}")

        # Generate test leads
        leads = self.generate_test_leads(num_leads)
        assert (
            len(leads) >= num_leads
        ), f"Failed to generate enough test leads. Got {len(leads)}, need {num_leads}"

        print(f"\nSending {len(leads)} webhook calls with rate limiting...")
        print("Expected: Controlled request rate ≤8 requests/second")
        print("Expected: HTTP timeout still occurs (proving need for async processing)")

        # Track results and timing
        results = {
            "timeouts": 0,
            "successes": 0,
            "errors": 0,
            "rate_limited": 0,
            "completed": 0,
            "test_rate_limited": 0,  # Count of requests rate-limited by our test
        }
        response_codes = {}
        request_times = []  # Track timing between requests
        start_time = time.time()
        last_request_time = start_time

        # Send requests with rate limiting (using smaller thread pool for controlled rate)
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all requests
            future_to_lead = {
                executor.submit(self.send_rate_limited_webhook_request, lead, i): (
                    lead,
                    i,
                )
                for i, lead in enumerate(leads)
            }

            print(f"Submitted {len(future_to_lead)} rate-limited requests...")

            # Process results as they complete
            for future in as_completed(future_to_lead):
                lead, index = future_to_lead[future]

                try:
                    result = future.result()
                    results["completed"] += 1

                    # Track timing between requests
                    current_time = time.time()
                    if len(request_times) > 0:
                        time_since_last = current_time - last_request_time
                        request_times.append(time_since_last)
                    last_request_time = current_time

                    # Track response code distribution
                    code = result.get("response_code", "None")
                    response_codes[code] = response_codes.get(code, 0) + 1

                    # Count test rate limiting
                    if result.get("rate_limited_by_test", False):
                        results["test_rate_limited"] += 1

                    if result["status"] == "timeout":
                        results["timeouts"] += 1
                        print(
                            f"Lead {result['index']}: {result['error']} (waited {result['wait_time']:.2f}s for rate limit)"
                        )

                        # For rate limiting test, we expect timeout but want to see more processing
                        # Don't fail-fast like the original test

                    elif result["status"] == "success":
                        results["successes"] += 1

                    elif result["status"] == "rate_limited":
                        results["rate_limited"] += 1
                        print(f"Lead {result['index']}: {result['error']}")

                    else:  # error
                        results["errors"] += 1
                        print(f"Lead {result['index']}: {result['error']}")

                    # Progress indicator every 50 requests for larger batch
                    if results["completed"] % 50 == 0 or results["timeouts"] > 0:
                        elapsed = time.time() - start_time
                        avg_rate = results["completed"] / elapsed if elapsed > 0 else 0

                        print(
                            f"Progress: {results['completed']}/{len(leads)} | "
                            f"Rate: {avg_rate:.1f} req/s | "
                            f"Test Rate Limited: {results['test_rate_limited']} | "
                            f"Timeouts: {results['timeouts']} | "
                            f"Successes: {results['successes']} | "
                            f"API Rate Limited: {results['rate_limited']} | "
                            f"Errors: {results['errors']} | "
                            f"Time: {elapsed:.1f}s"
                        )

                        # Stop after first timeout to demonstrate the issue
                        if results["timeouts"] > 0:
                            print(
                                "\n🔥 HTTP TIMEOUT DETECTED - Rate limiting alone not sufficient!"
                            )
                            print("Cancelling remaining requests...")

                            for remaining_future in future_to_lead:
                                if not remaining_future.done():
                                    remaining_future.cancel()
                            break

                except Exception as e:
                    results["errors"] += 1
                    print(f"Lead {index}: Exception processing result - {e}")

        total_time = time.time() - start_time

        print("\n=== STEP 2.3 RATE LIMITING INTEGRATION RESULTS ===")
        print(f"Total webhooks submitted: {len(leads)}")
        print(f"Requests completed: {results['completed']}")
        print(f"Test rate limited (our limiter): {results['test_rate_limited']}")
        print(f"Successes: {results['successes']}")
        print(f"Timeouts: {results['timeouts']}")
        print(f"API Rate Limited: {results['rate_limited']}")
        print(f"Errors: {results['errors']}")
        print(f"Total time: {total_time:.1f} seconds")
        print(f"Response codes: {response_codes}")

        if results["completed"] > 0:
            avg_rate = results["completed"] / total_time
            print(f"Average request rate: {avg_rate:.2f} requests/second")

            # Calculate rate limit effectiveness
            if request_times:
                avg_interval = sum(request_times) / len(request_times)
                calculated_rate = 1.0 / avg_interval if avg_interval > 0 else 0
                print(
                    f"Calculated rate (from intervals): {calculated_rate:.2f} requests/second"
                )

        # Verify rate limiting is working
        if results["completed"] > 0:
            avg_rate = results["completed"] / total_time

            # Check that our rate limiting is effective (≤10 req/sec, preferably ≤8)
            if avg_rate > 10:
                raise AssertionError(
                    f"Rate limiting FAILED: Average rate {avg_rate:.2f} req/s exceeds Instantly limit (10 req/s). "
                    f"Rate limiter not working properly."
                )
            elif avg_rate > 8:
                print(
                    f"⚠️  WARNING: Rate {avg_rate:.2f} req/s is close to limit (target ≤8 req/s)"
                )
            else:
                print(
                    f"✅ Rate limiting WORKING: {avg_rate:.2f} req/s ≤ 8 req/s target"
                )

        # Verify that HTTP timeout still occurs (proving need for async processing)
        if results["timeouts"] == 0:
            print(
                "⚠️  WARNING: No HTTP timeout occurred. Rate limiting may be too restrictive or test too small."
            )
            print(
                "This might indicate rate limiting is working TOO well and no timeout reproduction occurred."
            )
        else:
            print(
                f"✅ HTTP TIMEOUT REPRODUCED: {results['timeouts']} timeouts occurred"
            )
            print(
                "This proves that rate limiting alone is not sufficient - async processing needed!"
            )

        # This test should demonstrate both:
        # 1. Rate limiting is working (controlled rate)
        # 2. HTTP timeout still occurs (need async processing)

        # Expected outcome for Step 2.3:
        assert (
            results["test_rate_limited"] > 0 or avg_rate <= 8
        ), "Rate limiting should be active and controlling request rate to ≤8 req/s"

        # The test demonstrates the issue but doesn't fail - it shows progress toward solution
        print("\n✅ STEP 2.3 SUCCESS: Rate limiting integration verified!")
        print(f"Rate controlled to {avg_rate:.2f} req/s (≤ 8 req/s target)")
        print(f"Test rate limited {results['test_rate_limited']} requests")

        if results["timeouts"] > 0:
            print(
                f"HTTP timeout still occurred ({results['timeouts']} times) - proving need for async processing (Step 5)"
            )

            # As per plan, this test should still fail to show that more fixes are needed
            raise AssertionError(
                f"EXPECTED OUTCOME: Rate limiting works ({avg_rate:.2f} req/s ≤ 8) BUT HTTP timeout still occurred "
                f"({results['timeouts']} times). This proves rate limiting alone is insufficient and "
                f"async processing (Step 5) is needed to fully solve the timeout issue."
            )
        else:
            print(
                "No HTTP timeout occurred - rate limiting may be sufficient for this load"
            )
