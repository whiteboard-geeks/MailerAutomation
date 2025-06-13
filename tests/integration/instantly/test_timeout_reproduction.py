"""
Integration test to reproduce the timeout issue with the Instantly add_lead endpoint.

This test is designed to FAIL initially to prove we can reproduce the timeout problem
before implementing any fixes. It can use pre-generated test leads to avoid creating
leads every time, which speeds up test execution significantly.

SETUP INSTRUCTIONS:
1. Generate 3,000 test leads once: python scripts/generate_test_leads.py
2. Run tests with pre-generated leads (default behavior)
3. Set USE_PREGENERATED_LEADS = False to create new leads each time

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

# Import the test lead generator functions
import sys

sys.path.insert(
    0,
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
)
from scripts.generate_test_leads import load_test_leads


class TestInstantlyTimeoutReproduction:
    # Configure number of leads for timeout testing (easily adjustable)
    # Start with 40 for rapid iteration, scale up to 200+ for comprehensive testing
    TIMEOUT_TEST_LEAD_COUNT = 1000

    # New configuration for Step 2.3 rate limiting integration test
    RATE_LIMITING_TEST_LEAD_COUNT = 700

    # Flag to control whether to use pre-generated leads or create new ones
    USE_PREGENERATED_LEADS = True  # Set to False to create leads each time

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

            # Warm up Redis connection by doing a few operations
            self._warmup_redis_connection()
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
        # Only delete test leads if they were created fresh (not pre-generated)
        if not self.USE_PREGENERATED_LEADS:
            for lead_id in self.test_data.get("lead_ids", []):
                try:
                    self.close_api.delete_lead(lead_id)
                except Exception as e:
                    print(f"Warning: Could not delete test lead {lead_id}: {e}")
        else:
            # For pre-generated leads, just clear the tracking list
            lead_count = len(self.test_data.get("lead_ids", []))
            if lead_count > 0:
                print(
                    f"Skipping deletion of {lead_count} pre-generated test leads (reusable)"
                )

        # Clean up rate limiter keys from Redis
        if self.redis_client:
            for key in self.rate_limiter_keys:
                try:
                    self.redis_client.delete(key)
                except Exception as e:
                    print(f"Warning: Could not cleanup rate limiter key {key}: {e}")

    def generate_test_leads(self, count=None):
        """
        Generate or load the specified number of test leads.

        Args:
            count (int): Number of test leads to create/load (default: uses TIMEOUT_TEST_LEAD_COUNT)

        Returns:
            list: List of lead data (either pre-generated or newly created)
        """
        if count is None:
            count = self.TIMEOUT_TEST_LEAD_COUNT

        # Try to use pre-generated leads if the flag is set
        if self.USE_PREGENERATED_LEADS:
            print(f"\n=== Loading {count} pre-generated test leads ===")
            pregenerated_leads = load_test_leads()

            if len(pregenerated_leads) >= count:
                # Use the first 'count' leads from the pre-generated set
                selected_leads = pregenerated_leads[:count]

                # Convert to the format expected by the test (add id to lead_ids for cleanup)
                self.test_data["lead_ids"] = [lead["id"] for lead in selected_leads]

                # Convert the lead format to match what create_test_lead returns
                formatted_leads = []
                for lead in selected_leads:
                    formatted_lead = {
                        "id": lead["id"],
                        "name": lead.get("name", "TimeoutTestLead"),
                        "date_created": lead.get("created_at"),
                        # Add other fields that tests might expect
                        "contacts": [{"emails": [{"email": lead["email"]}]}],
                    }
                    formatted_leads.append(formatted_lead)

                print(f"✓ Loaded {len(formatted_leads)} pre-generated test leads")
                print(
                    f"  Generated at: {pregenerated_leads[0].get('created_at', 'unknown') if pregenerated_leads else 'unknown'}"
                )
                return formatted_leads
            else:
                print(
                    f"⚠️  Warning: Only {len(pregenerated_leads)} pre-generated leads available, need {count}"
                )
                print("Falling back to creating new leads...")

        # Fallback to creating new leads (original logic)
        print(f"\n=== Generating {count} new test leads ===")
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
        close_task_id = f"task_timeout_test_{self.timestamp}_{index}"
        payload["event"]["data"]["id"] = close_task_id

        result = {
            "index": index,
            "lead_id": lead["id"],
            "close_task_id": close_task_id,
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

                # Check if this is actually an error disguised as HTTP 202
                if response.status_code == 202:
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

                # Check if this is actually an error disguised as HTTP 202
                if response.status_code == 202:
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
                    "Rate limiter not working properly."
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
                "async processing (Step 5) is needed to fully solve the timeout issue."
            )
        else:
            print(
                "No HTTP timeout occurred - rate limiting may be sufficient for this load"
            )

    def test_add_lead_scaled_testing(self):
        """
        Scaled testing for add_lead functionality with 100% success rate requirement.

        This test is configured for reliable operation with zero tolerance for failures.
        All webhook requests must complete successfully without timeouts, rate limiting,
        or other errors.

        Configuration for 100% success rate:
        - Uses sequential execution (use_concurrent = False) for controlled processing
        - 1.0 second delay between requests to avoid rate limiting
        - Full Instantly verification enabled by default
        - Conservative scaling recommendations

        Usage:
        - Start with 2 leads to verify basic functionality
        - Gradually increase: 5, 10, 20, 50+ (keeping sequential mode)
        - All requests must succeed for test to pass
        - Provides guidance for reliable scaling
        """
        # CONFIGURABLE: Change this number to scale up testing
        num_leads = 5  # Start with 2, then try: 5, 10, 20, 50, 100+

        # CONFIGURABLE: Choose execution mode (using sequential for 100% success rate)
        use_concurrent = (
            False  # Set to False for sequential execution (100% success rate)
        )
        concurrent_workers = 5  # Number of concurrent workers if using concurrent mode
        request_delay = 0.1  # Delay between sequential requests (seconds)

        # CONFIGURABLE: Verification options
        verify_instantly_success = True  # Set to False for HTTP-only testing (faster)
        verification_timeout = (
            180  # Seconds to wait for webhook processing (Instantly is slow)
        )
        verification_poll_interval = (
            5  # Seconds between status checks (reduce API calls)
        )

        print(f"\n=== SCALED ADD_LEAD TESTING ({num_leads} leads) ===")
        print(f"Campaign: {self.campaign_name}")
        print(f"Timestamp: {self.timestamp}")
        print(f"Execution mode: {'Concurrent' if use_concurrent else 'Sequential'}")
        print(
            f"Verification mode: {'Full Instantly verification' if verify_instantly_success else 'HTTP response only'}"
        )
        if not use_concurrent:
            print(f"Request delay: {request_delay}s between requests")
        else:
            print(f"Concurrent workers: {concurrent_workers}")
        if verify_instantly_success:
            print(
                f"Verification timeout: {verification_timeout}s (poll every {verification_poll_interval}s)"
            )

        # Generate test leads using existing infrastructure
        leads = self.generate_test_leads(num_leads)
        assert (
            len(leads) >= num_leads
        ), f"Failed to generate enough test leads. Got {len(leads)}, need {num_leads}"

        print(f"\nProcessing {len(leads)} leads...")

        # Warm up connections before starting the real test to avoid first-lead timing issues
        if verify_instantly_success:
            self._warmup_first_webhook_write()

        # Track results and timing
        results = {
            "timeouts": 0,
            "successes": 0,
            "errors": 0,
            "rate_limited": 0,
            "completed": 0,
            "instantly_failed": 0,  # New: Failed Instantly API calls
            "webhook_processed": 0,  # New: Successfully processed webhooks
        }
        response_codes = {}
        detailed_results = []  # Store detailed results for analysis
        start_time = time.time()

        if use_concurrent:
            # Concurrent execution for stress testing
            print("Sending webhooks concurrently...")

            with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
                # Submit all requests concurrently using verification method
                future_to_lead = {
                    executor.submit(
                        self.send_webhook_request_with_verification,
                        lead,
                        i,
                        verify_instantly_success,
                        verification_timeout,
                        verification_poll_interval,
                    ): (lead, i)
                    for i, lead in enumerate(leads)
                }

                print(f"Submitted {len(future_to_lead)} concurrent requests...")

                # Process results as they complete
                for future in as_completed(future_to_lead):
                    lead, index = future_to_lead[future]

                    try:
                        result = future.result()
                        detailed_results.append(result)
                        results["completed"] += 1

                        # Track response code distribution
                        code = result.get("response_code", "None")
                        response_codes[code] = response_codes.get(code, 0) + 1

                        # Categorize results with new verification status
                        if result["status"] == "timeout":
                            results["timeouts"] += 1
                            print(
                                f"Lead {result['index']}: TIMEOUT - {result['error']}"
                            )
                        elif result["status"] == "success":
                            results["successes"] += 1
                            if result.get("webhook_processed"):
                                results["webhook_processed"] += 1
                            if verify_instantly_success and result.get(
                                "instantly_success"
                            ):
                                print(
                                    f"Lead {result['index']}: SUCCESS ✅ (Instantly confirmed)"
                                )
                            else:
                                print(f"Lead {result['index']}: SUCCESS ✅ (HTTP only)")
                        elif result["status"] == "instantly_failed":
                            results["instantly_failed"] += 1
                            print(
                                f"Lead {result['index']}: INSTANTLY FAILED ❌ - {result['error']}"
                            )
                        elif result["status"] == "rate_limited":
                            results["rate_limited"] += 1
                            print(
                                f"Lead {result['index']}: RATE LIMITED - {result['error']}"
                            )
                        else:  # error
                            results["errors"] += 1
                            print(f"Lead {result['index']}: ERROR - {result['error']}")

                    except Exception as e:
                        results["errors"] += 1
                        print(f"Lead {index}: Exception processing result - {e}")

        else:
            # Sequential execution for controlled testing
            print("Sending webhooks sequentially...")

            for i, lead in enumerate(leads):
                print(f"Processing lead {i+1}/{len(leads)}...")

                try:
                    result = self.send_webhook_request_with_verification(
                        lead,
                        i,
                        verify_instantly_success,
                        verification_timeout,
                        verification_poll_interval,
                    )
                    detailed_results.append(result)
                    results["completed"] += 1

                    # Track response code distribution
                    code = result.get("response_code", "None")
                    response_codes[code] = response_codes.get(code, 0) + 1

                    # Categorize and log results with verification details
                    if result["status"] == "timeout":
                        results["timeouts"] += 1
                        print(f"  ❌ TIMEOUT - {result['error']}")
                    elif result["status"] == "success":
                        results["successes"] += 1
                        if result.get("webhook_processed"):
                            results["webhook_processed"] += 1
                        if verify_instantly_success and result.get("instantly_success"):
                            print("  ✅ SUCCESS (Instantly confirmed)")
                        else:
                            print("  ✅ SUCCESS (HTTP only)")
                    elif result["status"] == "instantly_failed":
                        results["instantly_failed"] += 1
                        print(f"  ❌ INSTANTLY FAILED - {result['error']}")
                    elif result["status"] == "rate_limited":
                        results["rate_limited"] += 1
                        print(f"  🔄 RATE LIMITED - {result['error']}")
                    else:  # error
                        results["errors"] += 1
                        print(f"  ❌ ERROR - {result['error']}")

                    # Add delay between requests if specified
                    if (
                        request_delay > 0 and i < len(leads) - 1
                    ):  # Don't delay after last request
                        time.sleep(request_delay)

                except Exception as e:
                    results["errors"] += 1
                    print(f"  ❌ EXCEPTION - {e}")

        total_time = time.time() - start_time

        # Detailed results analysis
        print(f"\n=== SCALED TESTING RESULTS ({num_leads} leads) ===")
        print(f"Execution mode: {'Concurrent' if use_concurrent else 'Sequential'}")
        print(f"Total webhooks submitted: {len(leads)}")
        print(f"Requests completed: {results['completed']}")
        print(f"Total time: {total_time:.2f} seconds")

        if results["completed"] > 0:
            avg_rate = results["completed"] / total_time
            print(f"Average rate: {avg_rate:.2f} requests/second")

        print("\nResult breakdown:")
        print(f"  ✅ HTTP Successes: {results['successes']}")
        if verify_instantly_success:
            print(f"  🔄 Webhooks Processed: {results['webhook_processed']}")
            instantly_success_count = sum(
                1 for r in detailed_results if r.get("instantly_success")
            )
            print(f"  ✅ Instantly Confirmed: {instantly_success_count}")
            print(f"  ❌ Instantly Failed: {results['instantly_failed']}")
        print(f"  ❌ Timeouts: {results['timeouts']}")
        print(f"  🔄 Rate Limited: {results['rate_limited']}")
        print(f"  ❌ Other Errors: {results['errors']}")
        print(f"Response codes: {response_codes}")

        # Calculate success rates
        if results["completed"] > 0:
            http_success_rate = (results["successes"] / results["completed"]) * 100
            print(f"HTTP success rate: {http_success_rate:.1f}%")

            if verify_instantly_success:
                instantly_success_count = sum(
                    1 for r in detailed_results if r.get("instantly_success")
                )
                instantly_success_rate = (
                    instantly_success_count / results["completed"]
                ) * 100
                print(f"Instantly success rate: {instantly_success_rate:.1f}%")

        # Analysis and recommendations
        print("\n=== ANALYSIS ===")

        if (
            results["timeouts"] == 0
            and results["rate_limited"] == 0
            and results["instantly_failed"] == 0
        ):
            if verify_instantly_success:
                instantly_success_count = sum(
                    1 for r in detailed_results if r.get("instantly_success")
                )
                if instantly_success_count == len(leads):
                    print(
                        f"✅ Perfect! All {num_leads} leads successfully added to Instantly!"
                    )
                else:
                    print(
                        f"⚠️ HTTP success but Instantly verification incomplete: {instantly_success_count}/{len(leads)} confirmed"
                    )
            else:
                print(f"✅ No HTTP issues with {num_leads} leads!")
                print(
                    "💡 Consider enabling verify_instantly_success=True for full verification"
                )
            print(
                f"💡 Recommendation: Try scaling up to {num_leads * 2} or {num_leads * 5} leads"
            )

        elif results["instantly_failed"] > 0:
            print(
                f"⚠️ Instantly API failures: {results['instantly_failed']} leads failed to be added"
            )
            print(
                "💡 Check Instantly API status, rate limits, or campaign configuration"
            )

        elif results["timeouts"] > 0:
            timeout_rate = (results["timeouts"] / results["completed"]) * 100
            print(f"⚠️  Timeouts occurred: {results['timeouts']} ({timeout_rate:.1f}%)")
            print(
                f"💡 This indicates {num_leads} leads is approaching/exceeding the threshold"
            )

            if num_leads <= 5:
                print(
                    f"🔥 CRITICAL: Timeouts with only {num_leads} leads suggests a serious issue"
                )
            elif num_leads <= 20:
                print(f"⚠️  WARNING: Timeouts starting at {num_leads} leads")
            else:
                print(f"📊 INFO: Found timeout threshold around {num_leads} leads")

        elif results["rate_limited"] > 0:
            rate_limit_rate = (results["rate_limited"] / results["completed"]) * 100
            print(
                f"🔄 Rate limiting occurred: {results['rate_limited']} ({rate_limit_rate:.1f}%)"
            )
            print(f"💡 API rate limits are being hit with {num_leads} leads")

        # Assertions based on expected behavior
        print("\n=== ASSERTIONS ===")

        # Basic assertion: all requests should complete
        assert results["completed"] == len(
            leads
        ), f"Not all requests completed: {results['completed']}/{len(leads)}"

        # Require 100% success rate - no timeouts allowed for any number of leads
        if results["timeouts"] > 0:
            raise AssertionError(
                f"TIMEOUT FAILURES: Got {results['timeouts']} timeouts with {num_leads} leads. "
                f"For 100% success rate, all requests must complete successfully. "
                f"Consider reducing concurrent workers, increasing delays, or using sequential mode."
            )
        else:
            print(
                f"✅ PASS: No timeouts with {num_leads} leads (100% success rate target)"
            )

        # Require no rate limiting failures
        if results["rate_limited"] > 0:
            raise AssertionError(
                f"RATE LIMITING FAILURES: Got {results['rate_limited']} rate limited requests with {num_leads} leads. "
                f"For 100% success rate, all requests must complete without rate limiting. "
                f"Consider increasing delays between requests or using sequential mode."
            )

        # Require 100% success rate for all requests
        if results["completed"] > 0:
            http_success_rate = (results["successes"] / results["completed"]) * 100

            if verify_instantly_success:
                instantly_success_count = sum(
                    1 for r in detailed_results if r.get("instantly_success")
                )
                instantly_success_rate = (
                    instantly_success_count / results["completed"]
                ) * 100

                if instantly_success_rate < 100:
                    raise AssertionError(
                        f"INSTANTLY SUCCESS RATE NOT 100%: Only {instantly_success_rate:.1f}% of leads confirmed added to Instantly with {num_leads} leads. "
                        f"HTTP success rate was {http_success_rate:.1f}%. For 100% success rate target, all leads must be successfully added to Instantly. "
                        f"Failed leads: {results['instantly_failed']}, Other errors: {results['errors']}"
                    )
                else:
                    print(
                        f"✅ PASS: Instantly success rate {instantly_success_rate:.1f}% = 100% target achieved!"
                    )
            else:
                if http_success_rate < 100:
                    raise AssertionError(
                        f"HTTP SUCCESS RATE NOT 100%: Only {http_success_rate:.1f}% HTTP success rate with {num_leads} leads. "
                        f"For 100% success rate target, all HTTP requests must succeed. "
                        f"Errors: {results['errors']}, Rate limited: {results['rate_limited']}, Timeouts: {results['timeouts']}"
                    )
                else:
                    print(
                        f"✅ PASS: HTTP success rate {http_success_rate:.1f}% = 100% target achieved!"
                    )

        print(f"✅ SCALED TESTING COMPLETED: {num_leads} leads processed successfully")

        # Provide scaling guidance for 100% success rate
        print("\n=== SCALING GUIDANCE ===")
        if (
            results["timeouts"] == 0
            and results["rate_limited"] == 0
            and results["errors"] == 0
        ):
            next_test_size = min(
                num_leads * 2, 50
            )  # Conservative scaling for 100% success rate
            print(f"💡 NEXT: Try testing with {next_test_size} leads")
            print("💡 Keep sequential mode (use_concurrent = False) for reliability")
            print(
                "💡 Consider increasing request_delay if issues arise with larger batches"
            )
        else:
            print(
                "💡 100% SUCCESS RATE NOT ACHIEVED: Review failures and adjust configuration"
            )
            print(
                f"💡 Current config: Sequential mode, {request_delay}s delay between requests"
            )
            print(
                f"💡 Consider increasing delay to {request_delay * 2}s for better reliability"
            )

        # Store results for potential follow-up analysis
        self.test_data["scaled_test_results"] = {
            "num_leads": num_leads,
            "execution_mode": "concurrent" if use_concurrent else "sequential",
            "results": results,
            "response_codes": response_codes,
            "total_time": total_time,
            "detailed_results": detailed_results,
        }

    def _warmup_redis_connection(self):
        """Warm up Redis connection to avoid first-request timing issues."""
        if not self.redis_client:
            return

        try:
            print("🔄 Warming up Redis connection...")
            warmup_key = f"warmup_{self.timestamp}"

            # Do a few Redis operations to warm up the connection
            self.redis_client.set(warmup_key, "warmup_value", ex=5)  # 5 second expiry
            self.redis_client.get(warmup_key)
            self.redis_client.delete(warmup_key)

            # Test the webhook status endpoint to warm up the Flask app's Redis connection too
            try:
                warmup_response = requests.get(
                    f"{self.base_url}/instantly/webhooks/status", timeout=5
                )
                print(f"  Flask app warmup response: {warmup_response.status_code}")
            except Exception as e:
                print(f"  Flask app warmup failed (OK): {e}")

            print("✅ Redis connection warmed up successfully")
        except Exception as e:
            print(f"⚠️ Redis warmup failed: {e}")

    def _warmup_first_webhook_write(self):
        """Send a dummy webhook to ensure the first real webhook write is fast."""
        if not self.redis_client:
            return

        try:
            print("🔄 Testing first webhook write timing...")

            # Send a dummy webhook to warm up the webhook tracker
            dummy_payload = self.base_payload.copy()
            dummy_task_id = f"warmup_task_{self.timestamp}"
            dummy_payload["event"]["data"]["id"] = dummy_task_id
            dummy_payload["event"]["data"]["text"] = "Instantly: TestWarmup"
            dummy_payload["event"]["data"]["lead_id"] = "warmup_lead"

            start_time = time.time()
            warmup_response = requests.post(
                f"{self.base_url}/instantly/add_lead",
                json=dummy_payload,
                timeout=10,
            )
            write_time = time.time() - start_time

            print(
                f"  Warmup webhook response: {warmup_response.status_code} in {write_time:.3f}s"
            )

            # Try to find it immediately to test lookup timing
            start_time = time.time()
            immediate_data = self.check_webhook_immediately_available(dummy_task_id)
            lookup_time = time.time() - start_time

            print(
                f"  Warmup webhook lookup: {'Found' if immediate_data else 'Not found'} in {lookup_time:.3f}s"
            )

            # Clean up
            if self.redis_client:
                self.redis_client.delete(f"webhook_tracker:{dummy_task_id}")

            print("✅ First webhook write timing tested")
        except Exception as e:
            print(f"⚠️ Webhook warmup failed: {e}")

    def check_webhook_immediately_available(self, close_task_id, route=None):
        """Check if webhook entry is immediately available (without waiting for completion)."""
        webhook_endpoint = (
            f"{self.base_url}/instantly/webhooks/status?close_task_id={close_task_id}"
        )
        if route:
            webhook_endpoint += f"&route={route}"

        try:
            start_time = time.time()
            response = requests.get(webhook_endpoint, timeout=5)
            lookup_time = time.time() - start_time

            print(
                f"DEBUG: Immediate check for {close_task_id} -> Status: {response.status_code} in {lookup_time:.3f}s"
            )

            if response.status_code == 200:
                webhook_data = response.json().get("data", {})
                if webhook_data:
                    print(
                        f"DEBUG: Found webhook data with keys: {list(webhook_data.keys())}"
                    )
                    # Add close_task_id to webhook data if not present
                    if "close_task_id" not in webhook_data:
                        webhook_data["close_task_id"] = close_task_id
                    return webhook_data
                else:
                    print(
                        f"DEBUG: 200 response but no data field in: {response.json()}"
                    )
            elif response.status_code == 404:
                error_data = response.json()
                print(
                    f"DEBUG: 404 - webhook not found: {error_data.get('message', 'No message')}"
                )
                return None
            else:
                print(
                    f"DEBUG: Unexpected status {response.status_code}: {response.text[:200]}"
                )
        except requests.exceptions.Timeout:
            print(f"ERROR: Webhook lookup timeout after 5s for {close_task_id}")
            return None
        except Exception as e:
            print(f"ERROR: Exception during webhook lookup for {close_task_id}: {e}")
            return None

        return None

    def wait_for_webhook_processed(
        self,
        close_task_id,
        route=None,
        wait_for_completion=True,
        timeout_seconds=180,
        poll_interval=5,
    ):
        """
        Wait for webhook to be processed by checking the webhook tracker API.

        Args:
            close_task_id: The Close task ID to check
            route: Optional route filter
            wait_for_completion: Whether to wait for full processing (processed=True)
            timeout_seconds: Total timeout (default 180s for Instantly processing)
            poll_interval: Seconds between checks (default 5s for Instantly)
        """
        webhook_endpoint = (
            f"{self.base_url}/instantly/webhooks/status?close_task_id={close_task_id}"
        )
        if route:
            webhook_endpoint += f"&route={route}"

        print(
            f"Waiting for webhook processing (timeout: {timeout_seconds}s, interval: {poll_interval}s)"
        )
        start_time = time.time()
        elapsed_time = 0
        check_count = 0

        while elapsed_time < timeout_seconds:
            check_count += 1
            try:
                response = requests.get(webhook_endpoint)
                print(
                    f"Check #{check_count} (after {elapsed_time:.0f}s): Status {response.status_code}"
                )

                if response.status_code == 200:
                    webhook_data = response.json().get("data", {})
                    if webhook_data:
                        # Add close_task_id to webhook data if not present
                        if "close_task_id" not in webhook_data:
                            webhook_data["close_task_id"] = close_task_id

                        status = webhook_data.get("status", "unknown")
                        processed = webhook_data.get("processed", False)
                        print(
                            f"  Webhook found: status={status}, processed={processed}"
                        )

                        # If we don't need to wait for completion, return immediately
                        if not wait_for_completion:
                            return webhook_data

                        # If we need completion, check if it's processed
                        if webhook_data.get("processed") is True:
                            print(
                                f"  ✅ Webhook processing completed after {elapsed_time:.0f}s"
                            )
                            return webhook_data
                        else:
                            print(f"  🔄 Still processing... (status: {status})")

                elif response.status_code == 404:
                    print("  ⏳ Webhook not found yet, continuing to wait...")
                else:
                    print(f"  ⚠️ Unexpected response: {response.status_code}")

            except Exception as e:
                print(f"  ❌ Error querying webhook API: {e}")

            time.sleep(poll_interval)  # Wait longer between checks for Instantly
            elapsed_time = time.time() - start_time

        print(f"❌ Timeout after {timeout_seconds}s waiting for webhook processing")
        return None

    def send_webhook_request_with_verification(
        self,
        lead,
        index,
        verify_instantly_success=True,
        timeout_seconds=180,
        poll_interval=5,
    ):
        """
        Send webhook request and optionally verify it was actually processed by Instantly.

        Args:
            lead: Lead data dictionary
            index: Lead index for unique identification
            verify_instantly_success: Whether to wait for and verify Instantly API success
            timeout_seconds: Seconds to wait for webhook processing (default 180s for Instantly)
            poll_interval: Seconds between status checks (default 5s for Instantly)

        Returns:
            dict: Result with webhook verification data
        """
        # Create unique payload for each lead
        payload = self.base_payload.copy()
        payload["event"]["data"]["lead_id"] = lead["id"]
        payload["event"]["data"]["text"] = f"Instantly: {self.campaign_name}"
        close_task_id = f"task_scaled_test_{self.timestamp}_{index}"
        payload["event"]["data"]["id"] = close_task_id

        result = {
            "index": index,
            "lead_id": lead["id"],
            "close_task_id": close_task_id,
            "status": None,
            "error": None,
            "response_code": None,
            "webhook_processed": False,
            "instantly_success": False,
            "instantly_result": None,
        }

        try:
            # Stage 1: Send webhook with 30-second timeout
            response = requests.post(
                f"{self.base_url}/instantly/add_lead",
                json=payload,
                timeout=30,
            )

            result["response_code"] = response.status_code

            # Parse response JSON
            try:
                response_json = response.json()
                result["response_json"] = response_json

                # Check HTTP response status
                if response.status_code not in [200, 202]:
                    result["status"] = "error"
                    result["error"] = f"HTTP {response.status_code}"
                    return result

                # Check for immediate errors in response
                message = response_json.get("message", "")
                if (
                    "Failed to add lead to Instantly" in message
                    or "rate limit" in message.lower()
                    or "429" in message
                ):
                    result["status"] = "rate_limited"
                    result["error"] = f"Rate limited: {message}"
                    return result

                # Stage 2: If verification is requested, wait for processing
                if verify_instantly_success:
                    # Add progressive delay strategy for Redis cold start issues
                    retry_delays = [
                        0.1,
                        0.5,
                        1.0,
                    ]  # Progressive delays: 100ms, 500ms, 1s
                    immediate_webhook_data = None

                    for attempt, delay in enumerate(retry_delays):
                        if attempt > 0:  # Don't delay on first attempt
                            print(f"  Retry {attempt}: waiting {delay}s for Redis...")
                            time.sleep(delay)

                        immediate_webhook_data = (
                            self.check_webhook_immediately_available(
                                close_task_id, "add_lead"
                            )
                        )

                        if immediate_webhook_data is not None:
                            if attempt > 0:
                                print(
                                    f"  ✅ Webhook found after {attempt + 1} attempts"
                                )
                            break

                    if immediate_webhook_data is None:
                        result["status"] = "error"
                        result["error"] = (
                            f"Webhook not findable after {len(retry_delays)} attempts with progressive delays. "
                            "This may indicate a Redis connection or timing issue."
                        )
                        return result

                    # Wait for webhook processing to complete
                    webhook_data = self.wait_for_webhook_processed(
                        close_task_id,
                        "add_lead",
                        wait_for_completion=True,
                        timeout_seconds=timeout_seconds,
                        poll_interval=poll_interval,
                    )

                    if webhook_data is None:
                        result["status"] = "timeout"
                        result["error"] = "Timeout waiting for webhook processing"
                        return result

                    result["webhook_processed"] = True
                    result["webhook_data"] = webhook_data

                    # Stage 3: Verify Instantly API success
                    instantly_result = webhook_data.get("instantly_result", {})
                    result["instantly_result"] = instantly_result

                    if instantly_result and instantly_result.get("status") == "success":
                        result["instantly_success"] = True
                        result["status"] = "success"
                    else:
                        result["status"] = "instantly_failed"
                        result["error"] = f"Instantly API failed: {instantly_result}"

                else:
                    # No verification requested - just check HTTP response
                    result["status"] = "success"

            except (ValueError, json.JSONDecodeError):
                result["status"] = "error"
                result["error"] = f"HTTP {response.status_code} - Invalid JSON"

        except requests.exceptions.Timeout:
            result["status"] = "timeout"
            result["error"] = "HTTP timeout after 30 seconds"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result
