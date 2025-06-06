"""
Integration tests for circuit breaker pattern functionality.

This test is designed to FAIL initially to prove we need circuit breaker protection
before implementing the circuit breaker system. It tests circuit states (CLOSED, OPEN,
HALF_OPEN), failure threshold triggering, automatic recovery, and API failure scenarios.
"""

import os
import time
import redis
import pytest
import requests
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from tests.utils.close_api import CloseAPI


class TestInstantlyCircuitBreaker:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Set up Redis for circuit breaker state tracking
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        try:
            self.redis_client = redis.from_url(self.redis_url)
            self.redis_client.ping()
            print(f"Successfully connected to Redis at: {self.redis_url}")
        except Exception as e:
            print(f"Warning: Failed to connect to Redis at {self.redis_url}: {e}")
            self.redis_client = None

        # Generate timestamp for unique testing
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.campaign_name = "CircuitBreakerTest"

        # Track circuit breaker keys for cleanup
        self.circuit_keys = []

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete test leads if they were created
        for lead_id in self.test_data.get("lead_ids", []):
            try:
                self.close_api.delete_lead(lead_id)
            except Exception as e:
                print(f"Warning: Could not delete test lead {lead_id}: {e}")

        # Clean up circuit breaker keys from Redis
        if self.redis_client:
            for key in self.circuit_keys:
                try:
                    self.redis_client.delete(key)
                except Exception as e:
                    print(f"Warning: Could not cleanup circuit breaker key {key}: {e}")

    def test_redis_connection_for_circuit_breaker(self):
        """Test that Redis connection is available for circuit breaker state storage."""
        if not self.redis_client:
            pytest.skip("Redis not available for circuit breaker testing")

        print("\n=== TESTING REDIS CONNECTION FOR CIRCUIT BREAKER ===")

        # Test operations needed for circuit breaker state management
        state_key = f"circuit_breaker_state:{self.timestamp}"
        self.circuit_keys.append(state_key)

        # Test setting circuit state
        self.redis_client.set(state_key, "CLOSED")
        state = self.redis_client.get(state_key)
        assert state.decode("utf-8") == "CLOSED", "Should store circuit state"

        # Test failure counter
        counter_key = f"circuit_breaker_failures:{self.timestamp}"
        self.circuit_keys.append(counter_key)

        count = self.redis_client.incr(counter_key)
        assert count == 1, "Should increment failure counter"

        # Test expiration for automatic reset
        self.redis_client.expire(counter_key, 2)
        ttl = self.redis_client.ttl(counter_key)
        assert ttl > 0, "Should set expiration for automatic reset"

        print("✅ Redis circuit breaker state operations working correctly")

    def test_circuit_breaker_class_exists(self):
        """Test that CircuitBreaker class exists - this should FAIL initially."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING CIRCUIT BREAKER CLASS EXISTENCE ===")

        # This test should FAIL because CircuitBreaker doesn't exist yet
        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail(
                "CircuitBreaker class not found in utils.circuit_breaker. "
                "This test is expected to FAIL initially until Step 4.2 "
                "(Implement Circuit Breaker) is completed."
            )

        # Test basic initialization
        circuit_breaker = CircuitBreaker(
            name=f"test_circuit_{self.timestamp}",
            failure_threshold=5,
            timeout=60,
            redis_client=self.redis_client,
        )

        assert circuit_breaker.name == f"test_circuit_{self.timestamp}"
        assert circuit_breaker.failure_threshold == 5
        assert circuit_breaker.timeout == 60

        print("✅ CircuitBreaker class exists and initializes correctly")

    def test_circuit_states_closed_open_half_open(self):
        """Test circuit breaker state transitions: CLOSED → OPEN → HALF_OPEN → CLOSED."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING CIRCUIT BREAKER STATE TRANSITIONS ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail(
                "CircuitBreaker class not found. "
                "This test is expected to FAIL initially until Step 4.2 is completed."
            )

        circuit_breaker = CircuitBreaker(
            name=f"test_states_{self.timestamp}",
            failure_threshold=3,
            timeout=2,  # Short timeout for testing
            redis_client=self.redis_client,
        )

        # Test 1: Initial state should be CLOSED
        assert circuit_breaker.get_state() == "CLOSED", "Initial state should be CLOSED"
        assert circuit_breaker.can_execute(), "Should allow execution when CLOSED"

        # Test 2: Record failures to trigger state change
        for i in range(3):
            circuit_breaker.record_failure()
            print(f"Recorded failure {i+1}/3")

        # Should now be OPEN
        assert (
            circuit_breaker.get_state() == "OPEN"
        ), "Should be OPEN after threshold failures"
        assert not circuit_breaker.can_execute(), "Should NOT allow execution when OPEN"

        # Test 3: Wait for timeout to trigger HALF_OPEN
        print("Waiting for timeout to trigger HALF_OPEN state...")
        time.sleep(3)  # Wait longer than timeout

        # First call after timeout should move to HALF_OPEN
        can_execute = circuit_breaker.can_execute()
        assert (
            circuit_breaker.get_state() == "HALF_OPEN"
        ), "Should be HALF_OPEN after timeout"
        assert can_execute, "Should allow limited execution when HALF_OPEN"

        # Test 4: Success in HALF_OPEN should return to CLOSED
        circuit_breaker.record_success()
        assert (
            circuit_breaker.get_state() == "CLOSED"
        ), "Should return to CLOSED after success"

        print("✅ Circuit breaker state transitions working correctly")

    def test_failure_threshold_triggering(self):
        """Test that circuit opens after reaching failure threshold."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING FAILURE THRESHOLD TRIGGERING ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail("CircuitBreaker class not found. Expected to FAIL initially.")

        # Test different threshold values
        for threshold in [1, 3, 5]:
            circuit_name = f"test_threshold_{threshold}_{self.timestamp}"
            circuit_breaker = CircuitBreaker(
                name=circuit_name,
                failure_threshold=threshold,
                timeout=60,
                redis_client=self.redis_client,
            )

            print(f"Testing failure threshold: {threshold}")

            # Should stay CLOSED before threshold
            for i in range(threshold - 1):
                circuit_breaker.record_failure()
                assert (
                    circuit_breaker.get_state() == "CLOSED"
                ), f"Should stay CLOSED at {i+1} failures"

            # Should open AT threshold
            circuit_breaker.record_failure()
            assert (
                circuit_breaker.get_state() == "OPEN"
            ), f"Should be OPEN at {threshold} failures"

        print("✅ Failure threshold triggering working correctly")

    def test_automatic_recovery_after_timeout(self):
        """Test automatic recovery from OPEN to HALF_OPEN after timeout."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING AUTOMATIC RECOVERY AFTER TIMEOUT ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail("CircuitBreaker class not found. Expected to FAIL initially.")

        circuit_breaker = CircuitBreaker(
            name=f"test_recovery_{self.timestamp}",
            failure_threshold=2,
            timeout=1,  # Very short timeout for quick testing
            redis_client=self.redis_client,
        )

        # Force circuit to OPEN state
        circuit_breaker.record_failure()
        circuit_breaker.record_failure()
        assert circuit_breaker.get_state() == "OPEN", "Should be OPEN after failures"

        # Record the time when circuit opened
        open_time = time.time()

        # Wait for timeout + small buffer
        time.sleep(1.5)

        # Circuit should allow recovery attempt (move to HALF_OPEN)
        can_execute = circuit_breaker.can_execute()
        assert (
            circuit_breaker.get_state() == "HALF_OPEN"
        ), "Should be HALF_OPEN after timeout"
        assert can_execute, "Should allow execution in HALF_OPEN"

        recovery_time = time.time()
        elapsed = recovery_time - open_time
        print(f"✅ Circuit recovered after {elapsed:.2f} seconds (timeout was 1s)")

    def test_api_failure_scenarios(self):
        """Test circuit breaker response to various API failure types."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING API FAILURE SCENARIOS ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail("CircuitBreaker class not found. Expected to FAIL initially.")

        circuit_breaker = CircuitBreaker(
            name=f"test_api_failures_{self.timestamp}",
            failure_threshold=3,
            timeout=60,
            redis_client=self.redis_client,
        )

        # Test different types of failures that should trigger circuit breaker
        failure_scenarios = [
            ("429", "Rate limit exceeded"),
            ("500", "Internal server error"),
            ("502", "Bad gateway"),
            ("503", "Service unavailable"),
            ("timeout", "Request timeout"),
            ("connection_error", "Connection error"),
        ]

        for error_type, description in failure_scenarios:
            # Reset circuit for each test
            circuit_breaker._reset_circuit()
            assert circuit_breaker.get_state() == "CLOSED", "Should start CLOSED"

            print(f"Testing failure scenario: {error_type} - {description}")

            # Simulate the specific failure type
            if error_type == "timeout":
                exception = requests.exceptions.Timeout("Request timed out")
            elif error_type == "connection_error":
                exception = requests.exceptions.ConnectionError("Connection failed")
            else:
                # HTTP error response
                response = Mock()
                response.status_code = int(error_type)
                response.text = description
                exception = requests.exceptions.HTTPError(
                    f"{error_type}: {description}"
                )

            # Record this type of failure multiple times
            for i in range(3):
                circuit_breaker.record_failure(exception)

            # Circuit should be OPEN after threshold failures
            assert (
                circuit_breaker.get_state() == "OPEN"
            ), f"Should be OPEN after {error_type} failures"

        print("✅ API failure scenarios handled correctly")

    def test_circuit_breaker_with_mocked_instantly_api(self):
        """Test circuit breaker integration with mocked Instantly API calls."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING CIRCUIT BREAKER WITH MOCKED INSTANTLY API ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail("CircuitBreaker class not found. Expected to FAIL initially.")

        circuit_breaker = CircuitBreaker(
            name=f"test_instantly_integration_{self.timestamp}",
            failure_threshold=3,
            timeout=5,
            redis_client=self.redis_client,
        )

        def mock_instantly_api_call():
            """Mock function that simulates Instantly API call."""
            if circuit_breaker.can_execute():
                # Simulate API call
                time.sleep(0.1)  # Simulate processing time

                # For this test, simulate failures initially
                if circuit_breaker.get_failure_count() < 3:
                    circuit_breaker.record_failure(
                        requests.exceptions.HTTPError("429: Rate limit exceeded")
                    )
                    raise requests.exceptions.HTTPError("429: Rate limit exceeded")
                else:
                    circuit_breaker.record_success()
                    return {"status": "success", "lead_id": "test_123"}
            else:
                raise Exception("Circuit breaker is OPEN - request blocked")

        # Test sequence: failures → circuit opens → recovery
        print("Testing API call sequence...")

        # First 3 calls should fail and trigger circuit breaker
        for i in range(3):
            try:
                result = mock_instantly_api_call()
                pytest.fail(f"Call {i+1} should have failed")
            except requests.exceptions.HTTPError:
                print(f"Call {i+1}: Expected API failure")

        # Circuit should now be OPEN
        assert (
            circuit_breaker.get_state() == "OPEN"
        ), "Circuit should be OPEN after failures"

        # Next call should be blocked by circuit breaker
        try:
            result = mock_instantly_api_call()
            pytest.fail("Call should have been blocked by circuit breaker")
        except Exception as e:
            assert "Circuit breaker is OPEN" in str(
                e
            ), "Should be blocked by circuit breaker"
            print("Call blocked by OPEN circuit breaker ✅")

        print("✅ Circuit breaker integration with mocked API working correctly")

    def test_exponential_backoff_implementation(self):
        """Test exponential backoff for failed requests."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING EXPONENTIAL BACKOFF IMPLEMENTATION ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail("CircuitBreaker class not found. Expected to FAIL initially.")

        circuit_breaker = CircuitBreaker(
            name=f"test_backoff_{self.timestamp}",
            failure_threshold=5,
            timeout=60,
            redis_client=self.redis_client,
            enable_backoff=True,  # Enable exponential backoff
        )

        # Test that backoff delays increase exponentially
        backoff_delays = []

        for i in range(5):
            start_time = time.time()

            # Record failure and get backoff delay
            circuit_breaker.record_failure()
            delay = circuit_breaker.get_backoff_delay()
            backoff_delays.append(delay)

            print(f"Failure {i+1}: Backoff delay = {delay:.3f}s")

        # Verify exponential growth pattern
        for i in range(1, len(backoff_delays)):
            assert (
                backoff_delays[i] >= backoff_delays[i - 1]
            ), f"Backoff should increase: {backoff_delays[i-1]} -> {backoff_delays[i]}"

        # Test that backoff resets after success
        circuit_breaker.record_success()
        reset_delay = circuit_breaker.get_backoff_delay()
        assert reset_delay < backoff_delays[-1], "Backoff should reset after success"

        print("✅ Exponential backoff implementation working correctly")

    def test_circuit_breaker_metrics_and_monitoring(self):
        """Test circuit breaker metrics collection for monitoring."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING CIRCUIT BREAKER METRICS AND MONITORING ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail("CircuitBreaker class not found. Expected to FAIL initially.")

        circuit_breaker = CircuitBreaker(
            name=f"test_metrics_{self.timestamp}",
            failure_threshold=3,
            timeout=60,
            redis_client=self.redis_client,
        )

        # Test initial metrics
        metrics = circuit_breaker.get_metrics()
        assert metrics["total_requests"] == 0, "Should start with 0 requests"
        assert metrics["successful_requests"] == 0, "Should start with 0 successes"
        assert metrics["failed_requests"] == 0, "Should start with 0 failures"
        assert metrics["state"] == "CLOSED", "Should start CLOSED"

        # Record some requests and verify metrics
        circuit_breaker.record_success()
        circuit_breaker.record_success()
        circuit_breaker.record_failure()

        updated_metrics = circuit_breaker.get_metrics()
        assert updated_metrics["total_requests"] == 3, "Should have 3 total requests"
        assert updated_metrics["successful_requests"] == 2, "Should have 2 successes"
        assert updated_metrics["failed_requests"] == 1, "Should have 1 failure"

        # Calculate success rate
        success_rate = (
            updated_metrics["successful_requests"] / updated_metrics["total_requests"]
        )
        assert (
            success_rate == 2 / 3
        ), f"Success rate should be 66.7%, got {success_rate:.1%}"

        print(f"✅ Circuit breaker metrics: {updated_metrics}")

    def test_multiple_circuit_breakers_independence(self):
        """Test that multiple circuit breakers operate independently."""
        if not self.redis_client:
            pytest.skip("Redis not available for this test")

        print("\n=== TESTING MULTIPLE CIRCUIT BREAKERS INDEPENDENCE ===")

        try:
            from utils.circuit_breaker import CircuitBreaker
        except ImportError:
            pytest.fail("CircuitBreaker class not found. Expected to FAIL initially.")

        # Create two independent circuit breakers
        circuit_1 = CircuitBreaker(
            name=f"test_circuit_1_{self.timestamp}",
            failure_threshold=2,
            timeout=60,
            redis_client=self.redis_client,
        )

        circuit_2 = CircuitBreaker(
            name=f"test_circuit_2_{self.timestamp}",
            failure_threshold=3,
            timeout=60,
            redis_client=self.redis_client,
        )

        # Both should start CLOSED
        assert circuit_1.get_state() == "CLOSED", "Circuit 1 should start CLOSED"
        assert circuit_2.get_state() == "CLOSED", "Circuit 2 should start CLOSED"

        # Trigger failures in circuit 1 only
        circuit_1.record_failure()
        circuit_1.record_failure()  # Should open circuit 1

        # Circuit 1 should be OPEN, circuit 2 should remain CLOSED
        assert circuit_1.get_state() == "OPEN", "Circuit 1 should be OPEN"
        assert circuit_2.get_state() == "CLOSED", "Circuit 2 should remain CLOSED"

        # Circuit 2 should still accept requests
        assert not circuit_1.can_execute(), "Circuit 1 should block requests"
        assert circuit_2.can_execute(), "Circuit 2 should allow requests"

        print("✅ Multiple circuit breakers operating independently")
