"""
Circuit Breaker Pattern Implementation

Provides fault tolerance and automatic recovery for external API calls.
Implements the three-state pattern: CLOSED → OPEN → HALF_OPEN → CLOSED

States:
- CLOSED: Normal operation, all requests allowed
- OPEN: Circuit is open, all requests blocked until timeout
- HALF_OPEN: Limited requests allowed to test service recovery
"""

import time
import redis
from typing import Optional, Dict, Any


class CircuitBreaker:
    """
    Circuit breaker implementation with Redis-backed state persistence.

    Protects against cascading failures by monitoring request success/failure rates
    and automatically blocking requests when failure threshold is exceeded.
    """

    # Circuit breaker states
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        timeout: int = 60,
        redis_client: redis.Redis = None,
        enable_backoff: bool = False,
    ):
        """
        Initialize circuit breaker.

        Args:
            name: Unique identifier for this circuit breaker
            failure_threshold: Number of failures before opening circuit
            timeout: Seconds to wait before attempting recovery (OPEN → HALF_OPEN)
            redis_client: Redis client for state persistence
            enable_backoff: Whether to enable exponential backoff
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.redis_client = redis_client
        self.enable_backoff = enable_backoff

        # Redis keys for state persistence
        self.state_key = f"circuit_breaker_state:{self.name}"
        self.failure_count_key = f"circuit_breaker_failures:{self.name}"
        self.last_failure_key = f"circuit_breaker_last_failure:{self.name}"
        self.metrics_key = f"circuit_breaker_metrics:{self.name}"
        self.backoff_key = f"circuit_breaker_backoff:{self.name}"

        # Initialize circuit in CLOSED state if not already set
        if self.redis_client and not self.redis_client.exists(self.state_key):
            self._reset_circuit()

    def get_state(self) -> str:
        """Get current circuit breaker state."""
        if not self.redis_client:
            return self.CLOSED

        state = self.redis_client.get(self.state_key)
        if state:
            return state.decode("utf-8")
        return self.CLOSED

    def can_execute(self) -> bool:
        """
        Check if request execution is allowed.

        Returns:
            True if request can be executed, False otherwise
        """
        current_state = self.get_state()

        if current_state == self.CLOSED:
            return True
        elif current_state == self.OPEN:
            # Check if timeout has passed for recovery attempt
            if self._should_attempt_reset():
                self._set_state(self.HALF_OPEN)
                return True
            return False
        elif current_state == self.HALF_OPEN:
            # Allow limited execution in half-open state
            return True

        return False

    def record_failure(self, exception: Optional[Exception] = None) -> None:
        """
        Record a failure and update circuit state accordingly.

        Args:
            exception: Optional exception that caused the failure
        """
        if not self.redis_client:
            return

        # Increment failure counter
        failure_count = self.redis_client.incr(self.failure_count_key)

        # Record timestamp of last failure
        self.redis_client.set(self.last_failure_key, int(time.time()))

        # Update metrics
        self._update_metrics(success=False)

        # Update backoff if enabled
        if self.enable_backoff:
            self._update_backoff()

        current_state = self.get_state()

        if current_state == self.CLOSED:
            # Check if we should open the circuit
            if failure_count >= self.failure_threshold:
                self._set_state(self.OPEN)
                print(
                    f"Circuit breaker '{self.name}' opened after {failure_count} failures"
                )

        elif current_state == self.HALF_OPEN:
            # Failure in half-open state returns to open
            self._set_state(self.OPEN)
            print(f"Circuit breaker '{self.name}' returned to OPEN state after failure")

    def record_success(self) -> None:
        """Record a successful request and update circuit state."""
        if not self.redis_client:
            return

        # Update metrics
        self._update_metrics(success=True)

        # Reset backoff on success
        if self.enable_backoff:
            self.redis_client.delete(self.backoff_key)

        current_state = self.get_state()

        if current_state == self.HALF_OPEN:
            # Success in half-open state closes the circuit
            self._reset_circuit()
            print(f"Circuit breaker '{self.name}' closed after successful recovery")
        elif current_state == self.CLOSED:
            # Reset failure counter on success in closed state
            self.redis_client.delete(self.failure_count_key)

    def get_failure_count(self) -> int:
        """Get current failure count."""
        if not self.redis_client:
            return 0

        count = self.redis_client.get(self.failure_count_key)
        return int(count) if count else 0

    def get_backoff_delay(self) -> float:
        """
        Get exponential backoff delay in seconds.

        Returns:
            Delay in seconds (exponential backoff based on failure count)
        """
        if not self.enable_backoff or not self.redis_client:
            return 0.0

        backoff_level = self.redis_client.get(self.backoff_key)
        if not backoff_level:
            return 0.0

        level = int(backoff_level)
        # Exponential backoff: 2^level seconds, capped at 300 seconds (5 minutes)
        delay = min(2**level, 300)
        return float(delay)

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get circuit breaker metrics.

        Returns:
            Dictionary containing metrics data
        """
        if not self.redis_client:
            return {
                "total_requests": 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "state": self.CLOSED,
                "failure_count": 0,
            }

        metrics_data = self.redis_client.hgetall(self.metrics_key)

        # Convert bytes to appropriate types
        total_requests = int(metrics_data.get(b"total_requests", 0))
        successful_requests = int(metrics_data.get(b"successful_requests", 0))
        failed_requests = int(metrics_data.get(b"failed_requests", 0))

        return {
            "total_requests": total_requests,
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "state": self.get_state(),
            "failure_count": self.get_failure_count(),
            "success_rate": successful_requests / total_requests
            if total_requests > 0
            else 0.0,
        }

    def _set_state(self, state: str) -> None:
        """Set circuit breaker state in Redis."""
        if self.redis_client:
            self.redis_client.set(self.state_key, state)

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt circuit reset."""
        if not self.redis_client:
            return False

        last_failure_time = self.redis_client.get(self.last_failure_key)
        if not last_failure_time:
            return True

        time_since_failure = time.time() - int(last_failure_time)
        return time_since_failure >= self.timeout

    def _reset_circuit(self) -> None:
        """Reset circuit to initial CLOSED state."""
        if not self.redis_client:
            return

        self._set_state(self.CLOSED)
        self.redis_client.delete(self.failure_count_key)
        self.redis_client.delete(self.last_failure_key)
        if self.enable_backoff:
            self.redis_client.delete(self.backoff_key)

    def _update_metrics(self, success: bool) -> None:
        """Update request metrics in Redis."""
        if not self.redis_client:
            return

        # Use Redis hash to store metrics
        self.redis_client.hincrby(self.metrics_key, "total_requests", 1)

        if success:
            self.redis_client.hincrby(self.metrics_key, "successful_requests", 1)
        else:
            self.redis_client.hincrby(self.metrics_key, "failed_requests", 1)

    def _update_backoff(self) -> None:
        """Update exponential backoff level."""
        if not self.redis_client:
            return

        self.redis_client.incr(self.backoff_key)
        # Set expiration to prevent indefinite backoff
        self.redis_client.expire(self.backoff_key, 3600)  # 1 hour max
