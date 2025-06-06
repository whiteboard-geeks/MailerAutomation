# Instantly API Timeout Handling Plan

## 🎯 Completion Status

- ✅ **Step 1**: Timeout Reproduction Tests - COMPLETED
- ✅ **Step 2**: Redis Rate Limiter Implementation - COMPLETED  
- ✅ **Step 3**: Request Queue System - COMPLETED
- ✅ **Step 4**: Circuit Breaker Pattern - COMPLETED
- ⏳ **Step 5**: Full Async Processing - TODO
- ⏳ **Step 6**: Progress Tracking - TODO
- ⏳ **Step 7**: Verification System - TODO  
- ⏳ **Step 8**: Scale Testing - TODO
- ⏳ **Step 9**: CI/CD Integration - TODO

### ✅ Recently Completed (Step 4)

**Circuit Breaker Pattern Implementation:**

- Created `utils/circuit_breaker.py` with `CircuitBreaker` class
- Implemented complete state machine: CLOSED → OPEN → HALF_OPEN → CLOSED
- Redis-backed state persistence for durability across restarts
- Exponential backoff with configurable failure thresholds and timeouts
- Comprehensive metrics collection for monitoring (total requests, success rate, etc.)
- Independent circuit breaker instances for different services
- Comprehensive test suite: `tests/integration/instantly/test_circuit_breaker.py`
- All tests passing (10/10) including state transitions, failure scenarios, and API integration
- Updated test comments to reflect working implementation (removed "expected to fail" language)

### ✅ Previously Completed (Step 3)

**Request Queue System Implementation:**

- Created `utils/async_queue.py` with `InstantlyRequestQueue` class
- Implemented Redis-based queue with worker pool (5 concurrent workers)
- Integration with existing Redis rate limiter from Step 2
- Future-based async processing with thread-safe operations
- Comprehensive test suite: `tests/integration/instantly/test_request_queue.py`
- Performance verified: 100 requests processed at 6.64 req/s (within 8 req/s safety limit)
- All tests passing (5/5) with proper rate limiting and error handling

## Problem Summary

The `/instantly/add_lead` endpoint is failing due to Heroku's 30-second timeout
when processing large batches of leads. We need to implement async processing
with proper rate limiting and verification.

### Key Facts

- Heroku times out after 30 seconds (non-negotiable)
- Instantly API limit: **600 requests/minute** (10 requests/second)
- Current CI/CD: lint → unit tests → deploy staging → integration tests →
  deploy production

## Incremental Test-First Approach

### Step 1: Create Timeout Test (First)

**Goal:** Replicate the timeout error before fixing anything

#### 1.1 Create Failing Test

- [x] Create test file:
  `tests/integration/instantly/test_timeout_reproduction.py`
- [x] Generate test leads with required fields:
  - `email`: `lance+{timestamp}+{i}@whiteboardgeeks.com`
  - `first_name`: `TestLead`
  - `last_name`: `{i}`
  - `company_name`: `Test Company {i}` (optional)
  - `date_location`: `Test Location {timestamp}` (optional)
- [x] Mock payload structure based on existing Close webhook format
- [x] Use existing test campaign or create one: `TimeoutTest{timestamp}`
- [x] Task text format: `Instantly: TimeoutTest{timestamp}`
- [x] Set HTTP request timeout to 30 seconds on `/add_lead` call
- [x] Test should fail with `requests.exceptions.Timeout` error
- [x] **IMPROVED**: Test now sends multiple webhook calls rapidly
  (simulating real Close behavior)

#### 1.2 Two-Stage Testing Strategy

##### Stage 1: Quick Validation (20 leads)

- [x] **CONCURRENT IMPLEMENTATION**:
  - [x] Add `concurrent.futures` import for ThreadPoolExecutor
  - [x] Create `send_webhook_request()` helper method for individual requests
  - [x] Replace sequential loop with ThreadPoolExecutor (max_workers=50)
  - [x] Send all webhook requests simultaneously to trigger faster timeouts
  - [x] Process results as they complete using `as_completed()`
- [x] **FAIL-FAST IMPLEMENTATION**:
  - [x] When first timeout detected in `as_completed()` loop, cancel all
    remaining futures
  - [x] Use `future.cancel()` on all futures in `future_to_lead` dictionary
  - [x] Raise AssertionError immediately with timeout details
  - [x] Automatic cleanup still occurs via `teardown_method()`
  - [x] Provides much faster test iterations during development
- [x] Test with 20 leads first for rapid iteration and feedback
- [x] Verify timeout behavior occurs quickly (~30-60 seconds)
- [x] Get baseline metrics for processing time per lead

### Step 2: Implement Redis-Based Rate Limiter (Fix #1)

**Goal:** Add centralized rate limiting using Redis for concurrent request coordination

#### 2.1 Create Rate Limiter Test

- [x] Create test file: `tests/integration/instantly/test_redis_rate_limiter.py`
- [x] Test Redis connection and basic operations
- [x] Test leaky bucket rate limiting algorithm
- [x] Test concurrent access scenarios:
  - [x] Multiple threads trying to acquire tokens simultaneously
  - [x] Verify only allowed number of requests proceed
  - [x] Test window expiration and token refresh
- [x] Test should initially FAIL (no rate limiter implemented)

#### 2.2 Implement Redis Rate Limiter

- [x] Create `utils/rate_limiter.py` with `RedisRateLimiter` class
- [x] Implement leaky bucket algorithm using Redis
- [x] Handle Redis connection failures with fallback
- [x] Add configuration for different API limits (Instantly: 600/minute)
- [x] Test should now PASS

#### 2.3 Integration Test

- [x] Modify timeout test to verify rate limiting works
- [x] Test with 700 leads: should see controlled request rate (≤10/second)
- [x] HTTP request should still timeout (proving we need more fixes)

### Step 3: Implement Request Queue System (Fix #2) ✅ COMPLETED

**Goal:** Add request queuing to handle bursts without overwhelming APIs

#### 3.1 Create Queue Test ✅ COMPLETED

- [x] Create test file: `tests/integration/instantly/test_request_queue.py`
- [x] Test queue creation and basic operations
- [x] Test worker pool functionality
- [x] Test queue processing under load:
  - [x] Queue 100 requests simultaneously
  - [x] Verify they're processed in controlled manner
  - [x] Measure processing rate and verify it respects limits (6.64 req/s avg)
- [x] Test should initially FAIL (no queue system implemented)

#### 3.2 Implement Request Queue ✅ COMPLETED

- [x] Create `utils/async_queue.py` with `InstantlyRequestQueue` class
- [x] Implement async worker pool (5 concurrent workers)
- [x] Integrate with Redis rate limiter from Step 2
- [x] Add request queuing with Future-based responses
- [x] Test should now PASS

#### 3.3 Integration Test ✅ COMPLETED

- [x] Test with 100 leads: all queued and processed without timeouts
- [x] Verify processing rate stays within limits (≤8 req/s safety margin)
- [x] Measure total processing time (15.1s for 100 requests)

### Step 4: Implement Circuit Breaker Pattern (Fix #3)

**Goal:** Add resilience against API failures and rate limit responses

#### 4.1 Create Circuit Breaker Test ✅ COMPLETED

- [x] Create test file: `tests/integration/instantly/test_circuit_breaker.py`
- [x] Test circuit states: CLOSED, OPEN, HALF_OPEN
- [x] Test failure threshold triggering
- [x] Test automatic recovery after timeout
- [x] Mock API failures (429, 500, timeout errors)
- [x] Test initially designed to FAIL (proved need for circuit breaker)

#### 4.2 Implement Circuit Breaker ✅ COMPLETED

- [x] Create `utils/circuit_breaker.py` with `CircuitBreaker` class
- [x] Implement state machine (CLOSED → OPEN → HALF_OPEN → CLOSED)
- [x] Add exponential backoff for failed requests
- [x] Redis-backed state persistence for cross-restart durability
- [x] Comprehensive metrics collection and monitoring
- [x] Support for independent multiple circuit breaker instances
- [x] All tests now PASS (10/10)

#### 4.3 Integration Test ✅ COMPLETED

- [x] Test resilience under simulated API failures
- [x] Verify circuit opens after threshold failures
- [x] Verify automatic recovery after timeout
- [x] Updated test comments to remove "expected to fail" language
- [x] Ready for integration with queue system from Step 3

### Step 5: Implement Full Async Processing (Fix #4)

**Goal:** Return success immediately, process in background using Celery

#### 5.1 Create Async Processing Test ✅ COMPLETED

- [x] Create test file: `tests/integration/instantly/test_async_processing.py`
- [x] Test immediate response (no HTTP timeout) - **FAILED AS EXPECTED** ✅
  - **Result**: Request 1: 4.33s, Request 2: Timeout after 5.00s
  - **Conclusion**: Endpoint is still processing synchronously (as expected)
  - **Note**: Current endpoint returns Close `task_id`, not Celery async task ID
- [ ] Test Celery task queuing and execution
- [ ] Test integration of all previous components:
  - [ ] Rate limiter + Queue + Circuit breaker + Async
- [x] Test should initially FAIL (endpoint still synchronous) ✅

#### 5.2 Implement Async Endpoint

- [ ] Modify `/instantly/add_lead` to queue Celery task immediately
- [ ] Create `process_lead_batch_task()` Celery task
- [ ] Integrate all rate limiting components in background task
- [ ] Return task_id immediately (before 30 second timeout)
- [ ] Test should now PASS (no timeout) but leads still get processed

#### 5.3 Integration Test

- [ ] Test async processing with 500 leads
- [ ] Update test to verify immediate success response (no HTTP timeout)
- [ ] Add polling to check task completion
- [ ] Verify leads eventually appear in Instantly

### Step 6: Add Progress Tracking (Enhancement #1)

**Goal:** Monitor large batch processing progress

#### 6.1 Progress Tracking System

- [ ] Add Redis tracking for batch progress
- [ ] Create endpoint: `/instantly/task/{task_id}/status`
- [ ] Track: total leads, processed, success, errors, rate_limit_delays
- [ ] Update test to use progress tracking

### Step 7: Add Verification System (Enhancement #2)

**Goal:** Confirm leads actually exist in Instantly

#### 7.1 Lead Verification

- [ ] Research Instantly API endpoints for listing leads
- [ ] Implement lead verification function
- [ ] Add cleanup mechanism (remove test leads)
- [ ] Test verifies all leads present before cleanup

### Step 8: Scale Testing (Validation)

**Goal:** Test with realistic load

#### 8.1 Scaled Load Tests

- [ ] **Foundation**: Test with 20 leads (quick validation)
- [ ] **Baseline**: Test with 200 leads (comprehensive validation)
- [ ] **Scale Up**: Test with 500 leads (CI safe)
- [ ] **Production Scale**: Test with 1,000 leads (staging only)
- [ ] **Stress Test**: Test with 2,000 leads (manual/staging)
- [ ] Measure processing time and success rates at each stage

### Step 9: CI/CD Integration

- [ ] Add tests to existing integration test matrix
- [ ] Run Foundation tests (20 leads) on staging environment first
- [ ] Run Baseline tests (200 leads) for comprehensive validation
- [ ] Expect initial tests to FAIL (proving we can reproduce the issue)
- [ ] Document timeout behavior and processing time metrics

## Current CI/CD Integration Plan

### Existing Pipeline

```yaml
lint → unit-tests → deploy-staging → integration-tests → deploy-production
```

### Integration Points

- Integration tests run on staging environment
- Tests access staging Redis and Instantly API
- Matrix testing: `[instantly, easypost, gmail]`

### New Test Additions

- Add timeout test to `instantly` integration tests
- Expect initial test to FAIL (proving reproduction)
- Each step should make test pass further
- Final step should have complete verification

## Rate Limiting Calculations

### Instantly Limits

600 requests/minute = 10 requests/second

### Safe Rate

8 requests/second (20% buffer) = 0.125 second delay

### Conservative Rate

5 requests/second = 0.2 second delay

### Timeout Math

- 30 seconds ÷ 0.2 seconds = 150 leads max before timeout
- Need async processing for >150 leads

## Success Metrics

### Step 1 Stage 1

Test fails with timeout at 20 leads (quick reproduction)

### Step 1 Stage 2

Test fails with timeout at 200 leads (comprehensive reproduction)

### Step 2

Test fails with timeout but requests/sec ≤ 8 (both stages)

### Step 3

Test passes, returns immediately, processes async (both stages)

### Step 4

Progress tracking shows completion status

### Step 5

Verification confirms 100% of leads in Instantly

### Step 6

Handles progression: 20 → 200 → 500 → 1000+ leads with 95%+ success

## Notes from Discussion

- Heroku times out after 30 seconds - this is non-negotiable
- Must return success quickly, then process async
- Verification is critical - can't trust that async processing completed
- Consider using `lance+datetime+i@whiteboardgeeks.com` format for test emails
- Need Instantly "remove lead" API endpoint for test cleanup
- Current code already has some rate limiting (0.5s delay in campaign fetching)

## Next Steps

1. Review this plan with team
2. Get Instantly API documentation for remove/list endpoints
3. Set up test environment with Redis and Celery

## Rate Limiting Architecture Overview

### Component Integration

```text
Request → Redis Rate Limiter → Request Queue → Circuit Breaker → Instantly API
    ↓              ↓                ↓              ↓
 Immediate      Coordinate      Handle Bursts   Handle Failures
 Response       Across          Without         Gracefully
               Instances       Timeouts
```

### Test Progression Strategy

Each step builds on the previous:

1. **Step 2**: Rate limiter works, but still times out (proves rate limiting)
2. **Step 3**: Queue + Rate limiter, controlled processing, still times out
3. **Step 4**: + Circuit breaker, resilient to failures, still times out  
4. **Step 5**: + Async processing, NO timeout, background processing
5. **Step 6+**: Enhancements and validation

### Per-Step Success Criteria

#### Step 2 (Redis Rate Limiter) ✅ COMPLETED

- [x] Rate limiter test passes in isolation
- [x] Integration test shows ≤10 requests/second to Instantly
- [x] HTTP timeout still occurs (proving more fixes needed)

#### Step 3 (Request Queue) ✅ COMPLETED

- [x] Queue test passes in isolation
- [x] Can queue 100+ requests without immediate API calls
- [x] Processing rate controlled by rate limiter (6.64 req/s average)
- [x] Future-based async processing implemented

#### Step 4 (Circuit Breaker) ✅ COMPLETED

- [x] Circuit breaker test passes in isolation (10/10 tests passing)
- [x] System recovers from simulated API failures
- [x] Exponential backoff working with Redis persistence  
- [x] State machine transitions working (CLOSED → OPEN → HALF_OPEN → CLOSED)
- [x] Independent circuit breaker instances for multiple services
- [x] Comprehensive metrics collection for monitoring
- [x] Ready for integration with async processing

#### Step 5 (Async Processing)

- [ ] Async test passes
- [ ] HTTP request returns immediately (no timeout)
- [ ] Background processing completes successfully
- [ ] All rate limiting components working together
