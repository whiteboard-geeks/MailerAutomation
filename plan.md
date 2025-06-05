# Instantly API Timeout Handling Plan

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

- [ ] **CONCURRENT IMPLEMENTATION**:
  - [ ] Add `concurrent.futures` import for ThreadPoolExecutor
  - [ ] Create `send_webhook_request()` helper method for individual requests
  - [ ] Replace sequential loop with ThreadPoolExecutor (max_workers=50)
  - [ ] Send all 200 webhook requests simultaneously to trigger faster timeouts
  - [ ] Process results as they complete using `as_completed()`
- [ ] **FAIL-FAST IMPLEMENTATION**:
  - [ ] When first timeout detected in `as_completed()` loop, cancel all
    remaining futures
  - [ ] Use `future.cancel()` on all futures in `future_to_lead` dictionary
  - [ ] Raise AssertionError immediately with timeout details
  - [ ] Automatic cleanup still occurs via `teardown_method()`
  - [ ] Provides much faster test iterations during development
- [ ] Test with 20 leads first for rapid iteration and feedback
- [ ] Modify test to use `generate_test_leads(20)` temporarily
- [ ] Verify timeout behavior occurs quickly (~30-60 seconds)
- [ ] Get baseline metrics for processing time per lead

##### Stage 2: Comprehensive Testing (200 leads)

- [ ] Scale up to 200 leads for full reproduction
- [ ] Use default `generate_test_leads(200)`
- [ ] Measure timeout threshold and processing patterns
- [ ] Document complete failure scenario for fixing

- [ ] Verify test runs in existing CI/CD pipeline

#### 1.3 CI/CD Integration

- [ ] Add test to existing integration test matrix
- [ ] Run Stage 1 (20 leads) on staging environment first
- [ ] Run Stage 2 (200 leads) for comprehensive validation
- [ ] Expect both stages to FAIL (proving we can reproduce the issue)
- [ ] Document timeout behavior and processing time metrics

### Step 2: Implement Basic Rate Limiting (Fix #1)

**Goal:** Stay under Instantly's 600 requests/minute limit

#### 2.1 Add Rate Limiting to Existing Code

- [ ] Add delay between API calls in `add_to_instantly_campaign()`
- [ ] Calculate: 600/min = 10/second = 0.1 second delay minimum
- [ ] Start with 0.2 second delay for safety margin
- [ ] Handle 429 errors with exponential backoff

#### 2.2 Test Rate Limiting

- [ ] **Stage 1**: Test rate limiting with 20 leads first
  - [ ] Modify timeout test to include timing verification
  - [ ] HTTP request should still timeout after 30 seconds
  - [ ] Measure: requests/second should be ≤8 (within rate limit)
- [ ] **Stage 2**: Test rate limiting with 200 leads
  - [ ] Confirm rate limiting still works at scale
  - [ ] Document processing time improvements

### Step 3: Implement Async Processing (Fix #2)

**Goal:** Return success immediately, process in background

#### 3.1 Basic Celery Implementation

- [ ] Modify `/instantly/add_lead` to queue task instead of processing
  immediately
- [ ] Create simple Celery task: `process_lead_batch_task()`
- [ ] Return task_id immediately (before 30 second timeout)
- [ ] Test should now PASS (no timeout) but leads still get processed

#### 3.2 Test Async Processing

- [ ] **Stage 1**: Test async processing with 20 leads
  - [ ] Update test to verify immediate success response (no HTTP timeout)
  - [ ] Add polling to check task completion
  - [ ] Verify leads eventually appear in Instantly
- [ ] **Stage 2**: Test async processing with 200 leads
  - [ ] Confirm async processing scales properly
  - [ ] Test runs in CI/CD pipeline and passes

### Step 4: Add Progress Tracking (Enhancement #1)

**Goal:** Monitor large batch processing progress

#### 4.1 Progress Tracking System

- [ ] Add Redis tracking for batch progress
- [ ] Create endpoint: `/instantly/task/{task_id}/status`
- [ ] Track: total leads, processed, success, errors
- [ ] Update test to use progress tracking

### Step 5: Add Verification System (Enhancement #2)

**Goal:** Confirm leads actually exist in Instantly

#### 5.1 Lead Verification

- [ ] Research Instantly API endpoints for listing leads
- [ ] Implement lead verification function
- [ ] Add cleanup mechanism (remove test leads)
- [ ] Test verifies all leads present before cleanup

### Step 6: Scale Testing (Validation)

**Goal:** Test with realistic load

#### 6.1 Scaled Load Tests

- [ ] **Foundation**: Test with 20 leads (quick validation)
- [ ] **Baseline**: Test with 200 leads (comprehensive validation)
- [ ] **Scale Up**: Test with 500 leads (CI safe)
- [ ] **Production Scale**: Test with 1,000 leads (staging only)
- [ ] **Stress Test**: Test with 2,000 leads (manual/staging)
- [ ] Measure processing time and success rates at each stage

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
