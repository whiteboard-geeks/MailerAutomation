# Close.com Dynamic Rate Limiter Implementation Plan

## Problem Statement

We're hitting Close.com API rate limits (429 errors) when processing email webhooks from Instantly. Different Close endpoints have different rate limits that aren't known in advance:

- `/api/v1/me/` → `limit=160; remaining=159; reset=8`
- `/api/v1/data/search/` → `limit=16; remaining=14; reset=1`

## Solution Overview

Implement a **dynamic rate limiter** that learns and adapts to different endpoint limits by parsing the `ratelimit` headers from Close's responses.

### Key Features

1. **Endpoint-Specific Rate Limiting**: Each Close endpoint gets its own rate limit bucket
2. **Dynamic Limit Discovery**: Parse `ratelimit` headers to learn actual limits
3. **Response Header Integration**: Update rate limits based on actual API responses
4. **Fallback Safety**: Conservative defaults when limits are unknown
5. **Redis-Based**: Works across multiple app instances

## Architecture Components

### 1. Header Parser

```python
def parse_close_ratelimit_header(header_value):
    """Parse Close's ratelimit header format"""
    # Input: "limit=160; remaining=159; reset=8"
    # Output: {"limit": 160, "remaining": 159, "reset": 8}
```

### 2. Endpoint Key Extractor

```python
def extract_endpoint_key(url):
    """Extract consistent endpoint key from URL"""
    # https://api.close.com/api/v1/data/search/ -> /api/v1/data/search/
    # https://api.close.com/api/v1/lead/lead_123/ -> /api/v1/lead/{id}/
```

### 3. Dynamic Rate Limiter Class

```python
class CloseRateLimiter(RedisRateLimiter):
    def update_from_response_headers(self, endpoint, response):
        """Parse ratelimit header and update endpoint-specific limits"""
    
    def acquire_token_for_endpoint(self, endpoint):
        """Get token for specific endpoint with its own limits"""
```

### 4. Enhanced Close API Wrapper

```python
@dynamic_rate_limit
def make_close_request(method, url, **kwargs):
    """Enhanced Close API request with dynamic rate limiting"""
    # 1. Extract endpoint from URL
    # 2. Apply rate limiting before request
    # 3. Make request
    # 4. Parse response headers after request
    # 5. Update rate limiter with discovered limits
```

## Redis Key Structure

```txt
close_rate_limit:endpoint:/api/v1/data/search/  # Token bucket for search endpoint
close_rate_limit:endpoint:/api/v1/lead/{id}/    # Token bucket for lead endpoint
close_rate_limit:limits:/api/v1/data/search/    # Discovered limits cache
close_rate_limit:limits:/api/v1/lead/{id}/      # Discovered limits cache
```

## Implementation Plan (TDD Approach)

### Phase 1: Core Components (Unit Tests)

#### 1.1 Header Parsing ✅ COMPLETED

- **File**: `utils/rate_limiter.py` (added to existing file)
- **Tests**: `tests/unit/close_rate_limiter/test_header_parsing.py`
- **Function**: `parse_close_ratelimit_header()`

**Test Cases** (All 13 tests passing):

- ✅ Valid header: `"limit=160; remaining=159; reset=8"`
- ✅ Different parameter order
- ✅ Zero remaining tokens
- ✅ Extra whitespace handling
- ✅ Malformed headers (missing values, invalid format)
- ✅ Non-numeric values
- ✅ Empty/None headers
- ✅ Missing required fields
- ✅ Additional fields (ignored properly)
- ✅ Float to integer conversion
- ✅ Case insensitive parsing

**Implementation Details**:

- Function added to `utils/rate_limiter.py` instead of creating separate file
- Handles all edge cases with proper error messages
- Ignores additional non-numeric fields while preserving required fields
- Returns dictionary with `limit`, `remaining`, and `reset` as integers

#### 1.2 Endpoint Extraction ✅ COMPLETED

- **File**: `utils/rate_limiter.py` (added to existing file)
- **Tests**: `tests/unit/close_rate_limiter/test_endpoint_extraction.py`
- **Function**: `extract_endpoint_key()`

**Test Cases** (All 17 test methods passing):

- ✅ Static endpoints: `/api/v1/data/search/`
- ✅ Dynamic endpoints: `/api/v1/lead/lead_123/` → `/api/v1/lead/`
- ✅ Task endpoints: `/api/v1/task/task_456/` → `/api/v1/task/`
- ✅ Edge cases and malformed URLs
- ✅ URL variations (query params, fragments, HTTP vs HTTPS)
- ✅ Trailing slash normalization
- ✅ Input validation (None, empty, non-string)
- ✅ Error handling (malformed URLs, non-Close URLs)
- ✅ API version validation
- ✅ Complex nested resources
- ✅ Case sensitivity handling

**Implementation Details**:

- Function added to `utils/rate_limiter.py` alongside header parsing
- Handles all URL variations and edge cases robustly
- Preserves original case in output while doing case-insensitive validation
- Maps resource endpoints to root paths (e.g., `/api/v1/lead/lead_123/` → `/api/v1/lead/`)
- Preserves full paths for static endpoints (e.g., `/api/v1/data/search/`)
- Comprehensive error handling with clear error messages

#### 1.3 Rate Limiter Core Logic

- **File**: `utils/rate_limiter.py` (added to existing file)
- **Tests**: `tests/unit/close_rate_limiter/test_rate_limiter_core.py`
- **Class**: `CloseRateLimiter`

**Test Cases**:

- First call to unknown endpoint (uses conservative default)
- Subsequent calls use discovered limits
- Response header updates
- Invalid headers don't break existing limits
- Multiple endpoints with different limits

### Phase 2: Integration with close_utils.py

#### 2.1 Enhanced make_close_request()

- **File**: `close_utils.py`
- **Tests**: `tests/unit/close_rate_limiter/test_close_utils_integration.py`

**Changes**:

- Replace `@retry_with_backoff` with `@close_rate_limit`
- Add header parsing after each response
- Maintain backward compatibility

**Test Cases**:

- Rate limiting is applied before requests
- Headers are parsed after responses
- Limits are updated correctly
- Existing functionality unchanged

### Phase 3: Integration Tests

#### 3.1 Multi-Endpoint Testing

- **File**: `tests/integration/close_rate_limiter/test_multi_endpoint.py`

**Test Cases**:

- Different endpoints have different limits
- Limits are discovered independently
- Rate limiting works across endpoints

#### 3.2 Real API Integration

- **File**: `tests/integration/close_rate_limiter/test_real_api_calls.py`

**Test Cases**:

- Actual Close API calls respect rate limits
- Headers are parsed from real responses
- Rate limiting prevents 429 errors

### Phase 4: Integration with tests/utils/close_api.py

#### 4.1 Enhanced Test Helper

- **File**: `tests/utils/close_api.py`
- **Method**: `_make_request_with_retry()`

**Changes**:

- Integrate with new rate limiter
- Maintain existing retry logic for other errors
- Keep all existing test helper methods unchanged

## File Structure

```txt
utils/
├── rate_limiter.py              # Enhanced with Close header parsing ✅
└── close_rate_limiter.py        # New Close-specific rate limiter (TBD)

tests/unit/close_rate_limiter/
├── test_header_parsing.py       # Test header parsing logic ✅
├── test_endpoint_extraction.py  # Test URL -> endpoint key logic
├── test_rate_limiter_core.py   # Test core rate limiting logic
└── test_close_utils_integration.py # Test integration with close_utils

tests/integration/close_rate_limiter/
├── test_multi_endpoint.py       # Test different endpoints
└── test_real_api_calls.py       # Test with actual Close API

close_utils.py                   # Enhanced with rate limiting
tests/utils/close_api.py         # Enhanced test helpers
```

## Benefits

1. **Self-Learning**: Discovers actual rate limits automatically
2. **Endpoint-Specific**: Different limits for different endpoints
3. **Redis-Based**: Works across multiple app instances
4. **Conservative Start**: Begins with safe defaults
5. **Adaptive**: Adjusts to Close's actual limits over time
6. **Backward Compatible**: Existing code continues to work
7. **Testable**: Comprehensive test coverage with TDD approach

## Risk Mitigation

1. **Conservative Defaults**: Start with very low limits (1 req/sec) for unknown endpoints
2. **Fallback Behavior**: If Redis fails, fall back to in-memory rate limiting
3. **Gradual Rollout**: Can be enabled/disabled via environment variable
4. **Monitoring**: Log rate limit discoveries and adjustments
5. **Safety Factor**: Apply 80% safety margin to discovered limits

## Success Metrics

1. **Elimination of 429 Errors**: No more rate limit errors in logs
2. **Improved Throughput**: Better utilization of available rate limits
3. **Automatic Adaptation**: System learns new endpoint limits without code changes
4. **Zero Downtime**: Implementation doesn't break existing functionality

## Timeline

- **Phase 1**: 2-3 days (Core components with unit tests)
- **Phase 2**: 1-2 days (Integration with close_utils.py)
- **Phase 3**: 1-2 days (Integration tests)
- **Phase 4**: 1 day (Test helper integration)

**Total Estimated Time**: 5-8 days

## Next Steps

1. ✅ ~~Create failing unit tests for header parsing~~
2. ✅ ~~Implement header parsing to make tests pass~~
3. ✅ ~~Create failing unit tests for endpoint extraction~~
4. ✅ ~~Implement endpoint extraction to make tests pass~~
5. **NEXT**: Create failing unit tests for rate limiter core logic
6. Implement rate limiter core logic to make tests pass
7. Continue TDD approach through remaining phases
8. Deploy with feature flag for gradual rollout

## Progress Summary

**Phase 1.1 COMPLETED** ✅
**Phase 1.2 COMPLETED** ✅

- Header parsing function implemented and fully tested (13 test cases passing)
- Endpoint extraction function implemented and fully tested (17 test methods passing)
- Both functions handle all edge cases robustly with comprehensive error handling
- Functions work together to provide the foundation for Close API rate limiting
- Ready to proceed to Phase 1.3 (Rate Limiter Core Logic)
