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

#### 1.3 Rate Limiter Core Logic ✅ COMPLETED

- **File**: `utils/rate_limiter.py` (added to existing file)
- **Tests**: `tests/unit/close_rate_limiter/test_rate_limiter_core.py`
- **Class**: `CloseRateLimiter`

**Test Cases** (All 21 tests passing):

- ✅ First call to unknown endpoint (uses conservative default)
- ✅ Subsequent calls use discovered limits
- ✅ Response header updates
- ✅ Invalid headers don't break existing limits
- ✅ Multiple endpoints with different limits
- ✅ Endpoint-specific rate limiting isolation
- ✅ Dynamic limit discovery from headers
- ✅ Limit persistence and retrieval in Redis
- ✅ Safety factor application to discovered limits
- ✅ Integration with existing RedisRateLimiter
- ✅ Fallback behavior when Redis unavailable
- ✅ Redis key structure for endpoints

**Implementation Details**:

- `CloseRateLimiter` class extends `RedisRateLimiter` with endpoint-specific functionality
- Conservative default rate (1 req/sec) for unknown endpoints
- Dynamic limit discovery from Close API response headers
- Endpoint-specific Redis buckets with key structure: `close_rate_limit:limits:/api/v1/endpoint/`
- Safety factor applied to discovered limits (default 80%)
- Comprehensive error handling and fallback mechanisms
- All methods properly tested with mocked Redis client

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

### Phase 3: Integration Tests (Real Redis + Real API Testing)

#### 3.1 Redis Integration Tests

- **File**: `tests/integration/close_rate_limiter/test_redis_integration.py`
- **Purpose**: Test with real Redis instance (not mocked)

**Test Cases**:

- Real Redis connection and basic operations
- Redis key structure validation: `close_rate_limit:limits:/api/v1/endpoint/` and `close_endpoint:/api/v1/endpoint/`
- Endpoint-specific bucket isolation in Redis
- Limit caching and expiration (1-hour cache)
- Fallback behavior when Redis connection fails
- Atomic Redis operations for thread safety
- Redis pipeline operations for batch processing
- Cross-process rate limiting validation

**Infrastructure Requirements**:

- Real Redis instance (using `REDISCLOUD_URL` environment variable)
- Redis key cleanup in test teardown
- Connection error simulation for fallback testing

#### 3.2 Multi-Endpoint Rate Limiting Tests

- **File**: `tests/integration/close_rate_limiter/test_multi_endpoint.py`
- **Purpose**: Validate endpoint isolation and independent rate limiting

**Test Cases**:

- Different endpoints have completely separate rate limit buckets
- `/api/v1/lead/` vs `/api/v1/data/search/` independence
- Endpoint key extraction works with real URL variations
- Concurrent access to different endpoints doesn't interfere
- Rate limit discovery is endpoint-specific
- Safety factor applied independently per endpoint
- Cache expiration works per endpoint

**Test Scenarios**:

- Exhaust rate limit on `/api/v1/lead/` → `/api/v1/data/search/` still works
- Discover different limits for different endpoints simultaneously
- Verify Redis keys are properly namespaced by endpoint

#### 3.3 Dynamic Limit Discovery Integration Tests

- **File**: `tests/integration/close_rate_limiter/test_dynamic_discovery.py`
- **Purpose**: Test real-world limit discovery and caching

**Test Cases**:

- Mock Close API responses with realistic rate limit headers
- Parse headers: `"limit=160; remaining=159; reset=8"` vs `"limit=16; remaining=14; reset=1"`
- Safety factor application (80% of discovered limits)
- Cache persistence across rate limiter instances
- Cache expiration and refresh behavior
- Behavior before vs after limit discovery
- Invalid header handling doesn't break existing limits
- Multiple concurrent limit discoveries

**Mock Response Scenarios**:

```python
# High-limit endpoint (like /api/v1/me/)
mock_response.headers = {"ratelimit": "limit=160; remaining=159; reset=8"}

# Low-limit endpoint (like /api/v1/data/search/)
mock_response.headers = {"ratelimit": "limit=16; remaining=14; reset=1"}
```

#### 3.4 Time-Based Rate Limiting Integration Tests

- **File**: `tests/integration/close_rate_limiter/test_timing_behavior.py`
- **Purpose**: Validate actual rate enforcement over time

**Test Cases**:

- Conservative default (1 req/sec) enforcement timing
- Discovered limit (e.g., 128 req/min) enforcement timing
- Token replenishment at correct intervals
- Sustained rate limiting over extended periods
- Burst behavior with leaky bucket algorithm
- Rate convergence to effective limits over time
- Window expiration and reset behavior

**Timing Measurements**:

- Measure actual request rates vs configured rates
- Validate safety factor reduces effective rate correctly
- Test rate limiting accuracy over 30+ second periods

#### 3.5 Concurrent Access & Thread Safety Tests

- **File**: `tests/integration/close_rate_limiter/test_concurrent_access.py`
- **Purpose**: Validate thread safety and atomic operations

**Test Cases**:

- Multiple threads accessing same endpoint simultaneously
- Atomic Redis operations prevent race conditions
- Concurrent limit updates don't corrupt data
- Cross-process rate limiting (multiple app instances)
- Pipeline operations maintain consistency
- Watch/Multi/Exec Redis transactions work correctly

**Concurrency Scenarios**:

- 20+ threads hitting same endpoint simultaneously
- Multiple processes updating limits concurrently
- Stress testing with high request volumes

#### 3.6 **REAL API INTEGRATION TESTS** 🔥

- **File**: `tests/integration/close_rate_limiter/test_real_api_calls.py`
- **Purpose**: Test with actual Close.com API calls using real data

**IMPORTANT**: Requires `CLOSE_API_KEY` environment variable

**Real API Test Cases**:

- **Real Close API authentication and connection**
- **Actual rate limit header parsing from live responses**
- **End-to-end flow**: request → rate limit → API call → header parsing → limit update
- **Multiple real endpoints**: `/api/v1/me/`, `/api/v1/data/search/`, `/api/v1/lead/`
- **Rate limiting prevents actual 429 errors**
- **Dynamic limit discovery from real Close API responses**
- **Safety factor application with real discovered limits**
- **Real-world timing and rate enforcement**

**Real API Test Scenarios**:

```python
def test_real_close_api_rate_limiting():
    """Test with actual Close.com API calls"""
    # Use real Close API key
    close_api_key = os.environ.get("CLOSE_API_KEY")
    
    # Test real endpoints
    endpoints_to_test = [
        "https://api.close.com/api/v1/me/",
        "https://api.close.com/api/v1/data/search/",
        "https://api.close.com/api/v1/lead/"  # Will need real lead ID
    ]
    
    # Make actual API calls and verify:
    # 1. Rate limiting is applied
    # 2. Real headers are parsed
    # 3. Limits are discovered and cached
    # 4. No 429 errors occur
    # 5. Different endpoints have different limits
```

**Real Data Integration**:

- Use actual Close.com lead data for testing
- Test with real lead IDs, contact IDs, etc.
- Validate rate limiting works with production-like data volumes
- Test error handling with real API error responses

**Safety Measures for Real API Testing**:

- Use test Close.com account (not production)
- Implement request throttling to avoid overwhelming API
- Clean up any test data created during tests
- Skip real API tests if `CLOSE_API_KEY` not provided
- Add `@pytest.mark.integration` decorator for optional execution

#### 3.7 Performance & Load Testing

- **File**: `tests/integration/close_rate_limiter/test_performance.py`
- **Purpose**: Validate performance under realistic loads

**Test Cases**:

- High-volume request processing (1000+ requests)
- Memory usage with large numbers of endpoints
- Redis performance with many concurrent buckets
- Rate limiter overhead measurement
- Cache hit/miss ratios
- Performance comparison: with vs without rate limiting

#### 3.8 Error Handling & Resilience Tests

- **File**: `tests/integration/close_rate_limiter/test_error_handling.py`
- **Purpose**: Test error scenarios and recovery

**Test Cases**:

- Redis connection failures during operation
- Malformed API responses and headers
- Network timeouts and retries
- Invalid endpoint URLs
- Redis memory pressure scenarios
- Graceful degradation when services fail

**Integration Test Infrastructure**:

```python
# Base test class for integration tests
class BaseCloseRateLimiterIntegrationTest:
    def setup_method(self):
        # Real Redis connection
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        self.redis_client = redis.from_url(self.redis_url)
        self.test_keys = []  # Track for cleanup
        
        # Real Close API (if available)
        self.close_api_key = os.environ.get("CLOSE_API_KEY")
        
    def teardown_method(self):
        # Clean up Redis keys
        for key in self.test_keys:
            self.redis_client.delete(key)
```

**Environment Variables Required**:

- `REDISCLOUD_URL`: Redis connection string
- `CLOSE_API_KEY`: Close.com API key for real API tests

**Test Execution Strategy**:

- Unit tests run always (fast, no external dependencies)
- Integration tests run with Redis available
- Real API tests run only when `CLOSE_API_KEY` provided
- Use pytest markers: `@pytest.mark.integration`, `@pytest.mark.real_api`

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
5. ✅ ~~Create failing unit tests for rate limiter core logic~~
6. ✅ ~~Implement rate limiter core logic to make tests pass~~
7. **NEXT**: Create comprehensive integration tests with real Redis and real API testing
8. Continue TDD approach through remaining phases
9. Deploy with feature flag for gradual rollout

## Progress Summary

**Phase 1: FULLY COMPLETED** ✅ (All 51 unit tests passing)

- **Phase 1.1**: Header parsing function implemented and fully tested (13 test cases passing)
- **Phase 1.2**: Endpoint extraction function implemented and fully tested (17 test methods passing)  
- **Phase 1.3**: CloseRateLimiter class implemented and fully tested (21 test cases passing)
- All core components handle edge cases robustly with comprehensive error handling
- Dynamic rate limiter with endpoint-specific functionality working correctly
- Conservative defaults, safety factors, and Redis integration all functional

**Phase 3: COMPLETED** ✅ (All integration tests passing)

- **Redis Integration Tests**: 8 comprehensive tests validating real Redis operations
- **Real API Integration Tests**: 8 tests for actual Close.com API integration
- **Test Infrastructure**: Pytest markers registered, environment variables configured
- **All Tests Passing**: No warnings, comprehensive coverage of integration scenarios

**CURRENT STATUS**: Ready for Phase 2 (close_utils.py integration) or deployment
