import os
from redis import Redis
from utils.instantly import get_instantly_campaigns


def test_get_campaigns_single_page():
    """Test fetching a single page of campaigns"""
    print("test_get_campaigns_single_page")
    result = get_instantly_campaigns(limit=2)

    assert result["status"] == "success"
    assert "campaigns" in result
    assert "count" in result
    assert "pagination" in result
    assert isinstance(result["campaigns"], list)
    assert isinstance(result["count"], int)
    assert result["count"] == len(result["campaigns"])
    assert result["pagination"]["limit"] == 2

    # Verify campaign structure if any campaigns exist
    if result["campaigns"]:
        campaign = result["campaigns"][0]
        assert "id" in campaign
        assert "name" in campaign
        assert "status" in campaign


def test_get_campaigns_pagination():
    """Test fetching campaigns with pagination"""
    # Get first page
    first_page = get_instantly_campaigns(limit=1)
    assert first_page["status"] == "success"

    # If there are more pages
    if first_page["pagination"]["has_more"]:
        next_cursor = first_page["pagination"]["next_starting_after"]

        # Get second page
        second_page = get_instantly_campaigns(limit=1, starting_after=next_cursor)
        assert second_page["status"] == "success"

        # Verify we got different campaigns
        if first_page["campaigns"] and second_page["campaigns"]:
            assert first_page["campaigns"][0]["id"] != second_page["campaigns"][0]["id"]


def test_get_all_campaigns():
    """Test fetching all campaigns using fetch_all=True"""
    result = get_instantly_campaigns(fetch_all=True)

    assert result["status"] == "success"
    assert "campaigns" in result
    assert "count" in result
    assert isinstance(result["campaigns"], list)
    assert isinstance(result["count"], int)
    assert result["count"] == len(result["campaigns"])

    # Verify we got all campaigns by comparing with paginated results
    paginated_campaigns = []
    next_cursor = None

    while True:
        page = get_instantly_campaigns(limit=100, starting_after=next_cursor)
        assert page["status"] == "success"
        paginated_campaigns.extend(page["campaigns"])

        if not page["pagination"]["has_more"]:
            break

        next_cursor = page["pagination"]["next_starting_after"]

    assert len(result["campaigns"]) == len(paginated_campaigns)


def test_search_campaigns_by_name():
    """Test searching campaigns by name"""
    search_term = "Test20250227"
    result = get_instantly_campaigns(search=search_term)

    assert result["status"] == "success"
    assert "campaigns" in result
    assert "count" in result
    assert isinstance(result["campaigns"], list)

    # Verify that all returned campaigns contain the search term in their name
    for campaign in result["campaigns"]:
        assert search_term.lower() in campaign["name"].lower()


def test_campaign_search_caching():
    """Test that campaign search results are cached in Redis and cache is cleared after test."""
    search_term = "Test20250227"
    redis_url = os.environ.get("REDISCLOUD_URL")
    redis_client = Redis.from_url(redis_url) if redis_url else None
    cache_key = f"instantly:campaign_search:{search_term.lower().strip()}"

    # Ensure cache is clear before test
    if redis_client:
        redis_client.delete(cache_key)

    # First call (should not be cached)
    result1 = get_instantly_campaigns(search=search_term)
    assert result1["status"] == "success"
    assert "campaigns" in result1

    # Second call (should be cached, but will not be until implemented)
    result2 = get_instantly_campaigns(search=search_term)
    assert result2["status"] == "success"
    assert "campaigns" in result2

    # (Failing assertion: check that the cache is used, which will fail until implemented)
    assert (
        redis_client and redis_client.get(cache_key) is not None
    ), "Expected cache to be set after search, but it was not."

    # Clean up: clear the cache key
    if redis_client:
        redis_client.delete(cache_key)
