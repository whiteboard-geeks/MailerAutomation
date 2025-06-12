from blueprints.instantly import get_instantly_campaigns


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
