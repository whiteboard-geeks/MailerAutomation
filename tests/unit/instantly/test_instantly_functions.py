"""
Unit tests for the Instantly blueprint helper functions.
"""

from blueprints.instantly import get_instantly_campaign_name


def test_get_instantly_campaign_name():
    """Test that campaign name extraction works correctly with various inputs."""

    test_cases = [
        # Basic cases
        ("Instantly: Test Campaign", "Test Campaign"),
        ("Instantly:No Space", "No Space"),
        ("Instantly! With Exclamation", "With Exclamation"),
        ("Instantly-- With Dashes", "With Dashes"),
        ("Instantly Test", "Test"),
        # Edge cases
        ("InstantlyTest", ""),  # No separator, nothing extracted
        ("Instantly", ""),  # Just the keyword, nothing extracted
        (
            "Not an Instantly task",
            "Not an Instantly task",
        ),  # Doesn't start with "Instantly"
        ("", ""),  # Empty string
        # Bracket removal cases
        ("Instantly: Campaign Name [Note]", "Campaign Name"),
        ("Instantly: BP_BC_BlindInviteEmail1 [Noura Test]", "BP_BC_BlindInviteEmail1"),
        ("Instantly: Campaign [Note1] [Note2]", "Campaign"),
        ("Instantly: Campaign[No Space]", "Campaign"),
        ("Instantly Campaign [Note]", "Campaign"),
    ]

    for input_text, expected in test_cases:
        result = get_instantly_campaign_name(input_text)
        assert (
            result == expected
        ), f"Failed on input '{input_text}': expected '{expected}', got '{result}'"
