"""Integration-test campaign must fail fast instead of parking forever.

Integration tests run against production using TEST_CAMPAIGN_NAME, which deliberately
does not exist in Instantly. Before this guard the workflow caught the resulting error
and parked on the data_issue_fixed signal, leaving a Running workflow forever -- about
four per CI run. Twenty-four had accumulated by 2026-08-12, burying real stuck
workflows in the WaitingForResume queue.
"""

import pytest
from temporalio.exceptions import ApplicationError

from config import TEST_CAMPAIGN_NAME
from temporal.shared import raise_if_test_campaign


def test_test_campaign_raises():
    original = ValueError(f"Campaign '{TEST_CAMPAIGN_NAME}' does not exist in Instantly")
    with pytest.raises(ApplicationError) as excinfo:
        raise_if_test_campaign(original, TEST_CAMPAIGN_NAME)
    assert TEST_CAMPAIGN_NAME in str(excinfo.value)
    assert excinfo.value.__cause__ is original


@pytest.mark.parametrize(
    "campaign_name",
    ["BP_BC_BlindInviteEmail1", "APRIL_BC_Decline No Reason", "", None],
)
def test_real_campaigns_do_not_raise(campaign_name):
    """Real campaigns must still park so a human can repair and resume them."""
    assert raise_if_test_campaign(ValueError("boom"), campaign_name) is None


def test_guard_is_case_sensitive_and_exact():
    """Only the exact test campaign short-circuits; near-misses are real campaigns."""
    for near_miss in [
        TEST_CAMPAIGN_NAME.lower(),
        TEST_CAMPAIGN_NAME + "x",
        "x" + TEST_CAMPAIGN_NAME,
    ]:
        assert raise_if_test_campaign(ValueError("boom"), near_miss) is None


@pytest.mark.parametrize(
    "module_path",
    [
        "temporal/workflows/instantly/webhook_add_lead_workflow.py",
        "temporal/workflows/instantly/webhook_email_sent_workflow.py",
        "temporal/workflows/instantly/webhook_reply_received_workflow.py",
    ],
)
def test_every_parking_site_is_guarded(module_path):
    """Each _wait_for_signal_data_issue_fixed call must be preceded by the guard.

    A new unguarded parking site would silently reintroduce the CI-artifact leak.
    """
    source = open(module_path).read()
    # The definition itself contains the call; every *other* occurrence is a park site.
    park_calls = source.count("await self._wait_for_signal_data_issue_fixed()")
    guard_calls = source.count("raise_if_test_campaign(")
    assert guard_calls >= park_calls, (
        f"{module_path}: {park_calls} parking site(s) but only {guard_calls} guard(s)"
    )
