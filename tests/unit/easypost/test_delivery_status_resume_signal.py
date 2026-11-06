"""Parked workflows resume on the data_issue_fixed signal; without a handler they never can.

WebhookDeliveryStatusWorkflow parked on a Close 500 on 2026-10-28 and ignored the signal.
"""

import pytest
from temporalio import workflow

from temporal.workflows.easypost.webhook_create_tracker_workflow import WebhookCreateTrackerWorkflow
from temporal.workflows.easypost.webhook_delivery_status_workflow import WebhookDeliveryStatusWorkflow
from temporal.workflows.instantly.webhook_add_lead_workflow import WebhookAddLeadWorkflow
from temporal.workflows.instantly.webhook_email_sent_workflow import WebhookEmailSentWorkflow
from temporal.workflows.instantly.webhook_reply_received_workflow import WebhookReplyReceivedWorkflow


@pytest.mark.parametrize("cls", [
    WebhookDeliveryStatusWorkflow, WebhookCreateTrackerWorkflow, WebhookAddLeadWorkflow,
    WebhookEmailSentWorkflow, WebhookReplyReceivedWorkflow,
])
def test_parking_workflow_handles_data_issue_fixed(cls):
    assert "data_issue_fixed" in workflow._Definition.must_from_class(cls).signals
