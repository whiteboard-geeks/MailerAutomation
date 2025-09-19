"""
Integration tests for the Instantly add_lead webhook handler.
"""

import copy
import os
import uuid
import pytest
import requests
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime

from tenacity import Retrying, stop_after_delay, wait_fixed, retry_if_result, RetryError

from tests.utils.close_api import CloseAPI, Lead
from utils.instantly import search_campaigns_by_lead_email


class TestInstantlyAddLeadIntegration:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Load the mock webhook payload
        self.mock_payload = {
            "subscription_id": "whsub_1vT2aEze4uUzQlqLIBExYl",
            "event": {
                "id": "ev_34bKnJcMX9UnRJmuGH5Jtr",
                "date_created": "2025-02-28T19:20:45.507000",
                "date_updated": "2025-02-28T19:20:45.507000",
                "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                "user_id": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                "request_id": "req_5SPmoSjkZBMkMkOAaxz7o7",
                "api_key_id": "api_3fw37yHasQmGs00Nnybzq5",
                "oauth_client_id": None,
                "oauth_scope": None,
                "object_type": "task.lead",
                "object_id": "task_CIRBr39mOsTfWAc3ErihkSt4cX0PlVBpTovHGNj939w",
                "lead_id": "lead_mtonPqjLkC0X93AW6evKVa1Sbpq7l8opyuaV5olT2Cf",
                "action": "created",
                "changed_fields": [],
                "meta": {"request_path": "/api/v1/task/", "request_method": "POST"},
                "data": {
                    "_type": "lead",
                    "object_type": None,
                    "contact_id": None,
                    "is_complete": False,
                    "assigned_to_name": "Barbara Pigg",
                    "id": "task_CIRBr39mOsTfWAc3ErihkSt4cX0PlVBpTovHGNj939w",
                    "sequence_id": None,
                    "is_new": True,
                    "created_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "date": "2025-03-01",
                    "deduplication_key": None,
                    "created_by_name": "Barbara Pigg",
                    "date_updated": "2025-02-28T19:20:45.505000+00:00",
                    "is_dateless": False,
                    "sequence_subscription_id": None,
                    "lead_id": "lead_mtonPqjLkC0X93AW6evKVa1Sbpq7l8opyuaV5olT2Cf",
                    "object_id": None,
                    "updated_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "due_date": "2025-03-01",
                    "is_primary_lead_notification": True,
                    "updated_by_name": "Barbara Pigg",
                    "assigned_to": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "text": "Instantly: Test20250227",
                    "lead_name": "Test Instantly20250228132044",
                    "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                    "view": None,
                    "date_created": "2025-02-28T19:20:45.505000+00:00",
                },
                "previous_data": {},
            },
        }

        # Set environment type and current date
        os.environ.get("ENV_TYPE", "test")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Generate unique task ID for this test run
        import uuid

        unique_task_id = f"task_test_{uuid.uuid4().hex[:20]}"

        # Update the mock payload with unique identifiers
        self.mock_payload["event"]["object_id"] = unique_task_id
        self.mock_payload["event"]["data"]["id"] = unique_task_id
        self.mock_payload["event"]["data"]["lead_name"] = f"Test Instantly{timestamp}"

        self.lead_ids : list[str] = []

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        for lead_id in self.lead_ids:
            self.close_api.delete_lead(lead_id)

    @pytest.mark.parametrize("num_workers,num_leads", [
        (1, 1),
        (2, 10),
    ])
    def test_instantly_add_lead_success(self, num_workers, num_leads):
        """Test successful flow of adding lead(s) to an Instantly campaign."""
        instantly_campaign_name = "Test20250227"
        
        print(f"\n=== INTEGRATION TEST: {num_leads} leads, {num_workers} workers ===")
        
        # Stage 1: Create test leads in Close
        leads : list[Lead] = []
        for i in range(num_leads):
            lead_data = self.close_api.create_test_lead(
                include_date_location=True,
                email_suffix=f"{datetime.now().strftime('%Y%m%d%H%M%S')}+{i}")
            lead = Lead(**lead_data)
            leads.append(lead)
            self.lead_ids.append(lead.id)
        
        # Stage 2: Verify leads NOT in campaigns initially
        for lead in leads:
            campaigns_before = search_campaigns_by_lead_email(lead.contacts[0].emails[0].email)
            assert len(campaigns_before) == 0
        
        # Stage 3: Send webhooks with configurable concurrency
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures : list[Future] = []
            for i, lead in enumerate(leads):
                # Prepare payload for each lead - use deep copy to avoid shared nested objects
                payload = copy.deepcopy(self.mock_payload)
                payload["event"]["data"]["lead_id"] = lead.id
                payload["event"]["data"]["id"] = f"task_test_{uuid.uuid4().hex[:20]}_{i}"
                
                # Submit webhook request
                future = executor.submit(
                    requests.post,
                    f"{self.base_url}/instantly/add_lead",
                    json=payload
                )
                futures.append(future)
            
            # Collect all responses
            responses = [future.result() for future in futures]
        
        # Stage 4: Assert all HTTP responses successful
        for response in responses:
            assert response.status_code in [200, 202]
            response_data = response.json()
            assert response_data["status"] in ["success", "queued"]

        for lead in leads:
            print(f"lead {lead}")
        
        # Stage 5: Verify all leads ARE in campaigns
        emails = [lead.contacts[0].emails[0].email for lead in leads]
        wait_until_instantly_synced(emails, instantly_campaign_name, timeout=10, poll=2)


def wait_until_instantly_synced(
    emails: list[str],
    campaign_name: str,
    timeout: float = 60.0,
    poll: float = 0.5,
):
    """
    Poll Instantly until *all* emails are found in `campaign_name`, or timeout.

    - Sequential checks
    - Emails already found are not queried again
    - Transient errors are treated as "not yet" and retried
    """
    remaining = set(emails)

    def _tick():
        to_remove = []
        for email in list(remaining):
            try:
                campaigns = search_campaigns_by_lead_email(email)
            except Exception as e:
                # print short error that helps with debugging. Also prints the exception details.
                print(f"Error searching campaigns for {email}: {repr(e)}")
                continue

            print(f"Found {len(campaigns)} campaigns for {email}: {[c.name for c in campaigns]}")
            if any(c.name == campaign_name for c in campaigns):
                to_remove.append(email)

        for e in to_remove:
            remaining.discard(e)

        return remaining  # retry while non-empty

    try:
        Retrying(
            stop=stop_after_delay(timeout),
            wait=wait_fixed(poll),
            retry=retry_if_result(lambda rem: bool(rem)),
            reraise=True,
        )(_tick)  # <-- call the Retrying instance
    except RetryError:
        raise AssertionError(
            f"Timed out waiting for emails to appear in '{campaign_name}': {sorted(remaining)}"
        )
