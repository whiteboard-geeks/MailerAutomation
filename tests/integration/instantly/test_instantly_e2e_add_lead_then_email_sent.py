"""
End-to-end integration test for the complete Instantly workflow:
1. Add lead to campaign
2. Wait for Instantly to send email
3. Process email sent webhook
"""

import os
import time
import requests
from datetime import datetime
from tests.utils.close_api import CloseAPI


class TestInstantlyE2EAddLeadThenEmailSent:
    def setup_method(self):
        """Setup before each test."""
        self.close_api = CloseAPI()
        self.test_data = {}
        self.base_url = os.environ.get("BASE_URL", "http://localhost:8080")

        # Use the specified campaign name
        self.campaign_name = "Test20250227"

        # Set environment type and current date for unique email
        env_type = os.environ.get("ENV_TYPE", "test")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Create unique email for this test run
        self.test_email = f"lance+{env_type}.e2e{timestamp}@whiteboardgeeks.com"
        self.test_first_name = "Lance"
        self.test_last_name = f"E2ETest{timestamp}"

    def teardown_method(self):
        """Cleanup after each test."""
        # Delete the test lead if it was created
        if self.test_data.get("lead_id"):
            try:
                self.close_api.delete_lead(self.test_data["lead_id"])
                print(f"Cleaned up test lead: {self.test_data['lead_id']}")
            except Exception as e:
                print(
                    f"Warning: Could not clean up lead {self.test_data['lead_id']}: {e}"
                )

    def check_webhook_immediately_available(self, close_task_id, route=None):
        """Check if webhook entry is immediately available (without waiting for completion)."""
        webhook_endpoint = (
            f"{self.base_url}/instantly/webhooks/status?close_task_id={close_task_id}"
        )
        if route:
            webhook_endpoint += f"&route={route}"

        print(f"Checking immediate webhook availability: {webhook_endpoint}")
        try:
            response = requests.get(webhook_endpoint)
            print(f"Immediate check response status: {response.status_code}")
            if response.status_code == 200:
                webhook_data = response.json().get("data", {})
                print(f"Immediate webhook data: {webhook_data}")
                if webhook_data:
                    # Add close_task_id to webhook data if not present
                    if "close_task_id" not in webhook_data:
                        webhook_data["close_task_id"] = close_task_id
                    return webhook_data
            elif response.status_code == 404:
                print(f"404 response content: {response.json()}")
                return None
        except Exception as e:
            print(f"Error querying webhook API immediately: {e}")
            return None

        return None

    def wait_for_webhook_processed(
        self, close_task_id, route=None, wait_for_completion=True, timeout=60
    ):
        """Wait for webhook to be processed by checking the webhook tracker API."""
        webhook_endpoint = (
            f"{self.base_url}/instantly/webhooks/status?close_task_id={close_task_id}"
        )
        if route:
            webhook_endpoint += f"&route={route}"

        print(f"Checking webhook endpoint: {webhook_endpoint}")
        start_time = time.time()
        elapsed_time = 0

        while elapsed_time < timeout:
            try:
                response = requests.get(webhook_endpoint)
                print(f"Response status: {response.status_code}")
                if response.status_code == 200:
                    webhook_data = response.json().get("data", {})
                    print(f"Webhook data: {webhook_data}")
                    if webhook_data:
                        # Add close_task_id to webhook data if not present
                        if "close_task_id" not in webhook_data:
                            webhook_data["close_task_id"] = close_task_id

                        # If we don't need to wait for completion, return immediately
                        if not wait_for_completion:
                            return webhook_data

                        # If we need completion, check if it's processed
                        if webhook_data.get("processed") is True:
                            return webhook_data

                        print(
                            f"Webhook found but not yet processed. Status: {webhook_data.get('status', 'unknown')}"
                        )
                elif response.status_code == 404:
                    print(f"404 response content: {response.json()}")
            except Exception as e:
                print(f"Error querying webhook API: {e}")

            time.sleep(1)  # Check every second
            elapsed_time = time.time() - start_time
            print(f"Elapsed time: {int(elapsed_time)} seconds")

        status_description = "completed" if wait_for_completion else "found"
        raise TimeoutError(
            f"Timed out waiting for webhook to be {status_description} after {int(elapsed_time)} seconds"
        )

    def wait_for_task_completion(self, task_id, timeout_minutes=10):
        """
        Wait for a task to be marked as complete in Close CRM.

        Args:
            task_id (str): The task ID to monitor
            timeout_minutes (int): Maximum time to wait in minutes

        Returns:
            dict: Task data when completed

        Raises:
            TimeoutError: If task not completed within timeout
        """
        print("\n=== WAITING FOR TASK COMPLETION ===")
        print(f"Task ID: {task_id}")
        print(f"Timeout: {timeout_minutes} minutes")

        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        check_interval = 10  # Check every 10 seconds

        while time.time() - start_time < timeout_seconds:
            elapsed_minutes = (time.time() - start_time) / 60
            print(
                f"Checking task completion... ({elapsed_minutes:.1f}/{timeout_minutes} minutes elapsed)"
            )

            try:
                task = self.close_api.get_task(task_id)

                if task.get("is_complete"):
                    print(f"✅ Task {task_id} is now complete!")
                    return task
                else:
                    print(f"Task {task_id} is still incomplete...")

            except Exception as e:
                print(f"Error checking task status: {e}")

            print(f"Next check in {check_interval} sec...")
            time.sleep(check_interval)

        raise TimeoutError(
            f"Task {task_id} was not completed within {timeout_minutes} minutes"
        )

    def wait_for_email_activity(self, lead_id, test_email, timeout_minutes=10):
        """
        Wait for an outgoing email activity to be created in Close CRM.

        Args:
            lead_id (str): The lead ID to check for email activities
            test_email (str): The email address to look for
            timeout_minutes (int): Maximum time to wait in minutes

        Returns:
            dict: Email activity data when found

        Raises:
            TimeoutError: If email activity not found within timeout
        """
        print("\n=== WAITING FOR EMAIL ACTIVITY ===")
        print(f"Lead ID: {lead_id}")
        print(f"Test email: {test_email}")
        print(f"Timeout: {timeout_minutes} minutes")

        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        check_interval = 10  # Check every 10 seconds

        while time.time() - start_time < timeout_seconds:
            elapsed_minutes = (time.time() - start_time) / 60
            print(
                f"Checking for email activity... ({elapsed_minutes:.1f}/{timeout_minutes} minutes elapsed)"
            )

            try:
                email_activities = self.close_api.get_lead_email_activities(lead_id)

                # Look for outgoing email to our test address
                for email in email_activities:
                    if (
                        email.get("direction") == "outgoing"
                        and email.get("status") == "sent"
                        and email.get("to")
                        and test_email in email.get("to", [])
                    ):
                        print(f"✅ Found matching email activity: {email['id']}")
                        return email

                print(
                    f"No matching email activities found yet. Total activities: {len(email_activities)}"
                )

            except Exception as e:
                print(f"Error checking email activities: {e}")

            print(f"Next check in {check_interval} sec...")
            time.sleep(check_interval)

        raise TimeoutError(
            f"Email activity for {test_email} not found within {timeout_minutes} minutes"
        )

    def create_add_lead_payload(self, lead_id, close_task_id):
        """Create the add_lead webhook payload using real lead data."""
        import uuid

        payload = {
            "subscription_id": "whsub_1vT2aEze4uUzQlqLIBExYl",
            "event": {
                "id": f"ev_{uuid.uuid4().hex[:20]}",
                "date_created": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "date_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                "user_id": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                "request_id": f"req_{uuid.uuid4().hex[:20]}",
                "api_key_id": "api_3fw37yHasQmGs00Nnybzq5",
                "oauth_client_id": None,
                "oauth_scope": None,
                "object_type": "task.lead",
                "object_id": close_task_id,
                "lead_id": lead_id,
                "action": "created",
                "changed_fields": [],
                "meta": {"request_path": "/api/v1/task/", "request_method": "POST"},
                "data": {
                    "_type": "lead",
                    "object_type": None,
                    "contact_id": None,
                    "is_complete": False,
                    "assigned_to_name": "Barbara Pigg",
                    "id": close_task_id,
                    "sequence_id": None,
                    "is_new": True,
                    "created_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "deduplication_key": None,
                    "created_by_name": "Barbara Pigg",
                    "date_updated": datetime.now().strftime(
                        "%Y-%m-%dT%H:%M:%S.%f+00:00"
                    ),
                    "is_dateless": False,
                    "sequence_subscription_id": None,
                    "lead_id": lead_id,
                    "object_id": None,
                    "updated_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "due_date": datetime.now().strftime("%Y-%m-%d"),
                    "is_primary_lead_notification": True,
                    "updated_by_name": "Barbara Pigg",
                    "assigned_to": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
                    "text": f"Instantly: {self.campaign_name}",
                    "lead_name": f"{self.test_first_name} {self.test_last_name}",
                    "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
                    "view": None,
                    "date_created": datetime.now().strftime(
                        "%Y-%m-%dT%H:%M:%S.%f+00:00"
                    ),
                },
                "previous_data": {},
            },
        }

        return payload

    def test_instantly_e2e_add_lead_then_email_sent(self):
        """
        End-to-end test: Add lead to Instantly campaign, wait for email to be sent,
        then verify email sent webhook processing.

        This test can take several minutes as it waits for Instantly to actually send the email.
        """
        # Track test start time for duration calculation
        self.test_start_time = time.time()

        print(
            "\n=== STARTING E2E TEST: Add Lead → Wait for Email → Process Webhook ==="
        )
        print(f"Campaign: {self.campaign_name}")
        print(f"Test email: {self.test_email}")
        print("Expected duration: 2-10 minutes")

        # ===== STAGE 1: CREATE LEAD AND ADD TO CAMPAIGN =====
        print("\n--- STAGE 1: CREATE LEAD AND ADD TO CAMPAIGN ---")

        # Create a test lead in Close
        print("Creating test lead in Close...")
        lead_data = self.close_api.create_test_lead(
            email=self.test_email,
            first_name=self.test_first_name,
            last_name=self.test_last_name,
            include_date_location=True,
        )
        self.test_data["lead_id"] = lead_data["id"]
        print(f"Test lead created with ID: {lead_data['id']}")

        # Create a task with the campaign name
        print(f"Creating task for lead with campaign name: {self.campaign_name}...")
        task_data = self.close_api.create_task_for_lead(
            lead_data["id"], self.campaign_name
        )
        self.test_data["close_task_id"] = task_data["id"]
        print(f"Task created with ID: {task_data['id']}")

        # Stage 1 Assertions: Verify lead creation
        assert lead_data is not None, "Lead data should not be None"
        assert "id" in lead_data, "Lead should have an ID"
        assert lead_data["id"].startswith("lead_"), "Lead ID should have correct format"
        print("✅ Stage 1a: Lead creation verified")

        # Create and send the add_lead webhook payload
        close_task_id = task_data["id"]
        add_lead_payload = self.create_add_lead_payload(lead_data["id"], close_task_id)

        print("Sending add_lead webhook to endpoint...")
        response = requests.post(
            f"{self.base_url}/instantly/add_lead",
            json=add_lead_payload,
        )
        print(f"Add lead webhook response status: {response.status_code}")
        print(f"Add lead webhook response: {response.json()}")

        # Stage 1 Assertions: Verify webhook submission
        assert response.status_code in [
            200,
            202,
        ], f"Add lead webhook should return 200 or 202, got {response.status_code}"
        response_data = response.json()
        assert "status" in response_data, "Response should contain status"
        assert response_data["status"] in [
            "success",
            "queued",
        ], "Status should be success or queued"
        print("✅ Stage 1b: Add lead webhook submission verified")

        # Check immediate webhook availability
        print("Checking immediate webhook availability...")
        immediate_webhook_data = self.check_webhook_immediately_available(
            close_task_id, "add_lead"
        )

        # Stage 1 Assertions: Verify webhook is immediately findable
        assert (
            immediate_webhook_data is not None
        ), "Webhook should be immediately findable after submission"
        assert (
            immediate_webhook_data.get("route") == "add_lead"
        ), "Immediate webhook route should be add_lead"
        assert (
            immediate_webhook_data.get("lead_id") == lead_data["id"]
        ), "Immediate webhook lead_id should match"
        assert (
            immediate_webhook_data.get("close_task_id") == close_task_id
        ), "Immediate webhook close_task_id should match"
        assert (
            immediate_webhook_data.get("campaign_name") == self.campaign_name
        ), "Immediate webhook campaign_name should match"
        print("✅ Stage 1c: Immediate webhook availability verified")

        # Wait for add_lead webhook to be processed
        print("Waiting for add_lead webhook to be processed...")
        add_lead_webhook_data = self.wait_for_webhook_processed(
            close_task_id, "add_lead"
        )

        # Stage 1 Assertions: Verify add_lead processing completion
        assert (
            add_lead_webhook_data is not None
        ), "Add lead webhook data should not be None after processing"
        assert (
            add_lead_webhook_data.get("processed") is True
        ), "Add lead webhook wasn't marked as processed"
        assert (
            add_lead_webhook_data.get("campaign_name") == self.campaign_name
        ), "Campaign name doesn't match"

        # Verify Instantly API result
        instantly_result = add_lead_webhook_data.get("instantly_result", {})
        assert instantly_result, "Instantly result should be present"
        assert (
            instantly_result.get("status") == "success"
        ), f"Instantly API call failed: {instantly_result}"
        print("✅ Stage 1d: Add lead processing completed successfully")

        print(
            f"✅ STAGE 1 COMPLETE: Lead {lead_data['id']} added to campaign {self.campaign_name}"
        )

        # ===== STAGE 2: WAIT FOR TASK COMPLETION =====
        print("\n--- STAGE 2: WAIT FOR TASK COMPLETION ---")
        print(
            "Waiting for the task to be marked complete (indicates webhook was processed)..."
        )

        # Wait for the task to be completed (this indicates the webhook was processed)
        try:
            self.wait_for_task_completion(close_task_id, timeout_minutes=10)
            print("✅ Stage 2: Task marked as complete!")

        except TimeoutError as e:
            print(f"❌ Stage 2 TIMEOUT: {e}")
            print("This could mean:")
            print("1. The webhook was not processed by any environment")
            print("2. The campaign is not configured to send emails immediately")
            print("3. The campaign is paused or inactive")
            print("4. There's an issue with the webhook configuration")

            # Don't fail the test immediately - let's check current task status
            print("\nChecking current task status...")
            try:
                current_task = self.close_api.get_task(close_task_id)
                print(f"Current task status: {current_task}")
            except Exception as check_error:
                print(f"Error checking task status: {check_error}")

            # Re-raise the timeout error
            raise e

        # ===== STAGE 3: WAIT FOR EMAIL ACTIVITY =====
        print("\n--- STAGE 3: WAIT FOR EMAIL ACTIVITY ---")
        print(
            "Waiting for email activity to be created in Close (indicates email was sent)..."
        )

        # Wait for email activity to be created
        try:
            matching_email = self.wait_for_email_activity(
                lead_data["id"], self.test_email, timeout_minutes=10
            )
            print("✅ Stage 3: Email activity found!")

        except TimeoutError as e:
            print(f"❌ Stage 3 TIMEOUT: {e}")
            print("This could mean:")
            print("1. Instantly hasn't sent the email yet")
            print("2. The email was sent but activity wasn't created in Close")
            print("3. The campaign is not configured properly")

            # Don't fail the test immediately - let's check what activities exist
            print("\nChecking all email activities for this lead...")
            try:
                all_activities = self.close_api.get_lead_email_activities(
                    lead_data["id"]
                )
                print(f"Found {len(all_activities)} email activities:")
                for activity in all_activities:
                    print(
                        f"  - {activity.get('direction')} {activity.get('status')} to {activity.get('to', [])}"
                    )
            except Exception as check_error:
                print(f"Error checking email activities: {check_error}")

            # Re-raise the timeout error
            raise e

        # ===== FINAL VERIFICATION =====
        print("\n--- FINAL E2E VERIFICATION ---")

        # Verify final task state
        final_task = self.close_api.get_task(close_task_id)
        assert final_task["is_complete"], "Task should be marked as complete"
        print("✅ Final task verification: Task is complete")

        # Verify final email activity
        assert matching_email["direction"] == "outgoing", "Email should be outgoing"
        assert matching_email["status"] == "sent", "Email should be sent"
        assert self.test_email in matching_email.get(
            "to", []
        ), "Email should be sent to test address"
        print("✅ Final email verification: Email activity is correct")

        # Summary
        test_start_time = getattr(self, "test_start_time", time.time())
        total_duration = (time.time() - test_start_time) / 60

        print("\n🎉 E2E TEST COMPLETED SUCCESSFULLY!")
        print(f"📧 Lead: {lead_data['id']} ({self.test_email})")
        print(f"📋 Campaign: {self.campaign_name}")
        print(f"✅ Task: {close_task_id} (Complete)")
        print(f"✅ Email Activity: {matching_email['id']} (Sent)")
        print(f"⏱️  Total test duration: {total_duration:.1f} minutes")

        print("\n=== E2E TEST SUMMARY ===")
        print("1. ✅ Created lead in Close CRM")
        print("2. ✅ Created task to trigger webhook")
        print("3. ✅ Sent add_lead webhook to local endpoint")
        print("4. ✅ Verified webhook was processed (task completed)")
        print("5. ✅ Verified email was sent (email activity created)")
        print("6. ✅ Confirmed end-to-end automation pipeline")

        print("This test confirms the complete email automation pipeline is working!")
        print(
            "Note: This test verifies the end result regardless of which environment processed the webhook."
        )
