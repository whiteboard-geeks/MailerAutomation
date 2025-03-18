"""
Integration test for the Instantly webhook failure modes.
This test sends actual emails so you can verify the failure notifications.

To run just this test:
pytest tests/integration/instantly/test_webhook_failure_integration.py -v

Note: This requires a working email configuration in your environment.
"""

import json
import pytest
import time
from unittest.mock import patch
from app import flask_app

# Sample Close task created webhook payload
SAMPLE_PAYLOAD = {
    "subscription_id": "whsub_7Yqhrb6zEZo1waN6medQzn",
    "event": {
        "id": "ev_4mp5KdF52CVItarzu1kkCi",
        "date_created": "2025-03-18T10:54:52.098000",
        "date_updated": "2025-03-18T10:54:52.098000",
        "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
        "user_id": None,
        "request_id": None,
        "api_key_id": None,
        "oauth_client_id": None,
        "oauth_scope": None,
        "object_type": "task.lead",
        "object_id": "task_07y7VvRV9HXrxDsDCMpZUOdkgKRsCRpmV7fVnSrAhaM",
        "lead_id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
        "action": "created",
        "changed_fields": [],
        "meta": {},
        "data": {
            "date": "2025-03-18",
            "id": "task_07y7VvRV9HXrxDsDCMpZUOdkgKRsCRpmV7fVnSrAhaM",
            "date_created": "2025-03-18T10:54:52.096000+00:00",
            "updated_by_name": None,
            "object_type": None,
            "is_primary_lead_notification": True,
            "organization_id": "orga_0Vf4MtLblgQtq68DQaNmLsVkdaXRpilGNkXNSOOc7zw",
            "sequence_id": "seq_543h6u7YOdAZPJ74I49a0y",
            "lead_name": "Test - Noura M",
            "date_updated": "2025-03-18T10:54:52.096000+00:00",
            "view": None,
            "due_date": "2025-03-18",
            "is_dateless": False,
            "contact_id": None,
            "object_id": None,
            "is_complete": False,
            "created_by_name": "Barbara Pigg",
            "created_by": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
            "assigned_to": "user_8HHUh3SH67YzD8IMakjKoJ9SWputzlUdaihCG95g7as",
            "lead_id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
            "is_new": True,
            "sequence_subscription_id": "sub_38Qv1oCai2YqDuBYc5vpq4",
            "text": "Instantly: BP_BC_BlindInviteEmail1 [Noura Test]",
            "assigned_to_name": "Barbara Pigg",
            "updated_by": None,
            "_type": "lead",
            "deduplication_key": None,
        },
        "previous_data": {},
    },
}


@pytest.fixture
def client():
    """Create a test client with the actual Flask app."""
    # Ensure testing mode
    flask_app.config["TESTING"] = True
    # We want to use the actual email functionality
    flask_app.config["MAIL_SUPPRESS_SEND"] = False
    return flask_app.test_client()


def test_campaign_not_found_sends_real_email(client):
    """
    Integration test for campaign not found error.
    This will actually send a real email notification.
    """
    print("\n--- Testing campaign not found (real email will be sent) ---")

    # Test info
    campaign_name = (
        SAMPLE_PAYLOAD["event"]["data"]["text"].split("Instantly:")[1].strip()
    )
    print(f"Campaign name to test: {campaign_name}")
    print("Email will be sent to the hardcoded recipient in the send_email function")

    # Patch the campaign_exists function to simulate campaign not found
    with patch("blueprints.instantly.campaign_exists") as mock_campaign_exists:
        mock_campaign_exists.return_value = {
            "exists": False,
            "error": "Campaign not found in Instantly API",
        }

        # Send the webhook payload
        start_time = time.time()
        response = client.post(
            "/instantly/add_lead", json=SAMPLE_PAYLOAD, content_type="application/json"
        )
        elapsed = time.time() - start_time

        # Print response details
        print(f"\nResponse received in {elapsed:.2f} seconds")
        print(f"Status code: {response.status_code}")
        response_data = response.json
        print(f"Response body: {json.dumps(response_data, indent=2)}")

        # Assert the response has the expected format (200 status code with success status)
        assert (
            response.status_code == 200
        ), f"Expected status code 200, got {response.status_code}"
        assert (
            response_data.get("status") == "success"
        ), "Response status should be 'success'"

        print("\n✅ Test passed - Webhook returned 200 with 'success' status")
        print("Check your email for the notification about campaign not found.")

        # Print verification prompt
        print("\nVerify that:")
        print(
            "1. You received an email with subject 'Instantly Campaign Not Found: BP_BC_BlindInviteEmail1 [Noura Test]'"
        )
        print("2. The email contains error details")
        print("3. The JSON response has status 'success' despite the error")


def test_lead_not_found_sends_real_email(client):
    """
    Integration test for lead not found error.
    This will actually send a real email notification.
    """
    print("\n--- Testing lead not found (real email will be sent) ---")

    print("Email will be sent to the hardcoded recipient in the send_email function")

    # Patch functions to simulate campaign exists but lead not found
    with patch("blueprints.instantly.campaign_exists") as mock_campaign_exists:
        with patch("blueprints.instantly.get_lead_by_id") as mock_get_lead:
            mock_campaign_exists.return_value = {
                "exists": True,
                "campaign_id": "camp_123456",
            }
            mock_get_lead.return_value = None

            # Send the webhook payload
            start_time = time.time()
            response = client.post(
                "/instantly/add_lead",
                json=SAMPLE_PAYLOAD,
                content_type="application/json",
            )
            elapsed = time.time() - start_time

            # Print response details
            print(f"\nResponse received in {elapsed:.2f} seconds")
            print(f"Status code: {response.status_code}")
            response_data = response.json
            print(f"Response body: {json.dumps(response_data, indent=2)}")

            # Assert the response has the expected format
            assert (
                response.status_code == 200
            ), f"Expected status code 200, got {response.status_code}"
            assert (
                response_data.get("status") == "success"
            ), "Response status should be 'success'"

            print("\n✅ Test passed - Webhook returned 200 with 'success' status")
            print("Check your email for the notification about lead not found.")

            # Print verification prompt
            print("\nVerify that:")
            print("1. You received an email with subject 'Close Lead Details Error'")
            print("2. The email contains error details")
            print("3. The JSON response has status 'success' despite the error")


def test_api_error_sends_real_email(client):
    """
    Integration test for Instantly API error.
    This will actually send a real email notification.
    """
    print("\n--- Testing Instantly API error (real email will be sent) ---")

    print("Email will be sent to the hardcoded recipient in the send_email function")

    # Patch functions to simulate API error
    with patch("blueprints.instantly.campaign_exists") as mock_campaign_exists:
        with patch("blueprints.instantly.get_lead_by_id") as mock_get_lead:
            with patch(
                "blueprints.instantly.add_to_instantly_campaign"
            ) as mock_add_to_campaign:
                mock_campaign_exists.return_value = {
                    "exists": True,
                    "campaign_id": "camp_123456",
                }
                mock_get_lead.return_value = {
                    "id": "lead_OPosV1quUroYLWEZl11wZ0ZUlF6xQMuaER3mwuAC4Vc",
                    "name": "Test Lead",
                    "contacts": [
                        {"id": "cont_123", "emails": [{"email": "test@example.com"}]}
                    ],
                }
                mock_add_to_campaign.return_value = {
                    "status": "error",
                    "message": "Instantly API rate limit exceeded",
                }

                # Send the webhook payload
                start_time = time.time()
                response = client.post(
                    "/instantly/add_lead",
                    json=SAMPLE_PAYLOAD,
                    content_type="application/json",
                )
                elapsed = time.time() - start_time

                # Print response details
                print(f"\nResponse received in {elapsed:.2f} seconds")
                print(f"Status code: {response.status_code}")
                response_data = response.json
                print(f"Response body: {json.dumps(response_data, indent=2)}")

                # Assert the response has the expected format
                assert (
                    response.status_code == 200
                ), f"Expected status code 200, got {response.status_code}"
                assert (
                    response_data.get("status") == "success"
                ), "Response status should be 'success'"

                print("\n✅ Test passed - Webhook returned 200 with 'success' status")
                print("Check your email for the notification about API error.")

                # Print verification prompt
                print("\nVerify that:")
                print("1. You received an email with subject 'Instantly API Error'")
                print("2. The email contains error details")
                print("3. The JSON response has status 'success' despite the error")


if __name__ == "__main__":
    # This allows running the test directly if needed
    pytest.main(["-xvs", __file__])
