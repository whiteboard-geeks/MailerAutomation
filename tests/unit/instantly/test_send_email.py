"""
Demonstration test for send_email functionality.

This shows how to test the send_email function in different contexts:
1. In regular Flask routes (using current_app)
2. In Celery tasks (using direct import)
3. Mock vs real email testing
"""

import pytest
from unittest.mock import patch


def test_send_email_in_celery_task():
    """
    Test that send_email works correctly in Celery task context.
    This tests the fix we just implemented.
    """
    from blueprints.instantly import process_lead_batch_task

    # Sample payload that would trigger email sending (campaign not found)
    payload_with_invalid_campaign = {
        "event": {
            "data": {
                "id": "task_test123",
                "text": "Instantly: NonExistentCampaign",
                "lead_id": "lead_test456",
            }
        }
    }

    # Mock the campaign_exists to return False (campaign not found)
    with patch("blueprints.instantly.campaign_exists") as mock_campaign_exists:
        # Mock the send_email function to verify it gets called
        with patch("blueprints.instantly.send_email") as mock_send_email:
            mock_campaign_exists.return_value = {"exists": False}
            mock_send_email.return_value = {
                "status": "success",
                "message_id": "test123",
            }

            # Execute the task
            result = process_lead_batch_task(payload_with_invalid_campaign)

            # Verify the task handled the error correctly
            assert result["status"] == "error"
            assert "does not exist in Instantly" in result["message"]

            # Verify send_email was called (this proves the context issue is fixed)
            mock_send_email.assert_called_once()
            call_args = mock_send_email.call_args
            assert "Campaign Not Found" in call_args[1]["subject"]

            print("✅ send_email works correctly in Celery task context!")


def test_send_email_with_real_gmail_api():
    """
    Test that demonstrates how to test with real Gmail API.
    This test is skipped by default but shows the pattern.
    """
    import os

    # Skip if no Gmail credentials (to avoid failing in CI/CD)
    if not os.environ.get("GMAIL_SERVICE_ACCOUNT_INFO"):
        pytest.skip("Gmail credentials not available for integration test")

    from utils.email import send_email

    # Send a real test email
    result = send_email(
        subject="Test Email from Pytest",
        body="<h1>This is a test email</h1><p>Sent from the test suite to verify send_email works.</p>",
        text_content="This is a test email. Sent from the test suite to verify send_email works.",
        recipients="lance@whiteboardgeeks.com",  # Override default recipients
    )

    # Verify the email was sent successfully
    assert result["status"] == "success"
    assert "message_id" in result

    print(f"✅ Real email sent successfully! Message ID: {result['message_id']}")


def test_send_email_mocked():
    """
    Test send_email functionality with mocked Gmail API.
    This is the preferred approach for unit tests.
    """
    # Mock the Gmail API function and set env_type to production
    with patch("blueprints.gmail.send_gmail") as mock_send_gmail, \
         patch("app.env_type", "production"):
        
        mock_send_gmail.return_value = {
            "status": "success",
            "message_id": "mock_message_123",
            "thread_id": "mock_thread_456",
        }

        from utils.email import send_email

        # Test the send_email function
        result = send_email(
            subject="Test Subject",
            body="<h1>Test HTML</h1>",
            text_content="Test text content",
        )

        # Verify the result
        assert result["status"] == "success"
        assert result["message_id"] == "mock_message_123"

        # Verify the Gmail API was called with correct parameters
        mock_send_gmail.assert_called_once()
        call_args = mock_send_gmail.call_args[1]

        # Check that environment info was added to the email
        assert "Environment:" in call_args["html_content"]
        assert "[MailerAutomation]" in call_args["subject"]

        print("✅ send_email works correctly with mocked Gmail API!")


def test_error_handling_in_celery_task():
    """
    Test that exceptions in Celery tasks are handled properly and emails are sent.
    """
    from blueprints.instantly import process_lead_batch_task

    # Malformed payload that should trigger general exception handling
    malformed_payload = {"invalid": "data"}

    # Mock send_email to verify it gets called for errors
    with patch("blueprints.instantly.send_email") as mock_send_email:
        mock_send_email.return_value = {"status": "success"}

        # Execute the task with bad data
        result = process_lead_batch_task(malformed_payload)

        # Verify error was handled
        assert result["status"] == "error"
        assert "celery_task_id" in result

        # Verify error email was sent
        mock_send_email.assert_called_once()
        call_args = mock_send_email.call_args
        assert "Campaign Name Extraction Error" in call_args[1]["subject"]

        print("✅ Error handling in Celery task works correctly!")


if __name__ == "__main__":
    """Run the tests directly."""
    print("Running send_email demonstration tests...\n")

    try:
        test_send_email_in_celery_task()
        test_send_email_mocked()
        test_error_handling_in_celery_task()

        # Only run real Gmail test if explicitly requested
        import sys

        if "--real-email" in sys.argv:
            test_send_email_with_real_gmail_api()
        else:
            print("⏭️  Skipping real Gmail test (use --real-email to enable)")

        print(
            "\n🎉 All tests passed! The send_email function works correctly in Celery tasks."
        )

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
