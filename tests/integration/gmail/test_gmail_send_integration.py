import os
import pytest
import uuid
from datetime import datetime
from time import sleep

# Import the function from the blueprints
from blueprints.gmail import send_gmail, check_for_emails


class TestGmailSendIntegration:
    def setup_method(self):
        """Setup before each test."""
        # Check if Gmail credentials are available
        self.gmail_service_account_info = os.environ.get("GMAIL_SERVICE_ACCOUNT_INFO")
        if not self.gmail_service_account_info:
            pytest.skip("GMAIL_SERVICE_ACCOUNT_INFO environment variable not set")

        # Use default sender from the Gmail blueprint
        self.sender = os.environ.get("TEST_EMAIL_SENDER", "lance@whiteboardgeeks.com")

        # Set recipient email (use the same sender for simplicity)
        self.recipient = os.environ.get("TEST_EMAIL_RECIPIENT", self.sender)

        # Generate a unique subject for this test run to be able to find it later
        self.test_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.email_subject = f"Test Email {self.test_id} - {timestamp}"

        # Email content
        self.html_content = f"""
        <html>
            <body>
                <h1>Test Email</h1>
                <p>This is a test email sent from the TestGmailSendIntegration test.</p>
                <p>Test ID: {self.test_id}</p>
                <p>Timestamp: {timestamp}</p>
            </body>
        </html>
        """

        self.text_content = f"""
        Test Email
        
        This is a test email sent from the TestGmailSendIntegration test.
        Test ID: {self.test_id}
        Timestamp: {timestamp}
        """

    def test_send_and_verify_email(self):
        """
        Test sending a live email via Gmail API and verify it was received.

        This test:
        1. Sends an actual email through Gmail
        2. Waits for the email to be delivered
        3. Queries Gmail to verify the email was received
        """
        # 1. Send the email
        result = send_gmail(
            sender=self.sender,
            to=self.recipient,
            subject=self.email_subject,
            html_content=self.html_content,
            text_content=self.text_content,
        )

        # Verify the send was successful
        assert (
            result["status"] == "success"
        ), f"Failed to send email: {result.get('message')}"
        assert "message_id" in result, "No message ID returned"

        # Store the message ID for later verification
        message_id = result["message_id"]
        print(f"Email sent successfully with message ID: {message_id}")

        # 2. Wait for the email to be delivered (Gmail can take a moment)
        print("Waiting for email delivery...")
        sleep(10)  # Wait 10 seconds for delivery

        # 3. Query Gmail to verify the email was received
        # Create a query that will find our specific email
        query = f"subject:{self.email_subject}"

        # Check for the email
        check_result = check_for_emails(
            user_email=self.recipient, query=query, max_results=5, include_content=True
        )

        # Verify the email was found
        assert (
            check_result["status"] == "success"
        ), f"Failed to check emails: {check_result.get('message')}"

        messages = check_result.get("messages", [])
        assert len(messages) > 0, f"Email with subject '{self.email_subject}' not found"

        # Verify the content of the email
        found_message = None
        for message in messages:
            if message.get("subject") == self.email_subject:
                found_message = message
                break

        assert (
            found_message is not None
        ), f"Could not find exact email with subject '{self.email_subject}'"
        assert self.test_id in found_message.get(
            "snippet", ""
        ), "Test ID not found in email snippet"

        print(f"Email successfully verified in recipient's inbox")
