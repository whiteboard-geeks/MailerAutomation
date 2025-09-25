"""
Unit tests for consultant notification logic in Instantly reply received webhook.

These tests follow the Test-Driven Development (TDD) approach - they will initially fail
until we implement the determine_notification_recipients() function and modify the
handle_instantly_reply_received() function.
"""

import os
import sys

# Add the project root to the path so we can import blueprints
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from utils.instantly_reply_received import determine_notification_recipients


class TestConsultantNotification:
    """Test consultant-based notification routing logic."""

    def setup_method(self):
        """Setup test data for each test."""
        # Mock lead details with Barbara as consultant
        self.barbara_lead_details = {
            "id": "lead_barbara_123",
            "name": "Test Lead Barbara",
            "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi": "Barbara Pigg",
            "contacts": [
                {
                    "id": "contact_123",
                    "name": "Test Contact",
                    "emails": [{"email": "test@example.com"}],
                }
            ],
        }

        # Mock lead details with April as consultant
        self.april_lead_details = {
            "id": "lead_april_456",
            "name": "Test Lead April",
            "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi": "April Lowrie",
            "contacts": [
                {
                    "id": "contact_456",
                    "name": "Test Contact",
                    "emails": [{"email": "test@example.com"}],
                }
            ],
        }

        # Mock lead details with unknown consultant
        self.unknown_consultant_lead_details = {
            "id": "lead_unknown_789",
            "name": "Test Lead Unknown",
            "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi": "John Doe",
            "contacts": [
                {
                    "id": "contact_789",
                    "name": "Test Contact",
                    "emails": [{"email": "test@example.com"}],
                }
            ],
        }

        # Mock lead details with empty consultant field
        self.empty_consultant_lead_details = {
            "id": "lead_empty_101",
            "name": "Test Lead Empty",
            "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi": "",
            "contacts": [
                {
                    "id": "contact_101",
                    "name": "Test Contact",
                    "emails": [{"email": "test@example.com"}],
                }
            ],
        }

        # Mock lead details with missing consultant field
        self.missing_consultant_lead_details = {
            "id": "lead_missing_102",
            "name": "Test Lead Missing",
            "contacts": [
                {
                    "id": "contact_102",
                    "name": "Test Contact",
                    "emails": [{"email": "test@example.com"}],
                }
            ],
            # Note: consultant field is completely missing
        }

        # Mock lead details with null consultant field
        self.null_consultant_lead_details = {
            "id": "lead_null_103",
            "name": "Test Lead Null",
            "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi": None,
            "contacts": [
                {
                    "id": "contact_103",
                    "name": "Test Contact",
                    "emails": [{"email": "test@example.com"}],
                }
            ],
        }

        # Mock lead details with case-sensitive consultant test
        self.lowercase_consultant_lead_details = {
            "id": "lead_lowercase_104",
            "name": "Test Lead Lowercase",
            "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi": "april lowrie",  # lowercase
            "contacts": [
                {
                    "id": "contact_104",
                    "name": "Test Contact",
                    "emails": [{"email": "test@example.com"}],
                }
            ],
        }

    def test_barbara_pigg_lead_uses_default_recipients(self):
        """Test that Barbara's leads use default notification recipients."""
        # Test in production environment
        recipients, error = determine_notification_recipients(
            self.barbara_lead_details, "production"
        )

        # Barbara should use default recipients (None means use default)
        assert recipients is None, "Barbara's leads should use default recipients"
        assert error is None, "No error should occur for Barbara's leads"

        # Test in development environment
        recipients, error = determine_notification_recipients(
            self.barbara_lead_details, "development"
        )

        # Barbara should use default recipients in development too
        assert (
            recipients is None
        ), "Barbara's leads should use default recipients in development"
        assert error is None, "No error should occur for Barbara's leads in development"

    def test_april_lowrie_lead_uses_custom_recipients_production(self):
        """Test that April's leads use custom recipients in production."""
        recipients, error = determine_notification_recipients(
            self.april_lead_details, "production"
        )

        # April should get custom recipients in production
        expected_recipients = "april.lowrie@whiteboardgeeks.com,lauren.poche@whiteboardgeeks.com"
        assert (
            recipients == expected_recipients
        ), f"Expected April's team recipients, got {recipients}"
        assert error is None, "No error should occur for April's leads"

    def test_april_lowrie_lead_uses_lance_in_development(self):
        """Test that April's leads use Lance only in development."""
        recipients, error = determine_notification_recipients(
            self.april_lead_details, "development"
        )

        # April should get Lance only in development
        expected_recipients = "lance@whiteboardgeeks.com"
        assert (
            recipients == expected_recipients
        ), f"Expected Lance only in development, got {recipients}"
        assert error is None, "No error should occur for April's leads in development"

    def test_unknown_consultant_uses_default_recipients(self):
        """Test that unknown consultant uses default recipients (graceful fallback)."""
        recipients, error = determine_notification_recipients(
            self.unknown_consultant_lead_details, "production"
        )

        # Unknown consultant should use default recipients (no error)
        assert (
            recipients is None
        ), "Recipients should be None (default) for unknown consultant"
        assert (
            error is None
        ), "No error should be returned for unknown consultant (graceful fallback)"

    def test_empty_consultant_uses_default_recipients(self):
        """Test that empty consultant field uses default recipients (graceful fallback)."""
        recipients, error = determine_notification_recipients(
            self.empty_consultant_lead_details, "production"
        )

        # Empty consultant should use default recipients (no error)
        assert (
            recipients is None
        ), "Recipients should be None (default) for empty consultant"
        assert (
            error is None
        ), "No error should be returned for empty consultant (graceful fallback)"

    def test_missing_consultant_field_uses_default_recipients(self):
        """Test that missing consultant field uses default recipients (graceful fallback)."""
        recipients, error = determine_notification_recipients(
            self.missing_consultant_lead_details, "production"
        )

        # Missing consultant field should use default recipients (no error)
        assert (
            recipients is None
        ), "Recipients should be None (default) for missing consultant field"
        assert (
            error is None
        ), "No error should be returned for missing consultant field (graceful fallback)"

    def test_null_consultant_uses_default_recipients(self):
        """Test that null consultant field uses default recipients (graceful fallback)."""
        recipients, error = determine_notification_recipients(
            self.null_consultant_lead_details, "production"
        )

        # Null consultant should use default recipients (no error)
        assert (
            recipients is None
        ), "Recipients should be None (default) for null consultant"
        assert (
            error is None
        ), "No error should be returned for null consultant (graceful fallback)"

    def test_consultant_case_sensitive_uses_default_recipients(self):
        """Test that case-mismatched consultant uses default recipients (graceful fallback)."""
        recipients, error = determine_notification_recipients(
            self.lowercase_consultant_lead_details, "production"
        )

        # Lowercase "april lowrie" should use default recipients (no error)
        assert (
            recipients is None
        ), "Recipients should be None (default) for case-mismatched consultant"
        assert (
            error is None
        ), "No error should be returned for case-mismatched consultant (graceful fallback)"

    def test_consultant_field_key_constant(self):
        """Test that we're using the correct consultant field key."""
        # This test verifies we're using the right custom field ID
        consultant_field_key = "custom.lcf_TRIulkQaxJArdGl2k89qY6NKR0ZTYkzjRdeILo1h5fi"

        # Verify our test data uses the correct field key
        assert consultant_field_key in self.barbara_lead_details
        assert consultant_field_key in self.april_lead_details
        assert consultant_field_key in self.unknown_consultant_lead_details

        # Verify the field values are what we expect
        assert self.barbara_lead_details[consultant_field_key] == "Barbara Pigg"
        assert self.april_lead_details[consultant_field_key] == "April Lowrie"
        assert self.unknown_consultant_lead_details[consultant_field_key] == "John Doe"

    def test_april_team_recipients_format(self):
        """Test that April's team recipients are formatted correctly."""
        recipients, error = determine_notification_recipients(
            self.april_lead_details, "production"
        )

        # Verify the exact format and order of April's team
        expected_recipients = "april.lowrie@whiteboardgeeks.com,lauren.poche@whiteboardgeeks.com"
        assert (
            recipients == expected_recipients
        ), f"April's team recipients format incorrect: {recipients}"

        # Verify all three team members are included
        recipient_list = recipients.split(",")
        assert (
            len(recipient_list) == 2
        ), f"Expected 2 recipients for April's team, got {len(recipient_list)}"
        assert "april.lowrie@whiteboardgeeks.com" in recipient_list
        assert "lauren.poche@whiteboardgeeks.com" in recipient_list

    def test_development_environment_override(self):
        """Test that development environment always uses Lance only."""
        # Test Barbara in development
        recipients, error = determine_notification_recipients(
            self.barbara_lead_details, "development"
        )
        assert recipients is None, "Barbara should use default (Lance) in development"
        assert error is None

        # Test April in development
        recipients, error = determine_notification_recipients(
            self.april_lead_details, "development"
        )
        assert (
            recipients == "lance@whiteboardgeeks.com"
        ), "April should use Lance only in development"
        assert error is None

    def test_production_environment_behavior(self):
        """Test that production environment uses consultant-specific recipients."""
        # Test Barbara in production
        recipients, error = determine_notification_recipients(
            self.barbara_lead_details, "production"
        )
        assert recipients is None, "Barbara should use default team in production"
        assert error is None

        # Test April in production
        recipients, error = determine_notification_recipients(
            self.april_lead_details, "production"
        )
        expected_april_team = "april.lowrie@whiteboardgeeks.com,lauren.poche@whiteboardgeeks.com"
        assert (
            recipients == expected_april_team
        ), "April should use her team in production"
        assert error is None
