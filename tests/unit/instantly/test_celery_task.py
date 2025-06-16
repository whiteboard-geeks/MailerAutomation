"""
Unit tests for the Instantly Celery task process_lead_batch_task.

These tests mock external dependencies and focus on testing the task logic.
"""

from unittest.mock import patch, MagicMock

from blueprints.instantly import process_lead_batch_task


class TestProcessLeadBatchTask:
    """Test the process_lead_batch_task Celery task."""

    def setup_method(self):
        """Set up test data for each test."""
        self.sample_payload = {
            "event": {
                "action": "created",
                "object_type": "task.lead",
                "data": {
                    "id": "task_test123",
                    "text": "Instantly: Test Campaign",
                    "lead_id": "lead_test456",
                    "is_complete": False,
                },
            }
        }

        self.sample_lead_details = {
            "id": "lead_test456",
            "name": "Test Lead",
            "contacts": [
                {
                    "name": "John Doe",
                    "emails": [{"email": "john.doe@example.com"}],
                }
            ],
            "custom.lcf_tRacWU9nMn0l2i0xhizYpewewmw995aWYaJKgDgDb9o": "Test Company",
            "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9": "Mon 1/15 to Austin, TX",
        }

        self.sample_campaign_check = {
            "exists": True,
            "campaign_id": "campaign_test789",
            "campaign_data": {"id": "campaign_test789", "name": "Test Campaign"},
        }

        self.sample_instantly_result = {
            "status": "success",
            "lead_id": "instantly_lead_123",
            "message": "Lead added to Instantly campaign",
            "response": {"id": "instantly_lead_123"},
        }

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly._webhook_tracker")
    @patch("blueprints.instantly.add_to_instantly_campaign")
    @patch("blueprints.instantly.get_lead_by_id")
    @patch("blueprints.instantly.campaign_exists")
    def test_successful_lead_processing(
        self,
        mock_campaign_exists,
        mock_get_lead_by_id,
        mock_add_to_instantly,
        mock_webhook_tracker,
        mock_send_email,
    ):
        """Test successful processing of a lead batch task."""
        # Set up mocks
        mock_campaign_exists.return_value = self.sample_campaign_check
        mock_get_lead_by_id.return_value = self.sample_lead_details
        mock_add_to_instantly.return_value = self.sample_instantly_result

        # Mock the webhook tracker
        mock_webhook_tracker.add = MagicMock()
        # Mock the get method to return initial webhook data (simulating existing entry)
        initial_webhook_data = {
            "route": "add_lead",
            "lead_id": "lead_test456",
            "campaign_name": "Test Campaign",
            "processed": False,
            "status": "processing",
        }
        mock_webhook_tracker.get = MagicMock(return_value=initial_webhook_data)

        # Execute the task
        result = process_lead_batch_task(self.sample_payload)

        # Verify the result
        assert result["status"] == "success"
        assert result["lead_id"] == "lead_test456"
        assert result["close_task_id"] == "task_test123"
        assert result["campaign_name"] == "Test Campaign"
        assert result["campaign_id"] == "campaign_test789"
        assert "celery_task_id" in result

        # Verify function calls
        mock_campaign_exists.assert_called_once_with("Test Campaign")
        mock_get_lead_by_id.assert_called_once_with("lead_test456")
        mock_add_to_instantly.assert_called_once_with(
            campaign_id="campaign_test789",
            email="john.doe@example.com",
            first_name="John",
            last_name="Doe",
            company_name="Test Company",
            date_location="Mon 1/15 to Austin, TX",
        )

        # Verify webhook tracking
        mock_webhook_tracker.add.assert_called_once()
        call_args = mock_webhook_tracker.add.call_args
        assert call_args[0][0] == "task_test123"  # close_task_id
        webhook_data = call_args[0][1]
        assert webhook_data["route"] == "add_lead"
        assert webhook_data["lead_id"] == "lead_test456"
        assert webhook_data["campaign_name"] == "Test Campaign"
        assert webhook_data["processed"] is True

        # Verify no error emails were sent
        mock_send_email.assert_not_called()

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly.campaign_exists")
    def test_campaign_not_found_error(self, mock_campaign_exists, mock_send_email):
        """Test error handling when campaign doesn't exist."""
        # Set up mock to return campaign not found
        mock_campaign_exists.return_value = {"exists": False}

        # Execute the task
        result = process_lead_batch_task(self.sample_payload)

        # Verify error result
        assert result["status"] == "error"
        assert "does not exist in Instantly" in result["message"]

        # Verify error email was sent
        mock_send_email.assert_called_once()
        call_args = mock_send_email.call_args
        assert "Campaign Not Found" in call_args[1]["subject"]

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly.get_lead_by_id")
    @patch("blueprints.instantly.campaign_exists")
    def test_lead_details_not_found_error(
        self, mock_campaign_exists, mock_get_lead_by_id, mock_send_email
    ):
        """Test error handling when lead details can't be retrieved."""
        # Set up mocks
        mock_campaign_exists.return_value = self.sample_campaign_check
        mock_get_lead_by_id.return_value = None

        # Execute the task
        result = process_lead_batch_task(self.sample_payload)

        # Verify error result
        assert result["status"] == "error"
        assert "Could not retrieve lead details" in result["message"]

        # Verify error email was sent
        mock_send_email.assert_called_once()

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly.get_lead_by_id")
    @patch("blueprints.instantly.campaign_exists")
    def test_no_email_found_error(
        self, mock_campaign_exists, mock_get_lead_by_id, mock_send_email
    ):
        """Test error handling when lead has no email."""
        # Set up mocks with lead that has no email
        mock_campaign_exists.return_value = self.sample_campaign_check
        lead_without_email = self.sample_lead_details.copy()
        lead_without_email["contacts"] = [{"name": "John Doe", "emails": []}]
        mock_get_lead_by_id.return_value = lead_without_email

        # Execute the task
        result = process_lead_batch_task(self.sample_payload)

        # Verify error result
        assert result["status"] == "error"
        assert "No email found" in result["message"]

        # Verify error email was sent
        mock_send_email.assert_called_once()

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly.add_to_instantly_campaign")
    @patch("blueprints.instantly.get_lead_by_id")
    @patch("blueprints.instantly.campaign_exists")
    def test_instantly_api_error(
        self,
        mock_campaign_exists,
        mock_get_lead_by_id,
        mock_add_to_instantly,
        mock_send_email,
    ):
        """Test error handling when Instantly API fails."""
        # Set up mocks
        mock_campaign_exists.return_value = self.sample_campaign_check
        mock_get_lead_by_id.return_value = self.sample_lead_details
        mock_add_to_instantly.return_value = {
            "status": "error",
            "message": "API rate limit exceeded",
        }

        # Execute the task
        result = process_lead_batch_task(self.sample_payload)

        # Verify error result
        assert result["status"] == "error"
        assert "Failed to add lead to Instantly" in result["message"]

        # Verify error email was sent
        mock_send_email.assert_called_once()

    @patch("blueprints.instantly.send_email")
    def test_invalid_campaign_name_extraction(self, mock_send_email):
        """Test error handling when campaign name can't be extracted."""
        # Set up mock
        mock_send_email.return_value = {"status": "success"}

        # Create payload with invalid task text
        invalid_payload = {
            "event": {
                "data": {
                    "id": "task_test123",
                    "text": "Instantly",  # No campaign name
                    "lead_id": "lead_test456",
                }
            }
        }

        # Execute the task
        result = process_lead_batch_task(invalid_payload)

        # Verify error result
        assert result["status"] == "error"
        assert "Could not extract campaign name" in result["message"]

        # Verify error email was sent
        mock_send_email.assert_called_once()
        call_args = mock_send_email.call_args
        assert "Campaign Name Extraction Error" in call_args[1]["subject"]

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly.campaign_exists")
    def test_general_exception_handling(self, mock_campaign_exists, mock_send_email):
        """Test general exception handling in the task."""
        # Set up mock to raise an exception during campaign checking
        mock_campaign_exists.side_effect = Exception("Simulated database error")
        mock_send_email.return_value = {"status": "success"}

        # Use a valid payload structure that would pass early validation
        valid_payload = {
            "event": {
                "data": {
                    "id": "task_test123",
                    "text": "Instantly: Test Campaign",
                    "lead_id": "lead_test456",
                }
            }
        }

        # Execute the task
        result = process_lead_batch_task(valid_payload)

        # Verify error result
        assert result["status"] == "error"
        assert "celery_task_id" in result

        # Verify error email was sent
        mock_send_email.assert_called_once()
        call_args = mock_send_email.call_args
        assert "Async Processing Error" in call_args[1]["subject"]

    def test_name_splitting_functionality(self):
        """Test that names are properly split into first and last names."""
        from blueprints.instantly import split_name

        test_cases = [
            ("John Doe", ("John", "Doe")),
            ("John Michael Doe", ("John Michael", "Doe")),
            ("John", ("John", "")),
            ("", ("", "")),
            ("   ", ("", "")),
        ]

        for full_name, expected in test_cases:
            result = split_name(full_name)
            assert (
                result == expected
            ), f"Failed on input '{full_name}': expected {expected}, got {result}"

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly._webhook_tracker")
    @patch("blueprints.instantly.add_to_instantly_campaign")
    @patch("blueprints.instantly.get_lead_by_id")
    @patch("blueprints.instantly.campaign_exists")
    def test_complex_name_handling(
        self,
        mock_campaign_exists,
        mock_get_lead_by_id,
        mock_add_to_instantly,
        mock_webhook_tracker,
        mock_send_email,
    ):
        """Test handling of complex names and edge cases."""
        # Set up lead with complex name
        complex_lead = self.sample_lead_details.copy()
        complex_lead["contacts"] = [
            {
                "name": "Dr. Jane Mary Smith-Jones",
                "emails": [{"email": "jane.smith@example.com"}],
            }
        ]

        # Set up mocks
        mock_campaign_exists.return_value = self.sample_campaign_check
        mock_get_lead_by_id.return_value = complex_lead
        mock_add_to_instantly.return_value = self.sample_instantly_result
        mock_webhook_tracker.add = MagicMock()
        # Mock the get method to return initial webhook data
        initial_webhook_data = {
            "route": "add_lead",
            "lead_id": "lead_test456",
            "campaign_name": "Test Campaign",
            "processed": False,
            "status": "processing",
        }
        mock_webhook_tracker.get = MagicMock(return_value=initial_webhook_data)

        # Execute the task
        result = process_lead_batch_task(self.sample_payload)

        # Verify success
        assert result["status"] == "success"

        # Verify the name was properly split and passed to Instantly
        mock_add_to_instantly.assert_called_once()
        call_args = mock_add_to_instantly.call_args[1]
        assert call_args["first_name"] == "Dr. Jane Mary"
        assert call_args["last_name"] == "Smith-Jones"
        assert call_args["email"] == "jane.smith@example.com"

    @patch("blueprints.instantly.send_email")
    @patch("blueprints.instantly._webhook_tracker")
    @patch("blueprints.instantly.add_to_instantly_campaign")
    @patch("blueprints.instantly.get_lead_by_id")
    @patch("blueprints.instantly.campaign_exists")
    def test_missing_custom_fields_handling(
        self,
        mock_campaign_exists,
        mock_get_lead_by_id,
        mock_add_to_instantly,
        mock_webhook_tracker,
        mock_send_email,
    ):
        """Test handling when custom fields are missing."""
        # Set up lead without custom fields
        lead_without_custom = {
            "id": "lead_test456",
            "name": "Test Lead",
            "contacts": [
                {
                    "name": "John Doe",
                    "emails": [{"email": "john.doe@example.com"}],
                }
            ],
            # Missing custom fields
        }

        # Set up mocks
        mock_campaign_exists.return_value = self.sample_campaign_check
        mock_get_lead_by_id.return_value = lead_without_custom
        mock_add_to_instantly.return_value = self.sample_instantly_result
        mock_webhook_tracker.add = MagicMock()
        # Mock the get method to return initial webhook data
        initial_webhook_data = {
            "route": "add_lead",
            "lead_id": "lead_test456",
            "campaign_name": "Test Campaign",
            "processed": False,
            "status": "processing",
        }
        mock_webhook_tracker.get = MagicMock(return_value=initial_webhook_data)

        # Execute the task
        result = process_lead_batch_task(self.sample_payload)

        # Verify success
        assert result["status"] == "success"

        # Verify empty strings were passed for missing custom fields
        mock_add_to_instantly.assert_called_once()
        call_args = mock_add_to_instantly.call_args[1]
        assert call_args["company_name"] == ""
        assert call_args["date_location"] == ""
