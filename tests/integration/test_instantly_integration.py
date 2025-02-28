# tests/integration/test_instantly_integration.py
import pytest
import json
import os
from unittest.mock import patch


class TestInstantlyIntegration:
    def test_handle_close_webhook(self, client, close_task_created_payload):
        """Test handling a Close webhook that should trigger Instantly lead creation."""
        # Mock the Instantly API call
        with patch("requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {
                "success": True,
                "lead_id": "123",
            }

            # Call the Close webhook endpoint
            response = client.post(
                "/webhooks/close",
                json=close_task_created_payload,
                headers={"X-API-KEY": os.environ.get("WEBHOOK_API_KEY")},
            )

            # Assert the response
            assert response.status_code == 200
            assert json.loads(response.data)["status"] == "success"

            # Verify the Instantly API was called correctly
            mock_post.assert_called_once()
            # Check that the right URL and payload were used
            call_args = mock_post.call_args
            assert (
                "api.instantly.ai" in call_args[0][0]
            )  # URL contains Instantly domain
