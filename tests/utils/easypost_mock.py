import json
from unittest.mock import MagicMock


class EasyPostMock:
    """
    Utility class to mock EasyPost API calls for testing.
    """

    @staticmethod
    def load_mock_response(filename):
        """Load a mock response from a JSON file."""
        with open(filename, "r") as f:
            return json.load(f)

    @classmethod
    def mock_tracker_create(cls, monkeypatch, mock_response_file=None):
        """
        Mock the EasyPost tracker.create method.

        Args:
            monkeypatch: pytest's monkeypatch fixture
            mock_response_file: path to JSON file with mock response (optional)

        Returns:
            The mock object for further customization if needed
        """
        # Default mock response
        if mock_response_file is None:
            mock_response_file = (
                "tests/integration/easypost/mock_create_tracker_response.json"
            )

        # Load mock response
        mock_response = cls.load_mock_response(mock_response_file)

        # Create a mock Tracker object
        mock_tracker = MagicMock()
        mock_tracker.id = mock_response["id"]
        mock_tracker.tracking_code = mock_response["tracking_code"]
        mock_tracker.carrier = mock_response["carrier"]
        mock_tracker.status = mock_response["status"]

        # For JSON serialization by the API
        mock_tracker.__dict__.update(mock_response)

        # Create mock tracker.create method
        mock_create = MagicMock(return_value=mock_tracker)

        # Create mock tracker object
        mock_tracker_obj = MagicMock()
        mock_tracker_obj.create = mock_create

        # Create mock client
        mock_client = MagicMock()
        mock_client.tracker = mock_tracker_obj

        # Patch the get_easypost_client function to return our mock
        monkeypatch.setattr(
            "blueprints.easypost.get_easypost_client", lambda: mock_client
        )
        monkeypatch.setattr("blueprints.easypost.easypost_client", mock_client)

        return mock_tracker_obj
