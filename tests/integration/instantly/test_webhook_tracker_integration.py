"""
Integration tests for the webhook tracker functionality.
"""

import os
import json
import uuid
import time
import pytest

from blueprints.instantly import WebhookTracker


class TestWebhookTrackerIntegration:
    """Integration tests for the webhook tracker functionality."""

    def setup_method(self):
        """Setup before each test."""
        # Get Redis URL from environment or use localhost
        self.redis_url = os.environ.get("REDISCLOUD_URL", "redis://localhost:6379")
        self.test_data = {}

        # Create a unique prefix for this test run to avoid conflicts
        self.test_prefix = f"test_webhook_tracker_{uuid.uuid4().hex[:8]}"

        # Create a webhook tracker with a short expiration for testing
        self.tracker = WebhookTracker(expiration_seconds=5)

        # Store original prefix to restore later
        self.original_prefix = self.tracker.prefix

        # Set a unique prefix for this test run
        self.tracker.prefix = f"{self.test_prefix}:"

    def test_redis_connection(self):
        """Test that we can connect to Redis."""
        print(f"\n=== TESTING REDIS CONNECTION: {self.redis_url} ===")

        # Check if Redis is available - fail instead of skip
        assert self.tracker.redis is not None, "Redis is not available - test failed"

        # Try a simple ping operation
        try:
            self.tracker.redis.ping()
            print("Successfully connected to Redis")
        except Exception as e:
            pytest.fail(f"Failed to connect to Redis: {str(e)}")

    def test_add_and_get(self):
        """Test adding and retrieving webhook data from Redis."""
        print("\n=== TESTING ADD AND GET OPERATIONS ===")

        # Check if Redis is available - fail instead of skip
        assert self.tracker.redis is not None, "Redis is not available - test failed"

        # Generate a unique task ID
        task_id = f"task_{uuid.uuid4().hex[:8]}"

        # Create test webhook data
        webhook_data = {
            "route": "add_lead",
            "lead_id": f"lead_{uuid.uuid4().hex[:8]}",
            "campaign_name": "Test Campaign",
            "processed": True,
            "test_run": True,  # Mark as test data
        }

        # Add to tracker
        self.tracker.add(task_id, webhook_data)
        print(f"Added webhook data for task_id: {task_id}")

        # Store for cleanup
        self.test_data["task_id"] = task_id

        # Retrieve and verify
        stored_data = self.tracker.get(task_id)
        print(f"Retrieved data: {stored_data}")

        # Verify data was stored correctly
        assert stored_data["route"] == webhook_data["route"]
        assert stored_data["lead_id"] == webhook_data["lead_id"]
        assert stored_data["campaign_name"] == webhook_data["campaign_name"]
        assert stored_data["processed"] == webhook_data["processed"]
        assert "timestamp" in stored_data  # Should auto-add timestamp

    def test_get_all(self):
        """Test retrieving all webhook data."""
        print("\n=== TESTING GET_ALL OPERATION ===")

        # Check if Redis is available - fail instead of skip
        assert self.tracker.redis is not None, "Redis is not available - test failed"

        # Add multiple webhook entries
        task_ids = []
        for i in range(3):
            task_id = f"task_{uuid.uuid4().hex[:8]}"
            task_ids.append(task_id)

            webhook_data = {
                "route": "add_lead",
                "lead_id": f"lead_{uuid.uuid4().hex[:8]}",
                "campaign_name": f"Test Campaign {i}",
                "processed": True,
                "test_run": True,  # Mark as test data
            }

            self.tracker.add(task_id, webhook_data)
            print(f"Added webhook data for task_id: {task_id}")

        # Store for cleanup
        self.test_data["task_ids"] = task_ids

        # Get all webhooks
        all_webhooks = self.tracker.get_all()
        print(f"Retrieved {len(all_webhooks)} webhooks")

        # Verify all task IDs are in the result
        # Note: There might be other test data in Redis, so we only check our test IDs
        for task_id in task_ids:
            assert task_id in all_webhooks
            assert all_webhooks[task_id]["route"] == "add_lead"
            assert "test_run" in all_webhooks[task_id]

    def test_expiration(self):
        """Test that webhooks expire after the specified time."""
        print("\n=== TESTING EXPIRATION ===")

        # Check if Redis is available - fail instead of skip
        assert self.tracker.redis is not None, "Redis is not available - test failed"

        # Create a tracker with a very short expiration
        short_tracker = WebhookTracker(expiration_seconds=2)
        short_tracker.prefix = f"{self.test_prefix}_exp:"

        # Generate a unique task ID
        task_id = f"task_{uuid.uuid4().hex[:8]}"

        # Create test webhook data
        webhook_data = {
            "route": "add_lead",
            "lead_id": f"lead_{uuid.uuid4().hex[:8]}",
            "campaign_name": "Expiring Test",
            "processed": True,
            "test_run": True,  # Mark as test data
        }

        # Add to tracker
        short_tracker.add(task_id, webhook_data)
        print(f"Added webhook data with 2-second expiration for task_id: {task_id}")

        # Verify it exists immediately
        assert short_tracker.get(task_id)["route"] == "add_lead"

        # Wait for expiration
        print("Waiting for expiration (3 seconds)...")
        time.sleep(3)

        # Verify it's gone
        result = short_tracker.get(task_id)
        print(f"After expiration, get() returned: {result}")
        assert result == {} or not result, "Data should have expired"

    def test_webhook_status_endpoint(self):
        """Test the webhook status endpoint with real Redis."""
        print("\n=== TESTING WEBHOOK STATUS ENDPOINT ===")

        # Check if Redis is available - fail instead of skip
        assert self.tracker.redis is not None, "Redis is not available - test failed"

        # Import Flask and create a test client
        from flask import Flask
        from blueprints.instantly import instantly_bp, _webhook_tracker

        # Store original webhook tracker prefix
        original_prefix = _webhook_tracker.prefix

        try:
            # Set a unique prefix for this test
            _webhook_tracker.prefix = f"{self.test_prefix}_endpoint:"

            # Create a Flask app and register the blueprint
            app = Flask(__name__)
            app.register_blueprint(instantly_bp, url_prefix="/instantly")

            # Create a test client
            client = app.test_client()

            # Add a test webhook
            task_id = f"task_{uuid.uuid4().hex[:8]}"
            webhook_data = {
                "route": "add_lead",
                "lead_id": f"lead_{uuid.uuid4().hex[:8]}",
                "campaign_name": "Endpoint Test",
                "processed": True,
                "test_run": True,
            }

            _webhook_tracker.add(task_id, webhook_data)
            print(f"Added webhook data for endpoint test, task_id: {task_id}")

            # Test getting a specific task
            response = client.get(f"/instantly/webhooks/status?task_id={task_id}")
            assert response.status_code == 200
            data = json.loads(response.data)
            print(f"Response for specific task: {data}")

            assert data["status"] == "success"
            assert data["data"]["route"] == "add_lead"
            assert data["data"]["campaign_name"] == "Endpoint Test"

            # Test getting all webhooks
            response = client.get("/instantly/webhooks/status")
            assert response.status_code == 200
            data = json.loads(response.data)
            print(f"Response for all webhooks: {data}")

            assert data["status"] == "success"
            assert task_id in data["data"]
            assert data["data"][task_id]["route"] == "add_lead"

        finally:
            # Restore original prefix
            _webhook_tracker.prefix = original_prefix

    def teardown_method(self):
        """Cleanup after each test."""
        # Restore original prefix
        self.tracker.prefix = self.original_prefix

        # Clean up test data from Redis if available
        if self.tracker.redis:
            # Delete individual task
            if "task_id" in self.test_data:
                key = f"{self.test_prefix}:{self.test_data['task_id']}"
                self.tracker.redis.delete(key)

            # Delete multiple tasks
            if "task_ids" in self.test_data:
                for task_id in self.test_data["task_ids"]:
                    key = f"{self.test_prefix}:{task_id}"
                    self.tracker.redis.delete(key)

            # Delete any keys with our test prefix
            for prefix in [
                self.test_prefix,
                f"{self.test_prefix}_exp",
                f"{self.test_prefix}_endpoint",
            ]:
                keys = self.tracker.redis.keys(f"{prefix}:*")
                if keys:
                    self.tracker.redis.delete(*keys)

            print(
                f"Cleaned up {len(self.test_data.get('task_ids', []))+1 if 'task_id' in self.test_data else 0} test keys from Redis"
            )
