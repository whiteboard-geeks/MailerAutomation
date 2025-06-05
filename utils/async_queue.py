"""
Request queue system for handling burst API requests with rate limiting.

This module provides asynchronous request queuing functionality that integrates
with the Redis rate limiter to ensure API requests are processed in a controlled
manner without overwhelming external APIs.
"""

import time
import json
import threading
import redis
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional, Dict, Any
import logging
from dataclasses import dataclass
from utils.rate_limiter import RedisRateLimiter, APIRateConfig

logger = logging.getLogger(__name__)


@dataclass
class QueueStatus:
    """Status information for the request queue."""

    queued: int
    processing: int
    completed: int
    failed: int
    workers_running: bool
    queue_name: str


class InstantlyRequestQueue:
    """
    Redis-based request queue with worker pool and rate limiting integration.

    This queue system allows for:
    - Immediate response to API calls (no HTTP timeout)
    - Background processing with controlled rate limiting
    - Worker pool for concurrent processing
    - Integration with existing Redis rate limiter
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        max_workers: int = 5,
        queue_name: str = "instantly_requests",
        rate_limiter: Optional[RedisRateLimiter] = None,
    ):
        """
        Initialize the request queue.

        Args:
            redis_client: Redis client for queue storage
            max_workers: Number of worker threads (default: 5)
            queue_name: Name of the Redis queue (default: "instantly_requests")
            rate_limiter: Rate limiter instance (optional, will create default if None)
        """
        self.redis_client = redis_client
        self.max_workers = max_workers
        self.queue_name = queue_name

        # Initialize rate limiter if not provided
        if rate_limiter is None:
            self.rate_limiter = RedisRateLimiter(
                redis_client=redis_client,
                api_config=APIRateConfig.instantly(),
                safety_factor=0.8,
            )
        else:
            self.rate_limiter = rate_limiter

        # Queue keys for Redis
        self.queue_key = f"queue:{queue_name}"
        self.processing_key = f"processing:{queue_name}"
        self.completed_key = f"completed:{queue_name}"
        self.failed_key = f"failed:{queue_name}"

        # Worker pool management
        self.executor: Optional[ThreadPoolExecutor] = None
        self.workers_running = False
        self.stop_event = threading.Event()

        # Track pending futures
        self.pending_futures: Dict[str, Future] = {}
        self.futures_lock = threading.Lock()

        logger.info(
            f"InstantlyRequestQueue initialized: {queue_name}, {max_workers} workers"
        )

    def enqueue_request(self, request_data: Dict[str, Any]) -> Future:
        """
        Add a request to the queue and return a Future for the result.

        Args:
            request_data: Request data to be processed

        Returns:
            Future object that will contain the result when processing completes
        """
        # Generate unique request ID
        request_id = f"req_{int(time.time() * 1000000)}"

        # Create Future for this request
        future = Future()

        # Prepare request payload
        request_payload = {
            "id": request_id,
            "data": request_data,
            "timestamp": time.time(),
        }

        # Add to Redis queue
        try:
            self.redis_client.lpush(self.queue_key, json.dumps(request_payload))

            # Track the future
            with self.futures_lock:
                self.pending_futures[request_id] = future

            logger.debug(f"Enqueued request {request_id}")
            return future

        except Exception as e:
            logger.error(f"Failed to enqueue request: {e}")
            future.set_exception(e)
            return future

    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current queue status.

        Returns:
            Dictionary containing queue status information
        """
        try:
            queued = self.redis_client.llen(self.queue_key) or 0
            processing = self.redis_client.llen(self.processing_key) or 0
            completed = self.redis_client.llen(self.completed_key) or 0
            failed = self.redis_client.llen(self.failed_key) or 0

            return {
                "queued": queued,
                "processing": processing,
                "completed": completed,
                "failed": failed,
                "workers_running": self.workers_running,
                "queue_name": self.queue_name,
            }
        except Exception as e:
            logger.error(f"Failed to get queue status: {e}")
            return {
                "queued": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0,
                "workers_running": self.workers_running,
                "queue_name": self.queue_name,
                "error": str(e),
            }

    def start_workers(self) -> None:
        """Start the worker pool to process requests."""
        if self.workers_running:
            logger.warning("Workers are already running")
            return

        self.stop_event.clear()
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix=f"queue_{self.queue_name}"
        )

        # Start worker threads
        for i in range(self.max_workers):
            self.executor.submit(self._worker_loop, i)

        self.workers_running = True
        logger.info(f"Started {self.max_workers} workers for queue {self.queue_name}")

    def stop_workers(self) -> None:
        """Stop the worker pool."""
        if not self.workers_running:
            logger.warning("Workers are not running")
            return

        self.stop_event.set()
        self.workers_running = False

        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None

        logger.info(f"Stopped workers for queue {self.queue_name}")

    def is_running(self) -> bool:
        """Check if workers are currently running."""
        return self.workers_running

    def _worker_loop(self, worker_id: int) -> None:
        """
        Main worker loop that processes requests from the queue.

        Args:
            worker_id: Unique identifier for this worker
        """
        logger.info(f"Worker {worker_id} started for queue {self.queue_name}")

        while not self.stop_event.is_set():
            try:
                # Get request from queue (blocking with timeout)
                request_json = self.redis_client.brpop(self.queue_key, timeout=1)

                if request_json is None:
                    continue  # Timeout, check stop event and try again

                # Parse request
                _, request_data = request_json
                request_payload = json.loads(request_data.decode("utf-8"))
                request_id = request_payload["id"]

                logger.debug(f"Worker {worker_id} processing request {request_id}")

                # Move to processing queue
                self.redis_client.lpush(self.processing_key, request_data)

                # Process the request with rate limiting
                result = self._process_request(request_payload, worker_id)

                # Move to completed/failed queue based on result
                if result.get("success", False):
                    self.redis_client.lpush(
                        self.completed_key,
                        json.dumps(
                            {
                                "id": request_id,
                                "result": result,
                                "completed_at": time.time(),
                            }
                        ),
                    )
                else:
                    self.redis_client.lpush(
                        self.failed_key,
                        json.dumps(
                            {
                                "id": request_id,
                                "error": result.get("error", "Unknown error"),
                                "failed_at": time.time(),
                            }
                        ),
                    )

                # Remove from processing queue
                self.redis_client.lrem(self.processing_key, 1, request_data)

                # Complete the future
                with self.futures_lock:
                    if request_id in self.pending_futures:
                        future = self.pending_futures.pop(request_id)
                        if result.get("success", False):
                            future.set_result(result)
                        else:
                            future.set_exception(
                                Exception(result.get("error", "Processing failed"))
                            )

            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                time.sleep(1)  # Brief pause before retrying

        logger.info(f"Worker {worker_id} stopped for queue {self.queue_name}")

    def _process_request(
        self, request_payload: Dict[str, Any], worker_id: int
    ) -> Dict[str, Any]:
        """
        Process a single request with rate limiting.

        Args:
            request_payload: The request to process
            worker_id: ID of the worker processing this request

        Returns:
            Dictionary containing the result or error information
        """
        request_id = request_payload["id"]
        request_data = request_payload["data"]

        try:
            # Apply rate limiting
            rate_limit_key = f"instantly_api_{self.queue_name}"

            # Wait for rate limiter token
            max_attempts = 10
            for attempt in range(max_attempts):
                if self.rate_limiter.acquire_token(rate_limit_key):
                    break

                if self.stop_event.is_set():
                    return {"success": False, "error": "Processing stopped"}

                # Wait before retrying
                time.sleep(0.5)
                logger.debug(
                    f"Worker {worker_id} waiting for rate limit token (attempt {attempt + 1})"
                )
            else:
                return {"success": False, "error": "Rate limit exceeded"}

            # Simulate request processing (in real implementation, this would call Instantly API)
            logger.debug(
                f"Worker {worker_id} processing request {request_id} with data: {request_data}"
            )

            # For testing, just simulate some processing time
            time.sleep(0.1)  # Simulate API call time

            return {
                "success": True,
                "request_id": request_id,
                "worker_id": worker_id,
                "processed_at": time.time(),
                "data": request_data,
            }

        except Exception as e:
            logger.error(f"Error processing request {request_id}: {e}")
            return {"success": False, "error": str(e)}

    def cleanup(self) -> None:
        """Clean up queue data from Redis."""
        try:
            # Stop workers first
            if self.workers_running:
                self.stop_workers()

            # Clear all queue data
            keys_to_delete = [
                self.queue_key,
                self.processing_key,
                self.completed_key,
                self.failed_key,
            ]

            for key in keys_to_delete:
                self.redis_client.delete(key)

            # Clear pending futures
            with self.futures_lock:
                for future in self.pending_futures.values():
                    if not future.done():
                        future.set_exception(Exception("Queue cleanup"))
                self.pending_futures.clear()

            logger.info(f"Cleaned up queue {self.queue_name}")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
