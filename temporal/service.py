# temporal_service.py
import asyncio
import threading
from typing import Any, Optional, Coroutine

from temporalio.client import Client

from temporal.client_provider import get_temporal_client

try:
    # Present in Temporal Python SDK; only used if you enable TLS/mTLS via env
    from temporalio.service import TLSConfig  # type: ignore
except Exception:  # pragma: no cover
    TLSConfig = None  # type: ignore


class TemporalService:
    """
    Owns an asyncio event loop in a background thread and a single shared Temporal Client.
    Safe to import from Flask routes and Celery tasks (1 client per *process*).
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._lock = threading.Lock()
        self.client: Optional[Client] = None

    # ----- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Idempotent: start loop thread and connect the client once."""
        if self.client:
            return
        with self._lock:
            if self.client:  # double-checked
                return
            if not self._thread.is_alive():
                self._thread.start()
            fut = asyncio.run_coroutine_threadsafe(
                get_temporal_client(),
                self._loop,
            )
            self.client = fut.result()  # raise if connection fails

    def ensure_started(self) -> None:
        if not self.client:
            self.start()

    def stop(self) -> None:
        """Attempt to close the client (if supported) and stop the loop."""
        if self.client:
            # Some SDK versions expose async close(); handle both cases safely.
            close_fn = getattr(self.client, "close", None)
            if callable(close_fn):
                try:
                    if asyncio.iscoroutinefunction(close_fn):
                        asyncio.run_coroutine_threadsafe(close_fn(), self._loop).result()
                    else:
                        close_fn()
                except Exception:
                    pass
            self.client = None

        try:
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread.is_alive():
                self._thread.join(timeout=2)
        except Exception:
            pass

    # ----- sync entrypoint to run async Temporal calls -----------------------

    def run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Run an async Temporal coroutine on the service loop and block until done."""
        self.ensure_started()
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    # ----- helpers -----------------------------------------------------------


# Module-level singleton (one per *process*)
temporal = TemporalService()
