# temporal_service.py
import asyncio
import os
import threading
from typing import Optional, Awaitable

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

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

        # Env configuration (override in Heroku config vars)
        self._target = os.getenv("TEMPORAL_TARGET", "localhost:7233")
        self._namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
        self._tls_enabled = os.getenv("TEMPORAL_TLS", "false").lower() == "true"

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
                Client.connect(
                    self._target,
                    namespace=self._namespace,
                    data_converter=pydantic_data_converter,
                    tls=self._build_tls() if self._tls_enabled else None,
                ),
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

    def run(self, coro: Awaitable):
        """Run an async Temporal coroutine on the service loop and block until done."""
        self.ensure_started()
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    # ----- helpers -----------------------------------------------------------

    def _build_tls(self):
        if TLSConfig is None:
            return None
        server_name = os.getenv("TEMPORAL_TLS_SERVER_NAME")  # e.g. <ns>.<acct>.tmprl.cloud
        root_ca_pem = os.getenv("TEMPORAL_TLS_ROOT_CA_PEM")
        client_cert_pem = os.getenv("TEMPORAL_MTLS_CERT_PEM")
        client_key_pem = os.getenv("TEMPORAL_MTLS_KEY_PEM")
        return TLSConfig(
            domain=server_name,
            server_root_ca_cert=root_ca_pem.encode() if root_ca_pem else None,
            client_cert=client_cert_pem.encode() if client_cert_pem else None,
            client_private_key=client_key_pem.encode() if client_key_pem else None,
        )


# Module-level singleton (one per *process*)
temporal = TemporalService()
