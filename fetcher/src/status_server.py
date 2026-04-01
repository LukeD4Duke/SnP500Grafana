"""Small HTTP status server for fetcher health and readiness reporting."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Any

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class FetcherStatus:
    """Mutable runtime status exposed by the fetcher status endpoint."""

    phase: str = "booting"
    ready: bool = False
    scheduler_started: bool = False
    startup_mode: str = "unknown"
    startup_backfill_mode: str = "unknown"
    startup_backfill_requested: bool = False
    startup_backfill_scheduled: bool = False
    startup_backfill_running: bool = False
    startup_backfill_completed: bool = False
    startup_backfill_range: dict[str, str] | None = None
    last_incremental_sync: dict[str, Any] | None = None
    last_backfill_sync: dict[str, Any] | None = None
    last_message: str = "Booting"
    updated_at: str = field(default_factory=_utc_now_iso)


class FetcherStatusStore:
    """Thread-safe status storage for runtime progress."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._status = FetcherStatus()

    def update(self, **changes: Any) -> dict[str, Any]:
        with self._lock:
            for key, value in changes.items():
                setattr(self._status, key, value)
            self._status.updated_at = _utc_now_iso()
            return asdict(self._status)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return asdict(self._status)


def start_status_server(status_store: FetcherStatusStore, port: int) -> ThreadingHTTPServer:
    """Start a lightweight HTTP server exposing health and status endpoints."""

    class StatusHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            snapshot = status_store.snapshot()
            path = self.path.split("?", 1)[0]
            if path == "/healthz":
                self._write_json(200, {"ok": True, "phase": snapshot["phase"]})
                return
            if path == "/readyz":
                status_code = 200 if snapshot["ready"] else 503
                self._write_json(status_code, {"ready": snapshot["ready"], "phase": snapshot["phase"]})
                return
            if path == "/status":
                self._write_json(200, snapshot)
                return
            self._write_json(404, {"error": "not_found"})

        def log_message(self, format: str, *args: object) -> None:
            return

        def _write_json(self, status_code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("0.0.0.0", port), StatusHandler)
    thread = Thread(target=server.serve_forever, name="fetcher-status-server", daemon=True)
    thread.start()
    logger.info("Started fetcher status server on port %d", port)
    return server
