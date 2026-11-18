"""Serve the metrics dashboard and live snapshots from one local HTTP server."""

from __future__ import annotations

import argparse
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import ip_address
import json
from pathlib import Path
import signal
import sys
import threading
import time
from typing import Any, Sequence

from Bluesky_interface.post_fetcher_interface import ShardedPostQueue
from metrics.snapshot import build_snapshot
from spoiler_interface.spoiler_interface import ShardedLabelQueue


class SnapshotProvider:
    """Continuously build snapshots without blocking HTTP request threads."""

    def __init__(
        self,
        post_databases: Sequence[Path],
        label_databases: Sequence[Path],
        tag_config: Path,
        *,
        window_seconds: int,
        detector_workers: int = 0,
        publisher_workers: int = 0,
        live_rate_window_seconds: float = 10.0,
        caught_up_lag_seconds: float = 10.0,
        minimum_refresh_seconds: float = 0.5,
    ):
        self.post_queue = ShardedPostQueue(post_databases, migrate=False)
        self.label_queue = ShardedLabelQueue(label_databases, migrate=False)
        self.tag_config = tag_config
        self.window_seconds = window_seconds
        self.detector_workers = detector_workers
        self.publisher_workers = publisher_workers
        self.caught_up_lag_seconds = caught_up_lag_seconds
        self.minimum_refresh_seconds = minimum_refresh_seconds
        self.rate_tracker = LiveRateTracker(live_rate_window_seconds)
        self._lock = threading.Lock()
        self._snapshot: dict[str, Any] | None = None
        self._built_at = 0.0
        self._last_error: str | None = None
        self._stop_requested = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_reported_error: str | None = None
        self._last_error_reported_at = 0.0

    def get(self) -> dict[str, Any]:
        """Return the immutable snapshot reference without taking any lock."""
        snapshot = self._snapshot
        if snapshot is None:
            detail = f": {self._last_error}" if self._last_error else ""
            raise RuntimeError(f"metrics snapshot is not ready{detail}")
        return snapshot

    def status(self) -> dict[str, float | bool | str | None]:
        with self._lock:
            return {
                "snapshot_ready": self._snapshot is not None,
                "snapshot_age_seconds": (
                    None
                    if self._snapshot is None
                    else max(0.0, time.time() - self._built_at)
                ),
                "snapshot_error": self._last_error,
            }

    def _refresh(self) -> None:
        now = time.time()
        snapshot = build_snapshot(
            self.post_queue,
            self.label_queue,
            self.tag_config,
            window_seconds=self.window_seconds,
            caught_up_lag_seconds=self.caught_up_lag_seconds,
            detector_workers=self.detector_workers,
            publisher_workers=self.publisher_workers,
            now=now,
        )
        snapshot = self.rate_tracker.update(snapshot, now=now)
        with self._lock:
            self._snapshot = snapshot
            self._built_at = now
            self._last_error = None
        self._ready.set()

    def _report_refresh_error(self, error: Exception) -> None:
        message = f"{type(error).__name__}: {error}"
        monotonic_now = time.monotonic()
        with self._lock:
            self._last_error = message
            should_report = (
                message != self._last_reported_error
                or monotonic_now - self._last_error_reported_at >= 30
            )
            if should_report:
                self._last_reported_error = message
                self._last_error_reported_at = monotonic_now
        if should_report:
            print(
                f"[metrics-gateway] snapshot refresh delayed: {message}",
                file=sys.stderr,
                flush=True,
            )

    def _run(self) -> None:
        while not self._stop_requested.is_set():
            started_at = time.monotonic()
            try:
                self._refresh()
            except Exception as error:
                self._report_refresh_error(error)
            elapsed = time.monotonic() - started_at
            self._stop_requested.wait(max(0.0, self.minimum_refresh_seconds - elapsed))

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="metrics-snapshot-provider",
            daemon=True,
        )
        self._thread.start()

    def wait_until_ready(self, timeout: float | None = None) -> bool:
        return self._ready.wait(timeout)

    def stop(self, *, timeout: float = 2.0) -> None:
        self._stop_requested.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)


class LiveRateTracker:
    """Calculate responsive rates from monotonic lifetime-counter deltas."""

    _RATE_TOTALS = {
        "ingestion_per_second": "ingested",
        "label_per_second": "labels_enqueued",
        "publish_per_second": "published",
        "detection_per_second": "detected",
    }

    def __init__(self, window_seconds: float = 10.0):
        if window_seconds <= 0:
            raise ValueError("live rate window must be positive")
        self.window_seconds = window_seconds
        self.samples: deque[tuple[float, dict[str, int]]] = deque()

    def update(self, snapshot: dict[str, Any], *, now: float) -> dict[str, Any]:
        totals = {
            total_name: int(snapshot["totals"][total_name])
            for total_name in self._RATE_TOTALS.values()
        }
        self.samples.append((now, totals))
        cutoff = now - self.window_seconds
        while len(self.samples) > 2 and self.samples[1][0] <= cutoff:
            self.samples.popleft()

        started_at, started_totals = self.samples[0]
        elapsed = now - started_at
        rates = {
            rate_name: (
                0.0
                if elapsed <= 0
                else max(0, totals[total_name] - started_totals[total_name]) / elapsed
            )
            for rate_name, total_name in self._RATE_TOTALS.items()
        }
        snapshot["rates"] = rates
        snapshot["window_seconds"] = self.window_seconds
        return snapshot


class MetricsHTTPServer(ThreadingHTTPServer):
    """Exclusive local server for both dashboard assets and metrics JSON."""

    daemon_threads = True
    # Reuse an address left in TCP TIME_WAIT after a clean shutdown. This does
    # not permit a second server to bind while the first is still listening.
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        dashboard_directory: Path,
        snapshot_provider: SnapshotProvider,
    ):
        self.dashboard_directory = dashboard_directory.resolve()
        self.snapshot_provider = snapshot_provider
        super().__init__(server_address, MetricsRequestHandler)


class MetricsRequestHandler(BaseHTTPRequestHandler):
    server: MetricsHTTPServer

    _STATIC_FILES = {
        "/": ("index.html", "text/html; charset=utf-8"),
        "/index.html": ("index.html", "text/html; charset=utf-8"),
        "/app.js": ("app.js", "text/javascript; charset=utf-8"),
        "/styles.css": ("styles.css", "text/css; charset=utf-8"),
    }

    def _send_body(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._send_body(status, body, "application/json; charset=utf-8")

    def _serve_static(self, path: str) -> bool:
        static_file = self._STATIC_FILES.get(path)
        if static_file is None:
            return False
        filename, content_type = static_file
        try:
            body = (self.server.dashboard_directory / filename).read_bytes()
        except OSError as error:
            self._send_json(500, {"error": f"dashboard asset unavailable: {error}"})
        else:
            self._send_body(200, body, content_type)
        return True

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.partition("?")[0]
        if path == "/health":
            provider_status = self.server.snapshot_provider.status()
            self._send_json(
                200,
                {
                    "status": (
                        "active" if provider_status["snapshot_ready"] else "starting"
                    ),
                    "dashboard_url": f"http://{self.server.server_address[0]}:"
                    f"{self.server.server_address[1]}/",
                    **provider_status,
                },
            )
        elif path == "/metrics":
            try:
                snapshot = self.server.snapshot_provider.get()
            except RuntimeError as error:
                self._send_json(503, {"error": str(error)})
            else:
                self._send_json(200, snapshot)
        elif not self._serve_static(path):
            self._send_json(404, {"error": "not found"})

    def do_HEAD(self) -> None:  # noqa: N802
        path = self.path.partition("?")[0]
        if not self._serve_static(path):
            self._send_json(404, {"error": "not found"})

    def log_message(self, _format: str, *_args: object) -> None:
        return


def run_gateway(args: argparse.Namespace) -> int:
    try:
        loopback_host = ip_address(args.host).is_loopback
    except ValueError:
        loopback_host = args.host.lower() == "localhost"
    if not loopback_host:
        raise ValueError(
            "metrics host must be a loopback address while the dashboard is local-only"
        )

    required_assets = ("index.html", "app.js", "styles.css")
    missing_assets = [
        name for name in required_assets if not (args.dashboard_directory / name).is_file()
    ]
    if missing_assets:
        raise ValueError(
            "dashboard directory is missing required assets: "
            + ", ".join(missing_assets)
        )

    snapshot_provider = SnapshotProvider(
        args.post_database or [Path("data/posts.sqlite3")],
        args.label_database or [Path("data/labels.sqlite3")],
        args.config,
        window_seconds=args.window_seconds,
        detector_workers=args.detector_workers,
        publisher_workers=args.publisher_workers,
        live_rate_window_seconds=args.live_rate_window_seconds,
        caught_up_lag_seconds=args.caught_up_lag_seconds,
    )
    try:
        server = MetricsHTTPServer(
            (args.host, args.port),
            dashboard_directory=args.dashboard_directory,
            snapshot_provider=snapshot_provider,
        )
    except OSError as error:
        raise RuntimeError(
            f"cannot claim metrics HTTP endpoint {args.host}:{args.port}: {error}"
        ) from error

    stop_requested = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop_requested.set()

    stop_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        stop_signals.append(signal.SIGBREAK)
    previous_handlers = {
        signum: signal.signal(signum, request_stop) for signum in stop_signals
    }
    server_thread: threading.Thread | None = None
    try:
        snapshot_provider.start()
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        dashboard_url = f"http://{args.host}:{args.port}/"
        print(
            json.dumps(
                {
                    "metrics_gateway": "local",
                    "dashboard_url": dashboard_url,
                    "metrics_url": f"{dashboard_url}metrics",
                }
            ),
            flush=True,
        )
        while not stop_requested.wait(0.5):
            pass
        return 0
    finally:
        if server_thread is not None:
            server.shutdown()
        server.server_close()
        if server_thread is not None:
            server_thread.join(timeout=2)
        snapshot_provider.stop()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the live metrics dashboard locally")
    parser.add_argument("--post-database", type=Path, action="append")
    parser.add_argument("--label-database", type=Path, action="append")
    parser.add_argument("--config", type=Path, default=Path("config/spoiler_tags.json"))
    parser.add_argument("--dashboard-directory", type=Path, default=Path("docs"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--detector-workers", type=int, default=0)
    parser.add_argument("--publisher-workers", type=int, default=0)
    parser.add_argument("--live-rate-window-seconds", type=float, default=10.0)
    parser.add_argument("--caught-up-lag-seconds", type=float, default=10.0)
    args = parser.parse_args(argv)
    try:
        return run_gateway(args)
    except (RuntimeError, ValueError) as error:
        print(f"[metrics-gateway] {error}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
