"""Serve live metrics and register an outbound Cloudflare Quick Tunnel.

The gateway is coordinator-managed. GitHub is used only as a small endpoint
registry and cross-machine lease; live snapshots travel directly from this
HTTP server to the dashboard.
"""

from __future__ import annotations

import argparse
import base64
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from Bluesky_interface.post_fetcher_interface import PostQueue
from metrics.snapshot import build_snapshot
from spoiler_interface.spoiler_interface import LabelQueue


API_VERSION = "2022-11-28"
TUNNEL_URL_PATTERN = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


class LeaseUnavailable(RuntimeError):
    """Raised when another coordinator owns the public metrics endpoint."""


class GitHubEndpointRegistry:
    """JSON endpoint registry with GitHub SHA-based compare-and-swap writes."""

    def __init__(
        self,
        *,
        repository: str,
        path: str,
        branch: str,
        source_branch: str,
        token: str,
        opener: Callable[..., Any] = urlopen,
    ):
        if repository.count("/") != 1:
            raise ValueError("repository must have OWNER/REPOSITORY form")
        owner, name = repository.split("/", 1)
        repository_url = f"https://api.github.com/repos/{quote(owner)}/{quote(name)}"
        self.contents_url = (
            f"{repository_url}/contents/{quote(path.lstrip('/'), safe='/')}"
        )
        self.refs_url = f"{repository_url}/git/refs"
        self.branch = branch
        self.source_branch = source_branch
        self.token = token
        self.opener = opener

    def _request(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any] | None = None,
    ) -> Request:
        data = None if body is None else json.dumps(body).encode("utf-8")
        return Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "bluesky-spoiler-metrics-gateway",
                "X-GitHub-Api-Version": API_VERSION,
            },
        )

    def _read_json(self, request: Request) -> dict[str, Any]:
        with self.opener(request, timeout=5) as response:
            payload = json.load(response)
        if not isinstance(payload, dict):
            raise ValueError("GitHub response was not a JSON object")
        return payload

    def _ref(self, branch: str) -> dict[str, Any]:
        return self._read_json(
            self._request("GET", f"{self.refs_url}/heads/{quote(branch, safe='')}")
        )

    def ensure_branch(self) -> None:
        try:
            self._ref(self.branch)
            return
        except HTTPError as error:
            if error.code != 404:
                raise

        source = self._ref(self.source_branch)
        source_sha = source.get("object", {}).get("sha")
        if not isinstance(source_sha, str):
            raise ValueError("GitHub source branch response had no commit SHA")
        try:
            self._read_json(
                self._request(
                    "POST",
                    self.refs_url,
                    body={"ref": f"refs/heads/{self.branch}", "sha": source_sha},
                )
            )
        except HTTPError as error:
            if error.code != 422:
                raise
            self._ref(self.branch)

    def read(self) -> tuple[dict[str, Any] | None, str | None]:
        url = f"{self.contents_url}?{urlencode({'ref': self.branch})}"
        try:
            payload = self._read_json(self._request("GET", url))
        except HTTPError as error:
            if error.code == 404:
                return None, None
            raise
        sha = payload.get("sha")
        content = payload.get("content")
        if not isinstance(sha, str) or not isinstance(content, str):
            raise ValueError("GitHub registry response was missing content or SHA")
        record = json.loads(base64.b64decode(content).decode("utf-8"))
        if not isinstance(record, dict):
            raise ValueError("metrics endpoint registry must contain a JSON object")
        return record, sha

    def write(
        self,
        record: dict[str, Any],
        *,
        expected_sha: str | None,
    ) -> str:
        body: dict[str, Any] = {
            "message": "Update live metrics endpoint",
            "content": base64.b64encode(
                (json.dumps(record, indent=2, sort_keys=True) + "\n").encode("utf-8")
            ).decode("ascii"),
            "branch": self.branch,
        }
        if expected_sha is not None:
            body["sha"] = expected_sha
        payload = self._read_json(self._request("PUT", self.contents_url, body=body))
        sha = payload.get("content", {}).get("sha")
        if not isinstance(sha, str):
            raise ValueError("GitHub registry update returned no content SHA")
        return sha

    def acquire(self, instance_id: str, *, force: bool = False) -> str:
        self.ensure_branch()
        current, sha = self.read()
        if current and current.get("state") in {"starting", "active"}:
            owner = current.get("instance_id")
            if owner != instance_id and not force:
                raise LeaseUnavailable(
                    "the live metrics endpoint is already claimed by another coordinator "
                    f"({owner or 'unknown instance'}); use --force-metrics-lease only "
                    "after verifying that instance has stopped"
                )

        record = {
            "state": "starting",
            "instance_id": instance_id,
            "public_url": None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            return self.write(record, expected_sha=sha)
        except HTTPError as error:
            if error.code in {409, 422}:
                raise LeaseUnavailable(
                    "another coordinator acquired the live metrics endpoint concurrently"
                ) from error
            raise

    def activate(self, instance_id: str, public_url: str, *, sha: str) -> str:
        return self.write(
            {
                "state": "active",
                "instance_id": instance_id,
                "public_url": public_url.rstrip("/"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            expected_sha=sha,
        )

    def release(self, instance_id: str) -> None:
        current, sha = self.read()
        if current is None or sha is None or current.get("instance_id") != instance_id:
            return
        self.write(
            {
                "state": "offline",
                "instance_id": None,
                "public_url": None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            expected_sha=sha,
        )


class SnapshotProvider:
    def __init__(
        self,
        post_database: Path,
        label_database: Path,
        tag_config: Path,
        *,
        window_seconds: int,
        live_rate_window_seconds: float = 10.0,
        caught_up_lag_seconds: float = 10.0,
        minimum_refresh_seconds: float = 0.5,
    ):
        self.post_queue = PostQueue(post_database, migrate=False)
        self.label_queue = LabelQueue(label_database, migrate=False)
        self.tag_config = tag_config
        self.window_seconds = window_seconds
        self.caught_up_lag_seconds = caught_up_lag_seconds
        self.minimum_refresh_seconds = minimum_refresh_seconds
        self.rate_tracker = LiveRateTracker(live_rate_window_seconds)
        self._lock = threading.Lock()
        self._snapshot: dict[str, Any] | None = None
        self._built_at = 0.0

    def get(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            if self._snapshot is None or now - self._built_at >= self.minimum_refresh_seconds:
                snapshot = build_snapshot(
                    self.post_queue,
                    self.label_queue,
                    self.tag_config,
                    window_seconds=self.window_seconds,
                    caught_up_lag_seconds=self.caught_up_lag_seconds,
                    now=now,
                )
                self._snapshot = self.rate_tracker.update(snapshot, now=now)
                self._built_at = now
            return self._snapshot


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
    daemon_threads = True
    allow_reuse_address = False

    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        instance_id: str,
        allowed_origin: str,
        snapshot_provider: SnapshotProvider,
    ):
        self.instance_id = instance_id
        self.allowed_origin = allowed_origin.rstrip("/")
        self.snapshot_provider = snapshot_provider
        self.gateway_state = "starting"
        super().__init__(server_address, MetricsRequestHandler)


class MetricsRequestHandler(BaseHTTPRequestHandler):
    server: MetricsHTTPServer

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        origin = self.headers.get("Origin")
        if origin == self.server.allowed_origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.partition("?")[0]
        if path == "/health":
            self._send_json(
                200,
                {
                    "status": self.server.gateway_state,
                    "instance_id": self.server.instance_id,
                },
            )
        elif path == "/metrics":
            self._send_json(200, self.server.snapshot_provider.get())
        else:
            self._send_json(404, {"error": "not found"})

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        origin = self.headers.get("Origin")
        if origin == self.server.allowed_origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Accept")
            self.send_header("Vary", "Origin")
        self.end_headers()

    def log_message(self, _format: str, *_args: object) -> None:
        return


class QuickTunnel:
    def __init__(self, executable: str, local_url: str):
        self.executable = executable
        self.local_url = local_url
        self.process: subprocess.Popen[str] | None = None
        self._output_thread: threading.Thread | None = None

    def start(self, *, timeout_seconds: float = 30.0) -> str:
        executable = shutil.which(self.executable)
        if executable is None:
            raise RuntimeError(
                f"{self.executable!r} was not found; install cloudflared before "
                "starting the coordinator"
            )
        self.process = subprocess.Popen(
            [executable, "tunnel", "--url", self.local_url],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert self.process.stdout is not None
        urls: queue.Queue[str] = queue.Queue()

        def read_output() -> None:
            assert self.process is not None and self.process.stdout is not None
            for line in self.process.stdout:
                print(f"[metrics-tunnel] {line.rstrip()}", flush=True)
                match = TUNNEL_URL_PATTERN.search(line)
                if match:
                    urls.put(match.group(0))

        self._output_thread = threading.Thread(target=read_output, daemon=True)
        self._output_thread.start()
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self.process.poll() is not None and urls.empty():
                raise RuntimeError(
                    f"cloudflared exited before creating a tunnel (status {self.process.returncode})"
                )
            try:
                return urls.get(timeout=0.25)
            except queue.Empty:
                continue
        raise RuntimeError("timed out waiting for cloudflared to create a public URL")

    def poll(self) -> int | None:
        return None if self.process is None else self.process.poll()

    def stop(self) -> None:
        if self.process is None or self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


def run_gateway(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        raise RuntimeError(f"environment variable {args.token_env} is not set")

    instance_id = uuid4().hex
    snapshot_provider = SnapshotProvider(
        args.post_database,
        args.label_database,
        args.config,
        window_seconds=args.window_seconds,
        live_rate_window_seconds=args.live_rate_window_seconds,
        caught_up_lag_seconds=args.caught_up_lag_seconds,
    )
    try:
        server = MetricsHTTPServer(
            (args.host, args.port),
            instance_id=instance_id,
            allowed_origin=args.allowed_origin,
            snapshot_provider=snapshot_provider,
        )
    except OSError as error:
        raise RuntimeError(
            f"cannot claim metrics HTTP endpoint {args.host}:{args.port}: {error}"
        ) from error

    registry = GitHubEndpointRegistry(
        repository=args.repository,
        path=args.registry_path,
        branch=args.registry_branch,
        source_branch=args.registry_source_branch,
        token=token,
    )
    tunnel = QuickTunnel(args.cloudflared, f"http://{args.host}:{args.port}")
    stop_requested = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop_requested.set()

    previous_handlers = {
        signum: signal.signal(signum, request_stop)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    server_thread: threading.Thread | None = None
    lease_acquired = False
    try:
        lease_sha = registry.acquire(instance_id, force=args.force_lease)
        lease_acquired = True
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        public_url = tunnel.start(timeout_seconds=args.tunnel_timeout)
        registry.activate(instance_id, public_url, sha=lease_sha)
        server.gateway_state = "active"
        print(
            json.dumps(
                {
                    "metrics_gateway": "active",
                    "public_url": public_url,
                    "instance_id": instance_id,
                }
            ),
            flush=True,
        )
        while not stop_requested.wait(0.5):
            return_code = tunnel.poll()
            if return_code is not None:
                raise RuntimeError(f"cloudflared exited with status {return_code}")
        return 0
    finally:
        tunnel.stop()
        if server_thread is not None:
            server.shutdown()
        server.server_close()
        if server_thread is not None:
            server_thread.join(timeout=2)
        if lease_acquired:
            try:
                registry.release(instance_id)
            except Exception as error:
                print(
                    f"[metrics-gateway] failed to release endpoint lease: {error}",
                    file=sys.stderr,
                    flush=True,
                )
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Expose live metrics through an outbound tunnel")
    parser.add_argument("--post-database", type=Path, default=Path("data/posts.sqlite3"))
    parser.add_argument("--label-database", type=Path, default=Path("data/labels.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config/spoiler_tags.json"))
    parser.add_argument("--repository", required=True)
    parser.add_argument("--registry-path", default="docs/metrics-endpoint.json")
    parser.add_argument("--registry-branch", default="live-metrics-endpoint-registry")
    parser.add_argument("--registry-source-branch", default="main")
    parser.add_argument("--token-env", default="GITHUB_METRICS_TOKEN")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--allowed-origin", required=True)
    parser.add_argument("--cloudflared", default="cloudflared")
    parser.add_argument("--tunnel-timeout", type=float, default=30.0)
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--live-rate-window-seconds", type=float, default=10.0)
    parser.add_argument("--caught-up-lag-seconds", type=float, default=10.0)
    parser.add_argument("--force-lease", action="store_true")
    args = parser.parse_args(argv)
    try:
        return run_gateway(args)
    except (LeaseUnavailable, RuntimeError, ValueError) as error:
        print(f"[metrics-gateway] {error}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
