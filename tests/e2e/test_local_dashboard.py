"""Browser-level coverage for the complete local pipeline and dashboard."""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from typing import TextIO
from urllib.error import URLError
from urllib.request import urlopen

from playwright.sync_api import Page, expect
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason="the streaming replay fixture uses a POSIX named pipe",
)


def _unused_loopback_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def _wait_for_dashboard(process: subprocess.Popen[str], origin: str) -> None:
    deadline = time.monotonic() + 15
    last_error = "dashboard did not respond"
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            output = process.stdout.read() if process.stdout is not None else ""
            pytest.fail(
                f"coordinator exited with status {return_code} during startup:\n{output}"
            )
        try:
            with urlopen(f"{origin}/health", timeout=0.5) as response:
                if json.load(response).get("status") == "active":
                    return
        except (OSError, ValueError, URLError) as error:
            last_error = str(error)
        time.sleep(0.1)
    pytest.fail(f"timed out waiting for the local dashboard: {last_error}")


def _open_replay_writer(process: subprocess.Popen[str], replay: Path) -> TextIO:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout is not None else ""
            pytest.fail(f"coordinator stopped before opening replay input:\n{output}")
        try:
            descriptor = os.open(replay, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            time.sleep(0.1)
        else:
            return os.fdopen(descriptor, "w", encoding="utf-8")
    pytest.fail("timed out waiting for the reader to open its replay input")


def _stop_coordinator(process: subprocess.Popen[str], writer: TextIO | None) -> str:
    if writer is not None and not writer.closed:
        writer.close()
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        output, _ = process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        output, _ = process.communicate(timeout=5)
    return output


@pytest.mark.e2e
def test_coordinator_renders_processed_event_in_local_dashboard(
    page: Page,
    tmp_path: Path,
) -> None:
    port = _unused_loopback_port()
    origin = f"http://127.0.0.1:{port}"
    replay = tmp_path / "events.pipe"
    os.mkfifo(replay)
    config = {
        "databases": {
            "posts": str(tmp_path / "posts.sqlite3"),
            "labels": str(tmp_path / "labels.sqlite3"),
            "post_shards": 2,
            "label_shards": 2,
        },
        "detectors": {
            "batch_size": 10,
            "poll_seconds": 0.05,
            "tag_config": str(PROJECT_ROOT / "config/spoiler_tags.json"),
            "write_buffer_size": 10,
            "workers": 2,
        },
        "metrics": {
            "caught_up_lag_seconds": 10,
            "dashboard_directory": str(PROJECT_ROOT / "docs"),
            "enabled": True,
            "host": "127.0.0.1",
            "live_rate_window_seconds": 10,
            "open_browser": False,
            "port": port,
            "startup_timeout_seconds": 10,
            "tag_config": str(PROJECT_ROOT / "config/spoiler_tags.json"),
            "window_seconds": 60,
        },
        "publishers": {
            "batch_size": 10,
            "dry_run": True,
            "poll_seconds": 0.05,
            "workers": 2,
        },
        "reader": {
            "enabled": True,
            "input": None,
            "stream_id": "e2e-replay",
        },
        "supervision": {
            "max_restarts": 1,
            "poll_seconds": 0.05,
            "restart_delay_seconds": 0.05,
            "shutdown_timeout_seconds": 5,
        },
    }
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    process = subprocess.Popen(
        [
            sys.executable,
            "coordinator.py",
            "--config",
            str(config_path),
            "--input",
            str(replay),
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    writer: TextIO | None = None
    try:
        _wait_for_dashboard(process, origin)
        writer = _open_replay_writer(process, replay)
        event = {
            "kind": "commit",
            "seq": 1,
            "time_us": time.time_ns() // 1_000,
            "did": "did:plc:e2e",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "dashboard-test",
                "cid": "bafy-e2e",
                "record": {
                    "text": "A finale spoiler used by the E2E test",
                    "createdAt": "2026-11-18T00:00:00Z",
                },
            },
        }
        writer.write(json.dumps(event) + "\n")
        writer.flush()

        requested_urls: list[str] = []
        page.on("request", lambda request: requested_urls.append(request.url))
        response = page.goto(origin, wait_until="domcontentloaded")

        assert response is not None and response.ok
        expect(page).to_have_title("Pipeline Performance")
        expect(page.get_by_role("heading", name="Pipeline performance")).to_be_visible()
        expect(page.locator(".environment")).to_have_text("Local")
        expect(page.locator("#events-received")).to_have_text("1", timeout=15_000)
        expect(
            page.locator("#totals div").filter(has_text="Ingested").locator("dd")
        ).to_have_text("1", timeout=15_000)
        expect(
            page.locator("#totals div").filter(has_text="Detected").locator("dd")
        ).to_have_text("1", timeout=15_000)
        expect(
            page.locator("#totals div").filter(has_text="Published").locator("dd")
        ).to_have_text("1", timeout=15_000)
        expect(page.locator("#label-value")).to_have_text("spoiler")
        expect(page.locator("#detector-workers")).to_contain_text("/ 2 busy")
        expect(page.locator("#detector-shards")).to_have_text("2")
        expect(page.locator("#publisher-workers")).to_contain_text("/ 2 busy")
        expect(page.locator("#publisher-shards")).to_have_text("2")
        expect(page.locator("#detector-queue-wait")).not_to_have_text(
            "No recent claims"
        )
        expect(page.locator("#detector-db-wait")).not_to_have_text(
            "No recent claims"
        )
        shard_manifest = json.loads(
            (tmp_path / "posts.sqlite3.shards.json").read_text(encoding="utf-8")
        )
        assert shard_manifest["shard_count"] == 2
        assert len(shard_manifest["paths"]) == 2
        assert not (tmp_path / "posts.sqlite3").exists()
        label_manifest = json.loads(
            (tmp_path / "labels.sqlite3.shards.json").read_text(encoding="utf-8")
        )
        assert label_manifest["shard_count"] == 2
        assert len(label_manifest["paths"]) == 2
        assert not (tmp_path / "labels.sqlite3").exists()
        assert requested_urls
        assert all(url.startswith(origin) for url in requested_urls)
    finally:
        output = _stop_coordinator(process, writer)
        if process.returncode not in {0, -signal.SIGTERM}:
            pytest.fail(
                f"coordinator exited with unexpected status {process.returncode}:\n{output}"
            )
