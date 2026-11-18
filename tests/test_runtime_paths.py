"""Coverage for runtime entry points, recovery paths, and operator errors."""

from __future__ import annotations

import argparse
import asyncio
from io import BytesIO
import json
from pathlib import Path
import signal
import subprocess
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

import coordinator
from Bluesky_interface.post_fetcher_core import post_fetcher
from Bluesky_interface.post_fetcher_interface import PostQueue
from label_publisher import publisher
from metrics import gateway, snapshot
from spoiler_interface.spoiler_core import spoiler_detector
from spoiler_interface.spoiler_interface import (
    LabelJob,
    LabelQueue,
    PostIdentifier,
    SpoilerItem,
)


def _event(sequence: int = 1) -> dict:
    return {
        "kind": "commit",
        "seq": sequence,
        "did": "did:plc:runtime-test",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": str(sequence),
            "cid": f"cid-{sequence}",
            "record": {"text": "runtime spoiler test"},
        },
    }


@pytest.mark.parametrize(
    ("event", "error"),
    [
        ({}, "cursor"),
        ({"seq": 1, "kind": "commit", "commit": {"operation": "create", "collection": "app.bsky.feed.post"}}, "did"),
        ({"seq": 1, "kind": "commit", "did": "did:test", "commit": {"operation": "create", "collection": "app.bsky.feed.post", "rkey": "a", "cid": "c", "record": {}}}, "record text"),
    ],
)
def test_jetstream_parser_rejects_malformed_creates(event: dict, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        post_fetcher.parse_jetstream_event(event, "runtime")


@pytest.mark.parametrize(
    "event",
    [
        {"seq": 1, "kind": "identity"},
        {"seq": 1, "kind": "commit", "commit": None},
        {"seq": 1, "kind": "commit", "commit": {"operation": "update"}},
    ],
)
def test_jetstream_parser_ignores_non_create_events(event: dict) -> None:
    assert post_fetcher.parse_jetstream_event(event, "runtime") == (1, [])


def test_jetstream_parser_supports_time_cursor_and_action_alias() -> None:
    event = _event()
    event.pop("seq")
    event["time_us"] = 42
    event["commit"]["action"] = event["commit"].pop("operation")
    sequence, jobs = post_fetcher.parse_jetstream_event(event, "runtime")
    assert sequence == 42
    assert jobs[0].post_uri.endswith("/1")


def test_json_line_reader_reports_line_specific_errors() -> None:
    assert list(post_fetcher.json_line_events(["\n", json.dumps(_event())])) == [
        _event()
    ]
    with pytest.raises(ValueError, match="line 2"):
        list(post_fetcher.json_line_events(["\n", "not-json"]))
    with pytest.raises(ValueError, match="not an object"):
        list(post_fetcher.json_line_events(["[]"]))


def test_reader_run_accumulates_accepted_posts(tmp_path: Path) -> None:
    queue = PostQueue(tmp_path / "posts.sqlite3")
    reader = post_fetcher.PostReader(queue, "runtime")
    assert reader.run([_event(1), {"seq": 2, "kind": "identity"}]) == 1
    assert queue.get_cursor("runtime") == 2


def test_jetstream_url_and_receive_validate_inputs() -> None:
    with pytest.raises(ValueError, match="ws"):
        post_fetcher.build_jetstream_url("https://example.test/subscribe")
    with pytest.raises(ValueError, match="positive"):
        asyncio.run(post_fetcher.receive_jetstream_message(SimpleNamespace(), 0))


def test_live_reader_rotates_endpoints_and_records_invalid_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingQueue:
        def __init__(self) -> None:
            self.cursor = None
            self.connection_states: list[tuple[bool, bool]] = []
            self.invalid = 0

        def get_cursor(self, _stream_id: str) -> int | None:
            return self.cursor

        def set_stream_connected(
            self, _stream_id: str, connected: bool, *, reconnect: bool = False
        ) -> None:
            self.connection_states.append((connected, reconnect))

        def record_invalid_event(self, _stream_id: str) -> None:
            self.invalid += 1

        def enqueue_and_checkpoint(
            self,
            jobs: list,
            *,
            stream_id: str,
            sequence: int,
            source_event_at: float | None = None,
        ) -> int:
            del stream_id, source_event_at
            self.cursor = sequence
            return len(jobs)

    class Socket:
        def __init__(self) -> None:
            self.messages = [json.dumps(_event()).encode(), "[]"]

        async def recv(self) -> str | bytes:
            return self.messages.pop(0)

    class Connection:
        def __init__(self, socket: Socket | None = None, error: Exception | None = None):
            self.socket = socket
            self.error = error

        async def __aenter__(self):
            if self.error is not None:
                raise self.error
            return self.socket

        async def __aexit__(self, *_args: object) -> None:
            return None

    attempts = 0

    def connect(_url: str, **_kwargs: object) -> Connection:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return Connection(error=OSError("first endpoint unavailable"))
        return Connection(socket=Socket())

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("websockets.asyncio.client.connect", connect)
    monkeypatch.setattr(post_fetcher.asyncio, "sleep", no_sleep)
    queue = RecordingQueue()
    result = asyncio.run(
        post_fetcher.run_live_reader(
            post_fetcher.PostReader(queue, "runtime"),  # type: ignore[arg-type]
            ["wss://one.test/subscribe", "wss://two.test/subscribe"],
            max_events=2,
            initial_backoff=0.01,
        )
    )

    assert result == {"received": 2, "accepted": 1, "invalid": 1}
    assert queue.invalid == 1
    assert attempts == 2
    assert queue.connection_states == [(False, False), (True, False), (False, False)]


def test_live_reader_rejects_empty_endpoint_list() -> None:
    reader = post_fetcher.PostReader(SimpleNamespace(), "runtime")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at least one"):
        asyncio.run(post_fetcher.run_live_reader(reader, []))


def test_post_fetcher_main_supports_replay_and_live_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    replay = tmp_path / "events.jsonl"
    replay.write_text(json.dumps(_event()) + "\n", encoding="utf-8")
    database = tmp_path / "posts.sqlite3"

    assert post_fetcher.main(
        ["--database", str(database), "--input", str(replay), "--stream-id", "replay"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["accepted_posts"] == 1

    async def fake_live(*_args: object, **_kwargs: object) -> dict[str, int]:
        return {"received": 0, "accepted": 0, "invalid": 0}

    monkeypatch.setattr(post_fetcher, "run_live_reader", fake_live)
    assert post_fetcher.main(
        ["--database", str(tmp_path / "live.sqlite3"), "--endpoint", "wss://x.test"]
    ) == 0
    assert json.loads(capsys.readouterr().out)["received"] == 0


def _pipeline_config(tmp_path: Path) -> dict:
    return {
        "databases": {
            "posts": str(tmp_path / "posts.sqlite3"),
            "labels": str(tmp_path / "labels.sqlite3"),
            "post_shards": 1,
            "label_shards": 1,
        },
        "reader": {"enabled": False},
        "detectors": {"workers": 0},
        "publishers": {"workers": 0},
        "metrics": {"enabled": False},
        "supervision": {"poll_seconds": 0, "restart_delay_seconds": 0},
    }


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda config: config.clear(), "missing"),
        (lambda config: config["detectors"].update(workers=-1), "workers"),
        (lambda config: config["databases"].update(post_shards=0), "post_shards"),
        (lambda config: config["databases"].update(label_shards=0), "label_shards"),
        (lambda config: config["detectors"].update(workers=1) or config["databases"].update(post_shards=2), "detector"),
        (lambda config: config["publishers"].update(workers=1) or config["databases"].update(label_shards=2), "publisher"),
    ],
)
def test_config_validation_rejects_invalid_capacity(
    tmp_path: Path, mutate, message: str
) -> None:
    config = _pipeline_config(tmp_path)
    mutate(config)
    path = tmp_path / "pipeline.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        coordinator.load_config(path)


def test_config_validation_requires_json_object(tmp_path: Path) -> None:
    path = tmp_path / "pipeline.json"
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON object"):
        coordinator.load_config(path)


def test_component_specs_cover_disabled_and_legacy_endpoint_branches(tmp_path: Path) -> None:
    config = _pipeline_config(tmp_path)
    config["detectors"]["workers"] = 1
    config["publishers"]["workers"] = 1
    config["publishers"]["dry_run"] = False
    config["reader"] = {"enabled": True, "endpoint": "wss://legacy.test"}
    specs = coordinator.build_component_specs(config, python_executable="python-test")

    assert not any(spec.name == "metrics" for spec in specs)
    assert "--dry-run" not in next(
        spec.command for spec in specs if spec.name.startswith("publisher")
    )
    reader = next(spec for spec in specs if spec.is_reader)
    assert "wss://legacy.test" in reader.command

    config["reader"] = {"enabled": True, "endpoints": "not-a-list"}
    with pytest.raises(ValueError, match="non-empty array"):
        coordinator.build_component_specs(config)


class _MetricsQueue:
    def __init__(self, available: int = 0, leased: int = 0):
        self.available = available
        self.leased = leased

    def metrics(self) -> dict[str, int]:
        return {"available": self.available, "leased": self.leased}


class _Process:
    def __init__(self, return_code: int | None):
        self.return_code = return_code
        self.pid = 123
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.return_code

    def terminate(self) -> None:
        self.terminated = True

    def send_signal(self, _signum: int) -> None:
        self.terminated = True

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return self.return_code or 0

    def kill(self) -> None:
        self.killed = True
        self.return_code = -9


def _coordinator(specs: list[coordinator.ComponentSpec]) -> coordinator.Coordinator:
    return coordinator.Coordinator(
        specs,
        post_queue=_MetricsQueue(),  # type: ignore[arg-type]
        label_queue=_MetricsQueue(),  # type: ignore[arg-type]
        settings=coordinator.SupervisionSettings(
            poll_seconds=0, restart_delay_seconds=0, max_restarts=0
        ),
    )


@pytest.mark.parametrize(
    ("spec", "return_code", "expected"),
    [
        (coordinator.ComponentSpec("critical", ("x",), critical=True), 2, 2),
        (coordinator.ComponentSpec("reader", ("x",), is_reader=True), 3, 3),
        (coordinator.ComponentSpec("worker", ("x",)), 4, 4),
        (coordinator.ComponentSpec("reader", ("x",), is_reader=True), 0, 0),
    ],
)
def test_coordinator_handles_component_exit_modes(
    monkeypatch: pytest.MonkeyPatch,
    spec: coordinator.ComponentSpec,
    return_code: int,
    expected: int,
) -> None:
    instance = _coordinator([spec])

    def start(started_spec: coordinator.ComponentSpec) -> None:
        instance.processes[started_spec.name] = _Process(return_code)  # type: ignore[assignment]

    monkeypatch.setattr(instance, "_start", start)
    assert instance.run() == expected


def test_coordinator_readiness_and_shutdown_failure_paths(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec = coordinator.ComponentSpec(
        "metrics", ("x",), readiness_url="http://test/health", startup_timeout_seconds=1
    )
    instance = _coordinator([spec])
    instance.processes[spec.name] = _Process(9)  # type: ignore[assignment]
    assert not instance._wait_until_ready(spec)  # pylint: disable=protected-access
    assert "failed during startup" in capsys.readouterr().err

    class HungProcess(_Process):
        def wait(self, timeout: float | None = None) -> int:
            if not self.killed and timeout is not None:
                raise subprocess.TimeoutExpired("worker", timeout)
            return super().wait(timeout)

    hung = HungProcess(None)
    instance.processes["hung"] = hung  # type: ignore[assignment]
    instance.shutdown()
    assert hung.terminated and hung.killed

    monkeypatch.setattr(coordinator.subprocess, "Popen", lambda *_a, **_k: _Process(None))
    instance._start(coordinator.ComponentSpec("started", ("x",)))  # pylint: disable=protected-access
    assert "started" in instance.processes


def test_coordinator_main_rectifies_and_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _pipeline_config(tmp_path)
    path = tmp_path / "pipeline.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setattr(coordinator.Coordinator, "run", lambda _self: 7)
    assert coordinator.main(["--config", str(path)]) == 7
    assert (tmp_path / "posts.sqlite3.shards.json").is_file()
    assert (tmp_path / "labels.sqlite3.shards.json").is_file()


def test_publisher_retries_client_and_lease_failures(tmp_path: Path) -> None:
    labels = LabelQueue(tmp_path / "labels.sqlite3")
    first = LabelJob.create(
        post_uri="at://did:test/app.bsky.feed.post/1",
        post_cid="cid-1",
        label_value="spoiler",
        detector_version="test",
    )
    labels.enqueue(first)

    class FailingClient:
        @staticmethod
        def publish(_job: LabelJob) -> None:
            raise RuntimeError("publisher unavailable")

    result = publisher.process_once(
        labels, FailingClient(), "publisher", retry_delay_seconds=0
    )
    assert result == {"claimed": 1, "published": 0, "failed": 1}
    assert labels.metrics()["retried_total"] == 1

    class LostLeaseQueue:
        @staticmethod
        def claim(*_args: object, **_kwargs: object):
            return labels.claim("publisher", limit=1)

        @staticmethod
        def acknowledge(*_args: object, **_kwargs: object) -> bool:
            return False

        @staticmethod
        def retry(*_args: object, **_kwargs: object) -> bool:
            return True

    class Client:
        @staticmethod
        def publish(_job: LabelJob) -> None:
            return None

    assert publisher.process_once(LostLeaseQueue(), Client(), "publisher")["failed"] == 1  # type: ignore[arg-type]


def test_publisher_main_once_and_requires_dry_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "labels.sqlite3"
    with pytest.raises(SystemExit):
        publisher.main(["--database", str(database), "--once"])
    assert publisher.main(["--database", str(database), "--dry-run", "--once"]) == 0
    assert json.loads(capsys.readouterr().out)["claimed"] == 0


def test_detector_config_processing_and_main(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="tags array"):
        spoiler_detector.load_detector(invalid)

    config = tmp_path / "tags.json"
    config.write_text(
        json.dumps({"tags": [" finale ", "finale", ""], "label_value": "cw"}),
        encoding="utf-8",
    )
    detector = spoiler_detector.load_detector(config)
    assert detector.tags == ("finale",)
    assert detector.label_value == "cw"
    assert spoiler_detector.main(
        [
            "--post-database",
            str(tmp_path / "posts.sqlite3"),
            "--label-database",
            str(tmp_path / "labels.sqlite3"),
            "--config",
            str(config),
            "--once",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["claimed"] == 0


def test_detector_process_once_submits_classification_errors() -> None:
    identifier = PostIdentifier("job", "at://post", "cid")
    item = SpoilerItem(identifier, "text")

    class Interface:
        errors: list[tuple[PostIdentifier, BaseException]] = []

        @staticmethod
        def get_items(*, limit: int) -> list[SpoilerItem]:
            assert limit == 3
            return [item]

        @staticmethod
        def submit_result(_identifier: PostIdentifier, _result: bool) -> None:
            return None

        @classmethod
        def submit_error(cls, submitted: PostIdentifier, error: BaseException) -> None:
            cls.errors.append((submitted, error))

    class BrokenDetector:
        @staticmethod
        def detect(_text: str):
            raise RuntimeError("classification failed")

    result = spoiler_detector.process_once(Interface(), BrokenDetector(), batch_size=3)  # type: ignore[arg-type]
    assert result == {"claimed": 1, "positive": 0, "negative": 0, "failed": 1}
    assert Interface.errors[0][0] == identifier


def test_snapshot_config_write_and_main(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    invalid = tmp_path / "invalid-tags.json"
    invalid.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="tags array"):
        snapshot.load_tag_config(invalid)

    tags = tmp_path / "tags.json"
    tags.write_text('{"tags":["finale"]}', encoding="utf-8")
    posts = tmp_path / "posts.sqlite3"
    labels = tmp_path / "labels.sqlite3"
    PostQueue(posts)
    LabelQueue(labels)
    output = tmp_path / "nested" / "metrics.json"
    assert snapshot.main(
        [
            "--post-database",
            str(posts),
            "--label-database",
            str(labels),
            "--config",
            str(tags),
            "--output",
            str(output),
        ]
    ) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["configuration"]["tags"] == [
        "finale"
    ]
    assert json.loads(capsys.readouterr().out)["output"] == str(output)


def test_snapshot_provider_and_rate_tracker_error_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    provider = gateway.SnapshotProvider(
        [tmp_path / "posts.sqlite3"],
        [tmp_path / "labels.sqlite3"],
        tmp_path / "tags.json",
        window_seconds=60,
    )
    with pytest.raises(RuntimeError, match="not ready"):
        provider.get()
    provider._report_refresh_error(ValueError("bad snapshot"))  # pylint: disable=protected-access
    provider._report_refresh_error(ValueError("bad snapshot"))  # pylint: disable=protected-access
    with pytest.raises(RuntimeError, match="ValueError"):
        provider.get()
    assert capsys.readouterr().err.count("refresh delayed") == 1
    provider.stop()

    with pytest.raises(ValueError, match="positive"):
        gateway.LiveRateTracker(0)
    tracker = gateway.LiveRateTracker(1)
    base = {"totals": {"ingested": 10, "labels_enqueued": 10, "published": 10, "detected": 10}}
    tracker.update({**base, "rates": {}}, now=1)
    tracker.update({**base, "rates": {}}, now=2)
    latest = gateway.LiveRateTracker(1).update({**base, "rates": {}}, now=2)
    assert latest["rates"]["ingestion_per_second"] == 0


def test_metrics_http_error_head_and_not_found_paths(tmp_path: Path) -> None:
    dashboard = tmp_path / "docs"
    dashboard.mkdir()
    for name in ("index.html", "app.js", "styles.css"):
        (dashboard / name).write_text(name, encoding="utf-8")

    class NotReady:
        @staticmethod
        def status() -> dict:
            return {"snapshot_ready": False, "snapshot_age_seconds": None, "snapshot_error": None}

        @staticmethod
        def get() -> dict:
            raise RuntimeError("warming up")

    server = gateway.MetricsHTTPServer(
        ("127.0.0.1", 0),
        dashboard_directory=dashboard,
        snapshot_provider=NotReady(),  # type: ignore[arg-type]
    )
    thread = gateway.threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with urlopen(f"{origin}/health", timeout=1) as response:
            assert json.load(response)["status"] == "starting"
        with pytest.raises(HTTPError) as missing:
            urlopen(f"{origin}/missing", timeout=1)
        assert missing.value.code == 404
        with pytest.raises(HTTPError) as unavailable:
            urlopen(f"{origin}/metrics", timeout=1)
        assert unavailable.value.code == 503
        request = Request(f"{origin}/app.js", method="HEAD")
        with urlopen(request, timeout=1) as response:
            assert response.read() == b""
        (dashboard / "app.js").unlink()
        with pytest.raises(HTTPError) as asset_error:
            urlopen(f"{origin}/app.js", timeout=1)
        assert asset_error.value.code == 500
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def test_gateway_validation_success_cleanup_and_main_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    dashboard = tmp_path / "docs"
    dashboard.mkdir()
    for name in ("index.html", "app.js", "styles.css"):
        (dashboard / name).write_text(name, encoding="utf-8")

    args = argparse.Namespace(
        host="localhost",
        port=1234,
        dashboard_directory=dashboard,
        post_database=[],
        label_database=[],
        config=tmp_path / "tags.json",
        window_seconds=60,
        detector_workers=1,
        publisher_workers=1,
        live_rate_window_seconds=10,
        caught_up_lag_seconds=10,
    )

    class Provider:
        def __init__(self, *_args: object, **_kwargs: object):
            self.started = False
            self.stopped = False

        def start(self) -> None:
            self.started = True

        def stop(self) -> None:
            self.stopped = True

    class Server:
        server_address = ("localhost", 1234)

        def __init__(self, *_args: object, **_kwargs: object):
            self.closed = False

        @staticmethod
        def serve_forever() -> None:
            return None

        @staticmethod
        def shutdown() -> None:
            return None

        def server_close(self) -> None:
            self.closed = True

    monkeypatch.setattr(gateway, "SnapshotProvider", Provider)
    monkeypatch.setattr(gateway, "MetricsHTTPServer", Server)

    def install_signal(signum: int, handler):
        if callable(handler):
            handler(signum, None)
        return signal.SIG_DFL

    monkeypatch.setattr(gateway.signal, "signal", install_signal)
    assert gateway.run_gateway(args) == 0

    empty = tmp_path / "empty"
    empty.mkdir()
    args.dashboard_directory = empty
    with pytest.raises(ValueError, match="missing required assets"):
        gateway.run_gateway(args)

    assert gateway.main(["--host", "not-a-host", "--dashboard-directory", str(dashboard)]) == 2
    assert "loopback" in capsys.readouterr().err
