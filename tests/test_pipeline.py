from __future__ import annotations

import base64
import json
from io import BytesIO
from pathlib import Path
import sqlite3
import threading

import pytest

from Bluesky_interface.post_fetcher_core.post_fetcher import (
    PostReader,
    build_jetstream_url,
    parse_jetstream_event,
)
from Bluesky_interface.post_fetcher_interface import PostJob, PostQueue
from coordinator import build_component_specs, load_config, load_supervision_settings
from label_publisher.publisher import DryRunLabelClient, process_once as publish_once
from metrics.gateway import (
    GitHubEndpointRegistry,
    LeaseUnavailable,
    LiveRateTracker,
    MetricsHTTPServer,
    load_or_create_instance_id,
)
from metrics.snapshot import build_snapshot
from spoiler_interface.spoiler_core.spoiler_detector import (
    KeywordSpoilerDetector,
    isSpoiler,
    process_once as detect_once,
)
from spoiler_interface.spoiler_interface import (
    LabelJob,
    LabelQueue,
    SpoilerWorkerInterface,
)


def post_job(sequence: int = 1, text: str = "This contains a spoiler") -> PostJob:
    return PostJob.create(
        stream_id="test-stream",
        sequence=sequence,
        operation_index=0,
        post_uri=f"at://did:plc:test/app.bsky.feed.post/{sequence}",
        post_cid=f"cid-{sequence}",
        author_did="did:plc:test",
        text=text,
        received_at=100.0,
    )


def test_post_queue_checkpoints_and_deduplicates(tmp_path: Path) -> None:
    queue = PostQueue(tmp_path / "posts.sqlite3")
    job = post_job()

    assert queue.enqueue_and_checkpoint([job], stream_id="test-stream", sequence=1, now=100.0) == 1
    assert queue.enqueue_and_checkpoint([job], stream_id="test-stream", sequence=1, now=101.0) == 0
    assert queue.get_cursor("test-stream") == 1

    first = queue.claim("worker-a", now=102.0, lease_seconds=5)
    assert [delivery.job for delivery in first] == [job]
    assert queue.claim("worker-b", now=103.0) == []

    redelivered = queue.claim("worker-b", now=108.0)
    assert redelivered[0].job.job_id == job.job_id
    assert redelivered[0].attempt_count == 2
    assert queue.acknowledge(job.job_id, "worker-b", outcome="positive", now=109.0)
    assert queue.metrics(now=110.0)["available"] == 0
    assert queue.enqueue_and_checkpoint([job], stream_id="test-stream", sequence=1, now=111.0) == 0
    assert queue.metrics(now=112.0)["available"] == 0


def test_post_queue_migrates_existing_stream_health_for_source_lag(tmp_path: Path) -> None:
    database = tmp_path / "posts.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE stream_health (
                stream_id TEXT PRIMARY KEY,
                connected INTEGER NOT NULL DEFAULT 0,
                reconnect_count INTEGER NOT NULL DEFAULT 0,
                received_total INTEGER NOT NULL DEFAULT 0,
                invalid_total INTEGER NOT NULL DEFAULT 0,
                last_event_at REAL,
                updated_at REAL NOT NULL
            )
            """
        )

    PostQueue(database)

    with sqlite3.connect(database) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(stream_health)")}
    assert "source_event_at" in columns


def test_metrics_adapters_can_open_without_schema_writes(tmp_path: Path) -> None:
    post_database = tmp_path / "posts.sqlite3"
    label_database = tmp_path / "labels.sqlite3"

    PostQueue(post_database, migrate=False)
    LabelQueue(label_database, migrate=False)

    assert not post_database.exists()
    assert not label_database.exists()


def test_reader_accepts_only_post_creates_and_checkpoints_every_event(tmp_path: Path) -> None:
    queue = PostQueue(tmp_path / "posts.sqlite3")
    reader = PostReader(queue, "jetstream-test")
    create = {
        "kind": "commit",
        "seq": 10,
        "time_us": 10_000_000,
        "did": "did:plc:alice",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "abc",
            "cid": "bafy-post",
            "record": {"text": "finale spoiler", "createdAt": "2026-01-01T00:00:00Z"},
        },
    }
    update = {
        "kind": "commit",
        "seq": 11,
        "did": "did:plc:alice",
        "commit": {
            "operation": "update",
            "collection": "app.bsky.feed.post",
            "rkey": "abc",
            "cid": "bafy-updated",
            "record": {"text": "edited"},
        },
    }

    assert reader.process_event(create) == 1
    assert reader.process_event(update) == 0
    assert queue.get_cursor("jetstream-test") == 11
    assert queue.metrics(now=11.0)["stream_source_lag_seconds"] == 1.0
    claimed = queue.claim("detector", now=10_000_000_000.0)
    assert claimed[0].job.post_uri == "at://did:plc:alice/app.bsky.feed.post/abc"
    assert claimed[0].job.post_cid == "bafy-post"


def test_jetstream_url_uses_post_filter_and_durable_cursor() -> None:
    url = build_jetstream_url(
        "wss://jetstream.example/subscribe?unrelated=value&wantedCollections=old",
        cursor=123456,
    )

    assert url == (
        "wss://jetstream.example/subscribe?unrelated=value&"
        "wantedCollections=app.bsky.feed.post&cursor=123456"
    )


def test_detector_handoff_is_idempotent(tmp_path: Path) -> None:
    posts = PostQueue(tmp_path / "posts.sqlite3")
    labels = LabelQueue(tmp_path / "labels.sqlite3")
    detector = KeywordSpoilerDetector(["spoiler"], version="test-v1")
    positive = post_job(1, "A spoiler appears here")
    negative = post_job(2, "A harmless observation")
    posts.enqueue_and_checkpoint([positive], stream_id="test-stream", sequence=1)
    posts.enqueue_and_checkpoint([negative], stream_id="test-stream", sequence=2)

    with SpoilerWorkerInterface(
        posts,
        labels,
        "detector-a",
        label_value=detector.label_value,
        detector_version=detector.version,
    ) as worker_interface:
        result = detect_once(worker_interface, detector, batch_size=10)
        persisted = worker_interface.flush()

    assert result == {"claimed": 2, "positive": 2, "negative": 0, "failed": 0}
    assert persisted == {"positive": 2, "negative": 0, "failed": 0}
    assert posts.metrics()["available"] == 0

    claimed = labels.claim("publisher-a", limit=10)
    assert len(claimed) == 2
    assert {delivery.job.post_cid for delivery in claimed} == {
        positive.post_cid,
        negative.post_cid,
    }
    assert {delivery.job.label_value for delivery in claimed} == {"spoiler"}

    duplicate = LabelJob.create(
        post_uri=positive.post_uri,
        post_cid=positive.post_cid,
        label_value="spoiler",
        detector_version="different-version",
    )
    assert not labels.enqueue(duplicate)


def test_detector_function_returns_only_id_and_decision(tmp_path: Path) -> None:
    posts = PostQueue(tmp_path / "posts.sqlite3")
    labels = LabelQueue(tmp_path / "labels.sqlite3")
    detector = KeywordSpoilerDetector(["spoiler"])
    job = post_job()
    posts.enqueue_and_checkpoint([job], stream_id="test-stream", sequence=1)

    with SpoilerWorkerInterface(
        posts,
        labels,
        "detector-a",
        label_value=detector.label_value,
        detector_version=detector.version,
    ) as worker_interface:
        item = worker_interface.get_items()[0]
        identifier, decision = isSpoiler(item, detector)
        worker_interface.submit_result(identifier, decision)

    assert identifier == item.id
    assert decision is True


def test_label_write_does_not_block_classification_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posts = PostQueue(tmp_path / "posts.sqlite3")
    labels = LabelQueue(tmp_path / "labels.sqlite3")
    job = post_job()
    posts.enqueue_and_checkpoint([job], stream_id="test-stream", sequence=1)
    write_started = threading.Event()
    allow_write = threading.Event()
    enqueue = labels.enqueue

    def slow_enqueue(label: LabelJob, *, now: float | None = None) -> bool:
        write_started.set()
        assert allow_write.wait(timeout=2)
        return enqueue(label, now=now)

    monkeypatch.setattr(labels, "enqueue", slow_enqueue)
    with SpoilerWorkerInterface(
        posts,
        labels,
        "detector-a",
        label_value="spoiler",
        detector_version="test-v1",
    ) as worker_interface:
        item = worker_interface.get_items()[0]
        worker_interface.submit_result(item.id, True)
        try:
            assert write_started.wait(timeout=1)
            assert posts.metrics()["leased"] == 1
        finally:
            allow_write.set()
        assert worker_interface.flush() == {
            "positive": 1,
            "negative": 0,
            "failed": 0,
        }

    assert posts.metrics()["available"] == 0
    assert labels.metrics()["available"] == 1


class RecordingClient:
    def __init__(self) -> None:
        self.jobs: list[LabelJob] = []

    def publish(self, job: LabelJob) -> None:
        self.jobs.append(job)


def test_publisher_acknowledges_success(tmp_path: Path) -> None:
    labels = LabelQueue(tmp_path / "labels.sqlite3")
    job = LabelJob.create(
        post_uri="at://did:plc:test/app.bsky.feed.post/1",
        post_cid="cid-1",
        label_value="spoiler",
        detector_version="test-v1",
    )
    labels.enqueue(job)
    client = RecordingClient()

    result = publish_once(labels, client, "publisher-a")
    assert result == {"claimed": 1, "published": 1, "failed": 0}
    assert client.jobs == [job]
    assert labels.metrics()["published_total"] == 1
    assert not labels.enqueue(job)
    assert labels.metrics()["available"] == 0


def test_dry_run_publisher_logs_each_output(capsys) -> None:
    client = DryRunLabelClient()
    job = LabelJob.create(
        post_uri="at://did:plc:test/app.bsky.feed.post/1",
        post_cid="cid-1",
        label_value="spoiler",
        detector_version="test-v1",
    )

    client.publish(job)

    output = json.loads(capsys.readouterr().out)
    assert output == {
        "dry_run_published": 1,
        "post_uri": job.post_uri,
        "post_cid": job.post_cid,
        "label": "spoiler",
    }


def test_metrics_snapshot_combines_both_interfaces(tmp_path: Path) -> None:
    posts = PostQueue(tmp_path / "posts.sqlite3")
    labels = LabelQueue(tmp_path / "labels.sqlite3")
    config = tmp_path / "tags.json"
    config.write_text(
        '{"label_value":"spoiler","version":"test-v1","tags":["finale"]}',
        encoding="utf-8",
    )
    job = post_job()
    posts.enqueue_and_checkpoint(
        [job],
        stream_id="test-stream",
        sequence=1,
        source_event_at=99.0,
        now=100.0,
    )

    snapshot = build_snapshot(posts, labels, config, window_seconds=60, now=105.0)
    assert snapshot["configuration"]["tags"] == ["finale"]
    assert snapshot["queues"]["posts"]["available"] == 1
    assert snapshot["totals"]["ingested"] == 1
    assert snapshot["rates"]["ingestion_per_second"] == 1 / 60
    assert snapshot["stream"]["caught_up"]
    assert snapshot["stream"]["source_lag_seconds"] == 6.0


def test_parser_rejects_missing_cursor() -> None:
    try:
        parse_jetstream_event({"kind": "commit"}, "stream")
    except ValueError as error:
        assert "cursor" in str(error)
    else:
        raise AssertionError("missing cursor was accepted")


def test_metrics_registry_lease_uses_compare_and_swap() -> None:
    requests = []
    endpoint = {
        "state": "offline",
        "instance_id": None,
        "public_url": None,
        "updated_at": None,
    }

    def opener(request, *, timeout):
        requests.append((request, timeout))
        if "/git/refs/heads/live-metrics-endpoint-registry" in request.full_url:
            return BytesIO(b'{"object":{"sha":"branch-sha"}}')
        if request.method == "GET":
            encoded = base64.b64encode(json.dumps(endpoint).encode()).decode()
            return BytesIO(json.dumps({"sha": "old-sha", "content": encoded}).encode())
        return BytesIO(b'{"content":{"sha":"lease-sha"}}')

    registry = GitHubEndpointRegistry(
        repository="owner/repository",
        path="docs/metrics-endpoint.json",
        branch="live-metrics-endpoint-registry",
        source_branch="main",
        token="secret",
        opener=opener,
    )

    assert registry.acquire("instance-a") == "lease-sha"
    update = json.loads(requests[-1][0].data)
    assert update["sha"] == "old-sha"
    record = json.loads(base64.b64decode(update["content"]))
    assert record["state"] == "starting"
    assert record["instance_id"] == "instance-a"


def test_metrics_registry_refuses_an_active_owner() -> None:
    endpoint = {
        "state": "active",
        "instance_id": "instance-a",
        "public_url": "https://example.trycloudflare.com",
    }

    def opener(request, *, timeout):
        if "/git/refs/heads/live-metrics-endpoint-registry" in request.full_url:
            return BytesIO(b'{"object":{"sha":"branch-sha"}}')
        encoded = base64.b64encode(json.dumps(endpoint).encode()).decode()
        return BytesIO(json.dumps({"sha": "old-sha", "content": encoded}).encode())

    registry = GitHubEndpointRegistry(
        repository="owner/repository",
        path="docs/metrics-endpoint.json",
        branch="live-metrics-endpoint-registry",
        source_branch="main",
        token="secret",
        opener=opener,
    )

    with pytest.raises(LeaseUnavailable, match="already claimed"):
        registry.acquire("instance-b")


def test_metrics_registry_allows_same_installation_to_reclaim_stale_lease() -> None:
    requests = []
    endpoint = {
        "state": "active",
        "instance_id": "instance-a",
        "public_url": "https://stale.trycloudflare.com",
    }

    def opener(request, *, timeout):
        requests.append(request)
        if "/git/refs/heads/live-metrics-endpoint-registry" in request.full_url:
            return BytesIO(b'{"object":{"sha":"branch-sha"}}')
        if request.method == "GET":
            encoded = base64.b64encode(json.dumps(endpoint).encode()).decode()
            return BytesIO(json.dumps({"sha": "old-sha", "content": encoded}).encode())
        return BytesIO(b'{"content":{"sha":"new-sha"}}')

    registry = GitHubEndpointRegistry(
        repository="owner/repository",
        path="docs/metrics-endpoint.json",
        branch="live-metrics-endpoint-registry",
        source_branch="main",
        token="secret",
        opener=opener,
    )

    assert registry.acquire("instance-a") == "new-sha"
    update = json.loads(requests[-1].data)
    assert update["sha"] == "old-sha"


def test_metrics_instance_id_survives_restart(tmp_path: Path) -> None:
    instance_id_file = tmp_path / ".metrics-instance-id"

    first = load_or_create_instance_id(instance_id_file)
    second = load_or_create_instance_id(instance_id_file)

    assert first == second
    assert len(first) == 32


def test_metrics_http_port_disallows_socket_reuse() -> None:
    assert MetricsHTTPServer.allow_reuse_address is False
    assert MetricsHTTPServer.allow_reuse_port is False


def test_live_rates_respond_to_recent_counter_changes() -> None:
    tracker = LiveRateTracker(window_seconds=10)

    def snapshot(ingested: int, detected: int, labeled: int, published: int) -> dict:
        return {
            "rates": {},
            "totals": {
                "ingested": ingested,
                "detected": detected,
                "labels_enqueued": labeled,
                "published": published,
            },
        }

    first = tracker.update(snapshot(100, 90, 80, 70), now=100.0)
    second = tracker.update(snapshot(150, 140, 130, 120), now=105.0)

    assert first["rates"]["ingestion_per_second"] == 0
    assert second["rates"] == {
        "ingestion_per_second": 10,
        "label_per_second": 10,
        "publish_per_second": 10,
        "detection_per_second": 10,
    }
    assert second["window_seconds"] == 10


def test_coordinator_builds_complete_process_group(tmp_path: Path) -> None:
    config = load_config(Path("config/pipeline.json"))
    source = tmp_path / "events.jsonl"
    specs = build_component_specs(
        config,
        python_executable="python-test",
        input_override=source,
    )

    assert [spec.name for spec in specs] == [
        "metrics",
        *(f"detector-{index + 1}" for index in range(config["detectors"]["workers"])),
        *(f"publisher-{index + 1}" for index in range(config["publishers"]["workers"])),
        "reader",
    ]
    assert specs[-1].is_reader
    assert not specs[-1].restart
    assert str(source) in specs[-1].command
    assert "--dry-run" in specs[-2].command
    assert specs[1].command[specs[1].command.index("--write-buffer-size") + 1] == "100"
    assert load_supervision_settings(config).max_restarts == 5


def test_coordinator_does_not_require_reader_input_file() -> None:
    config = load_config(Path("config/pipeline.json"))
    specs = build_component_specs(config, python_executable="python-test")
    reader = next(spec for spec in specs if spec.is_reader)

    assert "--input" not in reader.command
    assert "--endpoint" in reader.command
    assert "wss://jetstream2.us-west.bsky.network/subscribe" in reader.command


def test_coordinator_can_bound_live_smoke_test() -> None:
    config = load_config(Path("config/pipeline.json"))
    specs = build_component_specs(
        config,
        python_executable="python-test",
        max_events_override=25,
    )
    reader = next(spec for spec in specs if spec.is_reader)

    assert reader.command[reader.command.index("--max-events") + 1] == "25"


def test_coordinator_runs_exclusive_live_metrics_gateway() -> None:
    pipeline = load_config(Path("config/pipeline.json"))
    specs = build_component_specs(pipeline, python_executable="python-test")
    metrics = next(spec for spec in specs if spec.name == "metrics")

    assert pipeline["metrics"]["repository"] == "CSE210-Team05/Bluesky-SpoilerDetection"
    assert pipeline["metrics"]["registry_path"] == "docs/metrics-endpoint.json"
    assert pipeline["metrics"]["registry_branch"] == "live-metrics-endpoint-registry"
    assert "metrics.gateway" in metrics.command
    assert metrics.critical
    assert not metrics.restart
    assert metrics.command[metrics.command.index("--host") + 1] == "127.0.0.1"
    assert metrics.command[metrics.command.index("--port") + 1] == "8765"
    assert metrics.command[metrics.command.index("--live-rate-window-seconds") + 1] == "10"
    assert metrics.command[metrics.command.index("--caught-up-lag-seconds") + 1] == "10"


def test_dashboard_waits_for_gateway_then_polls_live_metrics() -> None:
    app = Path("docs/app.js").read_text(encoding="utf-8")
    index = Path("docs/index.html").read_text(encoding="utf-8")
    workflow = Path(".github/workflows/deploy-pages.yml").read_text(encoding="utf-8")

    assert (
        "https://raw.githubusercontent.com/CSE210-Team05/"
        "Bluesky-SpoilerDetection/live-metrics-endpoint-registry/docs/metrics-endpoint.json"
    ) in app
    assert 'metricsSource = `${endpoint.public_url.replace(/\\/$/, "")}/metrics`' in app
    assert "window.setInterval(refresh, 1_000)" in app
    assert 'src="app.js?v=5"' in index
    assert "Up to date" in app
    assert "Catching up" in app
    assert '"docs/**"' not in workflow
    assert "docs/metrics.json" not in workflow
    assert "docs/metrics-endpoint.json" not in workflow
