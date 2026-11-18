from __future__ import annotations

import asyncio
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace
from urllib.request import urlopen

import pytest

from Bluesky_interface.post_fetcher_core.post_fetcher import (
    PostReader,
    build_jetstream_url,
    parse_jetstream_event,
    receive_jetstream_message,
)
from Bluesky_interface.post_fetcher_interface import (
    PostJob,
    PostQueue,
    ShardedPostQueue,
    rectify_post_shards,
)
from coordinator import (
    ComponentSpec,
    Coordinator,
    SupervisionSettings,
    build_component_specs,
    load_config,
    load_supervision_settings,
)
from label_publisher.publisher import DryRunLabelClient, process_once as publish_once
from metrics.gateway import (
    LiveRateTracker,
    MetricsHTTPServer,
    SnapshotProvider,
    run_gateway,
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
    ShardedLabelQueue,
    SpoilerWorkerInterface,
    rectify_label_shards,
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


def test_post_database_rectification_migrates_backlog_and_replaces_generation(
    tmp_path: Path,
) -> None:
    legacy_database = tmp_path / "posts.sqlite3"
    legacy = PostQueue(legacy_database)
    jobs = [post_job(sequence) for sequence in range(1, 21)]
    for job in jobs:
        legacy.enqueue_and_checkpoint(
            [job],
            stream_id="test-stream",
            sequence=job.sequence,
            source_event_at=float(job.sequence),
            now=float(job.sequence),
        )
    completed = legacy.claim("completed-worker", limit=3, now=30.0)
    for delivery in completed:
        assert legacy.acknowledge(
            delivery.job.job_id,
            "completed-worker",
            outcome="positive",
            now=31.0,
        )
    abandoned = legacy.claim("stopped-worker", limit=1, now=32.0)
    assert len(abandoned) == 1

    four_shards = rectify_post_shards(legacy_database, 4)
    sharded = ShardedPostQueue(four_shards, migrate=False)
    migrated_metrics = sharded.metrics(now=33.0)

    assert len(four_shards) == 4
    assert not legacy_database.exists()
    assert (tmp_path / "posts.sqlite3.shards.json").is_file()
    assert migrated_metrics["ingested_total"] == 20
    assert migrated_metrics["acknowledged_total"] == 3
    assert migrated_metrics["available"] == 17
    assert migrated_metrics["leased"] == 0
    assert sharded.get_cursor("test-stream") == 20

    remaining_ids: set[str] = set()
    populated_shards = 0
    for index, shard in enumerate(sharded.shards):
        deliveries = shard.claim(f"detector-{index}", limit=100, now=34.0)
        if deliveries:
            populated_shards += 1
        remaining_ids.update(delivery.job.job_id for delivery in deliveries)
    assert remaining_ids == {job.job_id for job in jobs[3:]}
    assert populated_shards > 1

    assert rectify_post_shards(legacy_database, 4) == four_shards
    two_shards = rectify_post_shards(legacy_database, 2)
    reshaped = ShardedPostQueue(two_shards, migrate=False)

    assert len(two_shards) == 2
    assert all(not path.exists() for path in four_shards)
    assert reshaped.metrics(now=35.0)["available"] == 17


def test_failed_post_rectification_keeps_legacy_database(tmp_path: Path) -> None:
    legacy_database = tmp_path / "posts.sqlite3"
    legacy_database.write_bytes(b"not a sqlite database")

    with pytest.raises(sqlite3.DatabaseError):
        rectify_post_shards(legacy_database, 2)

    assert legacy_database.read_bytes() == b"not a sqlite database"
    assert not (tmp_path / "posts.sqlite3.shards.json").exists()


def test_label_database_rectification_preserves_backlog_and_publication_history(
    tmp_path: Path,
) -> None:
    legacy_database = tmp_path / "labels.sqlite3"
    legacy = LabelQueue(legacy_database)
    jobs = [
        LabelJob.create(
            post_uri=f"at://did:plc:test/app.bsky.feed.post/{index}",
            post_cid=f"cid-{index}",
            label_value="spoiler",
            detector_version="test-v1",
            detected_at=float(index),
        )
        for index in range(1, 21)
    ]
    for index, job in enumerate(jobs, start=1):
        assert legacy.enqueue(job, now=float(index))
    completed = legacy.claim("completed-publisher", limit=3, now=30.0)
    for delivery in completed:
        assert legacy.acknowledge(
            delivery.job.job_id,
            "completed-publisher",
            now=31.0,
        )
    assert len(legacy.claim("stopped-publisher", limit=1, now=32.0)) == 1

    four_shards = rectify_label_shards(legacy_database, 4)
    sharded = ShardedLabelQueue(four_shards, migrate=False)
    migrated_metrics = sharded.metrics(now=33.0)

    assert len(four_shards) == 4
    assert not legacy_database.exists()
    assert (tmp_path / "labels.sqlite3.shards.json").is_file()
    assert migrated_metrics["enqueued_total"] == 20
    assert migrated_metrics["published_total"] == 3
    assert migrated_metrics["available"] == 17
    assert migrated_metrics["leased"] == 0

    published_ids: set[str] = set()
    remaining_ids: set[str] = set()
    for index, shard in enumerate(sharded.shards):
        with sqlite3.connect(shard.database) as connection:
            published_ids.update(
                str(row[0]) for row in connection.execute("SELECT job_id FROM published_labels")
            )
        remaining_ids.update(
            delivery.job.job_id
            for delivery in shard.claim(f"publisher-{index}", limit=100, now=34.0)
        )
    assert published_ids == {job.job_id for job in jobs[:3]}
    assert remaining_ids == {job.job_id for job in jobs[3:]}

    assert rectify_label_shards(legacy_database, 4) == four_shards
    two_shards = rectify_label_shards(legacy_database, 2)
    reshaped = ShardedLabelQueue(two_shards, migrate=False)

    assert len(two_shards) == 2
    assert all(not path.exists() for path in four_shards)
    assert reshaped.metrics(now=35.0)["available"] == 17


def test_failed_label_rectification_keeps_legacy_database(tmp_path: Path) -> None:
    legacy_database = tmp_path / "labels.sqlite3"
    legacy_database.write_bytes(b"not a sqlite database")

    with pytest.raises(sqlite3.DatabaseError):
        rectify_label_shards(legacy_database, 2)

    assert legacy_database.read_bytes() == b"not a sqlite database"
    assert not (tmp_path / "labels.sqlite3.shards.json").exists()


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


def test_jetstream_idle_timeout_detects_a_silent_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SilentWebsocket:
        async def recv(self) -> str:
            return "never reached"

    async def raise_timeout(awaitable, *, timeout):
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", raise_timeout)
    receive = receive_jetstream_message(SilentWebsocket(), 0.01)
    with pytest.raises(RuntimeError, match="delivered no events"):
        receive.send(None)


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

    assert result == {"claimed": 2, "positive": 1, "negative": 1, "failed": 0}
    assert persisted == {"positive": 1, "negative": 1, "failed": 0}
    assert posts.metrics()["available"] == 0

    claimed = labels.claim("publisher-a", limit=10)
    assert len(claimed) == 1
    assert claimed[0].job.post_cid == positive.post_cid
    assert {delivery.job.label_value for delivery in claimed} == {"spoiler"}

    duplicate = LabelJob.create(
        post_uri=positive.post_uri,
        post_cid=positive.post_cid,
        label_value="spoiler",
        detector_version="different-version",
    )
    assert not labels.enqueue(duplicate)


def test_keyword_detector_matches_labels_without_substring_false_positives() -> None:
    detector = KeywordSpoilerDetector(["spoiler", "finale"])

    positive = detector.detect("The FINALE spoiler is here")

    assert positive.is_spoiler is True
    assert positive.matched_tags == ("spoiler", "finale")
    assert detector.detect("This spoilerish word is harmless").is_spoiler is False
    assert detector.detect("No configured label appears here").is_spoiler is False


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

    snapshot = build_snapshot(
        posts,
        labels,
        config,
        window_seconds=60,
        detector_workers=10,
        publisher_workers=2,
        now=105.0,
    )
    assert snapshot["configuration"]["tags"] == ["finale"]
    assert snapshot["queues"]["posts"]["available"] == 1
    assert snapshot["totals"]["ingested"] == 1
    assert snapshot["rates"]["ingestion_per_second"] == 1 / 60
    assert snapshot["stream"]["caught_up"]
    assert snapshot["stream"]["source_lag_seconds"] == 6.0
    assert snapshot["workers"]["detectors"]["configured"] == 10
    assert snapshot["workers"]["detectors"]["shards"] == 1
    assert snapshot["workers"]["publishers"]["configured"] == 2
    assert snapshot["workers"]["window_seconds"] == 60


def test_queue_metrics_report_busy_workers_and_recent_claim_wait(
    tmp_path: Path,
) -> None:
    posts = PostQueue(tmp_path / "worker-posts.sqlite3")
    post = post_job()
    posts.enqueue_and_checkpoint(
        [post],
        stream_id="test-stream",
        sequence=1,
        now=100.0,
    )
    assert len(posts.claim("detector-a", now=105.0)) == 1
    post_metrics = posts.metrics(window_seconds=60, now=105.0)

    labels = LabelQueue(tmp_path / "worker-labels.sqlite3")
    label = LabelJob.create(
        post_uri=post.post_uri,
        post_cid=post.post_cid,
        label_value="spoiler",
        detector_version="test-v1",
    )
    assert labels.enqueue(label, now=200.0)
    assert len(labels.claim("publisher-a", now=203.0)) == 1
    label_metrics = labels.metrics(window_seconds=60, now=203.0)

    assert post_metrics["busy_workers"] == 1
    assert post_metrics["average_queue_wait_seconds"] == 5.0
    assert float(post_metrics["average_claim_seconds"]) >= 0
    assert label_metrics["busy_workers"] == 1
    assert label_metrics["average_queue_wait_seconds"] == 3.0
    assert float(label_metrics["average_claim_seconds"]) >= 0


def test_parser_rejects_missing_cursor() -> None:
    try:
        parse_jetstream_event({"kind": "commit"}, "stream")
    except ValueError as error:
        assert "cursor" in str(error)
    else:
        raise AssertionError("missing cursor was accepted")


def test_metrics_http_port_reuses_closed_address_not_active_listener(
    tmp_path: Path,
) -> None:
    dashboard = tmp_path / "restart-dashboard"
    dashboard.mkdir()
    (dashboard / "index.html").write_text("restart test", encoding="utf-8")
    (dashboard / "app.js").write_text("", encoding="utf-8")
    (dashboard / "styles.css").write_text("", encoding="utf-8")

    class ReadyProvider:
        @staticmethod
        def status() -> dict:
            return {
                "snapshot_ready": True,
                "snapshot_age_seconds": 0.1,
                "snapshot_error": None,
            }

        @staticmethod
        def get() -> dict:
            return {"generated_at": "test"}

    first = MetricsHTTPServer(
        ("127.0.0.1", 0),
        dashboard_directory=dashboard,
        snapshot_provider=ReadyProvider(),  # type: ignore[arg-type]
    )
    port = int(first.server_address[1])
    thread = threading.Thread(target=first.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(OSError):
            MetricsHTTPServer(
                ("127.0.0.1", port),
                dashboard_directory=dashboard,
                snapshot_provider=ReadyProvider(),  # type: ignore[arg-type]
            )
        with urlopen(f"http://127.0.0.1:{port}/", timeout=1) as response:
            assert response.read() == b"restart test"
    finally:
        first.shutdown()
        first.server_close()
        thread.join(timeout=1)

    restarted = MetricsHTTPServer(
        ("127.0.0.1", port),
        dashboard_directory=dashboard,
        snapshot_provider=ReadyProvider(),  # type: ignore[arg-type]
    )
    restarted.server_close()

    assert MetricsHTTPServer.allow_reuse_address is True
    assert MetricsHTTPServer.allow_reuse_port is False


def test_metrics_gateway_rejects_non_loopback_host() -> None:
    with pytest.raises(ValueError, match="loopback"):
        run_gateway(SimpleNamespace(host="0.0.0.0"))  # type: ignore[arg-type]


def test_metrics_server_hosts_dashboard_and_snapshot_on_one_origin(
    tmp_path: Path,
) -> None:
    dashboard = tmp_path / "docs"
    dashboard.mkdir()
    (dashboard / "index.html").write_text("<h1>Local dashboard</h1>", encoding="utf-8")
    (dashboard / "app.js").write_text("fetch('/metrics')", encoding="utf-8")
    (dashboard / "styles.css").write_text("body {}", encoding="utf-8")

    class ReadyProvider:
        @staticmethod
        def status() -> dict:
            return {
                "snapshot_ready": True,
                "snapshot_age_seconds": 0.1,
                "snapshot_error": None,
            }

        @staticmethod
        def get() -> dict:
            return {"generated_at": "test", "totals": {"ingested": 1}}

    server = MetricsHTTPServer(
        ("127.0.0.1", 0),
        dashboard_directory=dashboard,
        snapshot_provider=ReadyProvider(),  # type: ignore[arg-type]
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with urlopen(f"{origin}/", timeout=1) as response:
            assert response.headers.get_content_type() == "text/html"
            assert b"Local dashboard" in response.read()
        with urlopen(f"{origin}/metrics", timeout=1) as response:
            assert json.load(response)["totals"]["ingested"] == 1
        with urlopen(f"{origin}/health", timeout=1) as response:
            assert json.load(response)["status"] == "active"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


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


def test_metrics_requests_use_cached_snapshot_while_refresh_is_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_refresh_started = threading.Event()
    allow_second_refresh = threading.Event()
    calls = 0

    def build_test_snapshot(*_args, now: float, **_kwargs) -> dict:
        nonlocal calls
        calls += 1
        if calls == 2:
            second_refresh_started.set()
            assert allow_second_refresh.wait(timeout=2)
        return {
            "generated_at": str(now),
            "rates": {},
            "totals": {
                "ingested": calls,
                "detected": calls,
                "labels_enqueued": calls,
                "published": calls,
            },
        }

    monkeypatch.setattr("metrics.gateway.build_snapshot", build_test_snapshot)
    provider = SnapshotProvider(
        [tmp_path / "posts.sqlite3"],
        [tmp_path / "labels.sqlite3"],
        tmp_path / "tags.json",
        window_seconds=60,
        minimum_refresh_seconds=0.01,
    )
    provider.start()
    try:
        assert provider.wait_until_ready(timeout=1)
        first = provider.get()
        assert second_refresh_started.wait(timeout=1)

        started_at = time.monotonic()
        cached = provider.get()
        elapsed = time.monotonic() - started_at

        assert cached is first
        assert elapsed < 0.05
        assert provider.status()["snapshot_ready"] is True
    finally:
        allow_second_refresh.set()
        provider.stop()


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

    detectors = [spec for spec in specs if spec.name.startswith("detector-")]
    detector_databases = [
        spec.command[spec.command.index("--post-database") + 1]
        for spec in detectors
    ]
    assert len(set(detector_databases)) == config["databases"]["post_shards"]
    expected_maximum = (
        config["detectors"]["workers"] + config["databases"]["post_shards"] - 1
    ) // config["databases"]["post_shards"]
    assert (
        max(detector_databases.count(path) for path in set(detector_databases))
        == expected_maximum
    )
    metrics = next(spec for spec in specs if spec.name == "metrics")
    reader = next(spec for spec in specs if spec.is_reader)
    assert metrics.command.count("--post-database") == config["databases"]["post_shards"]
    assert metrics.command.count("--label-database") == config["databases"]["label_shards"]
    assert reader.command.count("--database") == config["databases"]["post_shards"]
    assert all(
        spec.command.count("--label-database") == config["databases"]["label_shards"]
        for spec in detectors
    )
    publishers = [spec for spec in specs if spec.name.startswith("publisher-")]
    publisher_databases = [
        spec.command[spec.command.index("--database") + 1]
        for spec in publishers
    ]
    assert len(set(publisher_databases)) == config["databases"]["label_shards"]


def test_coordinator_does_not_require_reader_input_file() -> None:
    config = load_config(Path("config/pipeline.json"))
    specs = build_component_specs(config, python_executable="python-test")
    reader = next(spec for spec in specs if spec.is_reader)

    assert "--input" not in reader.command
    assert "--endpoint" in reader.command
    endpoint_values = [
        reader.command[index + 1]
        for index, value in enumerate(reader.command)
        if value == "--endpoint"
    ]
    assert endpoint_values == config["reader"]["endpoints"]


def test_coordinator_can_bound_live_smoke_test() -> None:
    config = load_config(Path("config/pipeline.json"))
    specs = build_component_specs(
        config,
        python_executable="python-test",
        max_events_override=25,
    )
    reader = next(spec for spec in specs if spec.is_reader)

    assert reader.command[reader.command.index("--max-events") + 1] == "25"
    assert reader.command[reader.command.index("--idle-timeout") + 1] == "30"


def test_coordinator_runs_local_metrics_dashboard() -> None:
    pipeline = load_config(Path("config/pipeline.json"))
    specs = build_component_specs(pipeline, python_executable="python-test")
    metrics = next(spec for spec in specs if spec.name == "metrics")

    assert pipeline["metrics"]["dashboard_directory"] == "docs"
    assert "metrics.gateway" in metrics.command
    assert metrics.critical
    assert not metrics.restart
    assert "--dashboard-directory" in metrics.command
    assert metrics.command[metrics.command.index("--host") + 1] == "127.0.0.1"
    assert metrics.command[metrics.command.index("--port") + 1] == "8765"
    assert metrics.command[metrics.command.index("--live-rate-window-seconds") + 1] == "10"
    assert metrics.command[metrics.command.index("--caught-up-lag-seconds") + 1] == "10"
    assert metrics.command[metrics.command.index("--detector-workers") + 1] == str(
        pipeline["detectors"]["workers"]
    )
    assert metrics.command[metrics.command.index("--publisher-workers") + 1] == str(
        pipeline["publishers"]["workers"]
    )
    assert "--repository" not in metrics.command
    assert "--cloudflared" not in metrics.command
    assert metrics.readiness_statuses == ("active",)
    assert metrics.open_in_browser


def test_coordinator_opens_ready_local_dashboard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dashboard_url = "http://127.0.0.1:8765/"
    monkeypatch.setattr(
        "coordinator.urlopen",
        lambda *_args, **_kwargs: BytesIO(
            json.dumps(
                {"status": "active", "dashboard_url": dashboard_url}
            ).encode()
        ),
    )
    opened: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "coordinator.webbrowser.open",
        lambda url, *, new: opened.append((url, new)) or True,
    )

    spec = ComponentSpec(
        name="metrics",
        command=("metrics",),
        readiness_url=f"{dashboard_url}health",
        startup_timeout_seconds=1,
        open_in_browser=True,
    )
    coordinator = Coordinator(
        [spec],
        post_queue=PostQueue(tmp_path / "posts.sqlite3"),
        label_queue=LabelQueue(tmp_path / "labels.sqlite3"),
        settings=SupervisionSettings(),
    )

    class RunningProcess:
        @staticmethod
        def poll() -> None:
            return None

    coordinator.processes[spec.name] = RunningProcess()  # type: ignore[assignment]

    assert coordinator._wait_until_ready(spec)  # pylint: disable=protected-access
    assert opened == [(dashboard_url, 2)]


def test_dashboard_polls_same_origin_live_metrics() -> None:
    app = Path("docs/app.js").read_text(encoding="utf-8")
    index = Path("docs/index.html").read_text(encoding="utf-8")

    assert 'const metricsSource = "/metrics"' in app
    assert "raw.githubusercontent.com" not in app
    assert "endpointRegistry" not in app
    assert "window.setInterval(refresh, 1_000)" in app
    assert 'src="app.js?v=10"' in index
    assert '<span class="environment">Local</span>' in index
    assert "Worker pools" in index
    assert 'id="detector-workers"' in index
    assert 'id="detector-shards"' in index
    assert 'id="publisher-workers"' in index
    assert "average_queue_wait_seconds" in app
    assert "average_claim_seconds" in app
    assert "Up to date" in app
    assert "Catching up" in app
    assert "AbortController" in app
    assert "refreshInFlight" in app
    assert "updateWatchdog" in app
    assert "Reader stream is not advancing" in app
    assert not Path(".github/workflows/deploy-pages.yml").exists()
    assert not Path("docs/metrics-endpoint.json").exists()


def test_rendered_documentation_has_a_public_deployment() -> None:
    workflow = Path(".github/workflows/documentation.yml").read_text(
        encoding="utf-8"
    )
    readme = Path("README.md").read_text(encoding="utf-8")
    mkdocs = Path("mkdocs.yml").read_text(encoding="utf-8")

    site_url = "https://cse210-team05.github.io/Bluesky-SpoilerDetection/"
    assert "python -m mkdocs build --strict" in workflow
    assert "actions/upload-pages-artifact@v5" in workflow
    assert "actions/deploy-pages@v5" in workflow
    assert site_url in workflow
    assert "[Project documentation](documentation/index.md)" in readme
    assert "rendered documentation from `main`" in readme
    assert site_url in readme
    assert f"site_url: {site_url}" in mkdocs
