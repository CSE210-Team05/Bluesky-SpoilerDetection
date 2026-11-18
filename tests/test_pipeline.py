from __future__ import annotations

import base64
import json
from io import BytesIO
from pathlib import Path

from Bluesky_interface.post_fetcher_core.post_fetcher import (
    PostReader,
    build_jetstream_url,
    parse_jetstream_event,
)
from Bluesky_interface.post_fetcher_interface import PostJob, PostQueue
from coordinator import build_component_specs, load_config, load_supervision_settings
from label_publisher.publisher import process_once as publish_once
from metrics.publisher import GitHubContentsPublisher
from metrics.snapshot import build_snapshot
from spoiler_interface.spoiler_core.spoiler_detector import (
    KeywordSpoilerDetector,
    process_once as detect_once,
)
from spoiler_interface.spoiler_interface import LabelJob, LabelQueue


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


def test_reader_accepts_only_post_creates_and_checkpoints_every_event(tmp_path: Path) -> None:
    queue = PostQueue(tmp_path / "posts.sqlite3")
    reader = PostReader(queue, "jetstream-test")
    create = {
        "kind": "commit",
        "seq": 10,
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

    result = detect_once(posts, labels, detector, "detector-a", batch_size=10)
    assert result == {"claimed": 2, "positive": 2, "negative": 0, "failed": 0}
    assert posts.metrics()["available"] == 0

    claimed = labels.claim("publisher-a", limit=10)
    assert len(claimed) == 2
    assert claimed[0].job.post_cid == positive.post_cid
    assert claimed[0].job.label_value == "spoiler"

    duplicate = LabelJob.create(
        post_uri=positive.post_uri,
        post_cid=positive.post_cid,
        label_value="spoiler",
        detector_version="different-version",
    )
    assert not labels.enqueue(duplicate)


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


def test_metrics_snapshot_combines_both_interfaces(tmp_path: Path) -> None:
    posts = PostQueue(tmp_path / "posts.sqlite3")
    labels = LabelQueue(tmp_path / "labels.sqlite3")
    config = tmp_path / "tags.json"
    config.write_text(
        '{"label_value":"spoiler","version":"test-v1","tags":["finale"]}',
        encoding="utf-8",
    )
    job = post_job()
    posts.enqueue_and_checkpoint([job], stream_id="test-stream", sequence=1, now=100.0)

    snapshot = build_snapshot(posts, labels, config, window_seconds=60, now=110.0)
    assert snapshot["configuration"]["tags"] == ["finale"]
    assert snapshot["queues"]["posts"]["available"] == 1
    assert snapshot["totals"]["ingested"] == 1
    assert snapshot["rates"]["ingestion_per_second"] == 1 / 60


def test_parser_rejects_missing_cursor() -> None:
    try:
        parse_jetstream_event({"kind": "commit"}, "stream")
    except ValueError as error:
        assert "cursor" in str(error)
    else:
        raise AssertionError("missing cursor was accepted")


def test_github_metrics_publisher_updates_existing_snapshot() -> None:
    requests = []

    def opener(request, *, timeout):
        requests.append((request, timeout))
        if request.method == "GET":
            return BytesIO(b'{"sha":"existing-file-sha"}')
        return BytesIO(b'{"commit":{"sha":"new-commit-sha"}}')

    publisher = GitHubContentsPublisher(
        repository="CSE210-Team05/Bluesky-SpoilerDetection",
        path="docs/metrics.json",
        branch="main",
        token="secret-token",
        opener=opener,
    )
    commit = publisher.publish({"generated_at": "now", "rates": {}})

    assert commit == "new-commit-sha"
    assert [request.method for request, _ in requests] == ["GET", "PUT"]
    update = json.loads(requests[1][0].data)
    assert update["sha"] == "existing-file-sha"
    assert update["branch"] == "main"
    assert json.loads(base64.b64decode(update["content"])) == {
        "generated_at": "now",
        "rates": {},
    }


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
        "detector-1",
        "detector-2",
        "publisher-1",
        "reader",
    ]
    assert specs[-1].is_reader
    assert not specs[-1].restart
    assert str(source) in specs[-1].command
    assert "--dry-run" in specs[-2].command
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


def test_metrics_are_pushed_to_github_without_pipeline_listener() -> None:
    pipeline = load_config(Path("config/pipeline.json"))
    specs = build_component_specs(pipeline, python_executable="python-test")
    metrics = next(spec for spec in specs if spec.name == "metrics")

    assert "host" not in pipeline["metrics"]
    assert "port" not in pipeline["metrics"]
    assert pipeline["metrics"]["repository"] == "CSE210-Team05/Bluesky-SpoilerDetection"
    assert pipeline["metrics"]["path"] == "docs/metrics.json"
    assert pipeline["metrics"]["token_env"] == "GITHUB_METRICS_TOKEN"
    assert "metrics.publisher" in metrics.command
    assert "--host" not in metrics.command
    assert "--port" not in metrics.command


def test_dashboard_polls_raw_metrics_without_redeploying() -> None:
    app = Path("docs/app.js").read_text(encoding="utf-8")
    index = Path("docs/index.html").read_text(encoding="utf-8")
    workflow = Path(".github/workflows/deploy-pages.yml").read_text(encoding="utf-8")

    assert (
        "https://raw.githubusercontent.com/CSE210-Team05/"
        "Bluesky-SpoilerDetection/main/docs/metrics.json"
    ) in app
    assert "window.setInterval(refresh, 5_000)" in app
    assert 'src="app.js?v=2"' in index
    assert '"docs/**"' not in workflow
    assert "docs/metrics.json" not in workflow
