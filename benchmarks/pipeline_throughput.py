"""Profile representative end-to-end throughput through both SQLite queues."""

from __future__ import annotations

import argparse
import cProfile
from dataclasses import asdict, dataclass
from io import StringIO
import json
from pathlib import Path
import pstats
import tempfile
import time
from typing import Any

from Bluesky_interface.post_fetcher_interface import PostJob, PostQueue
from label_publisher.publisher import process_once as publish_once
from spoiler_interface.spoiler_core.spoiler_detector import (
    KeywordSpoilerDetector,
    process_once as detect_once,
)
from spoiler_interface.spoiler_interface import LabelJob, LabelQueue, SpoilerWorkerInterface


@dataclass(frozen=True, slots=True)
class ThroughputResult:
    """Serializable result from one benchmark run."""

    benchmark_version: int
    events: int
    elapsed_seconds: float
    throughput_per_second: float
    minimum_throughput_per_second: float
    passed: bool


class _NoOpLabelClient:
    """Remove network and console output while retaining publisher queue work."""

    published_count = 0

    def publish(self, _job: LabelJob) -> None:
        self.published_count += 1


def load_requirements(path: str | Path) -> dict[str, Any]:
    """Load and validate the versioned benchmark requirements."""
    with Path(path).open(encoding="utf-8") as source:
        requirements = json.load(source)
    if not isinstance(requirements, dict):
        raise ValueError("performance requirements must be a JSON object")
    events = requirements.get("pipeline_events")
    minimum = requirements.get("minimum_pipeline_throughput_per_second")
    version = requirements.get("benchmark_version")
    if not isinstance(events, int) or events < 1:
        raise ValueError("pipeline_events must be a positive integer")
    if not isinstance(minimum, (int, float)) or minimum <= 0:
        raise ValueError("minimum pipeline throughput must be positive")
    if not isinstance(version, int) or version < 1:
        raise ValueError("benchmark_version must be a positive integer")
    return requirements


def _jobs(event_count: int) -> list[PostJob]:
    return [
        PostJob.create(
            stream_id="throughput-benchmark",
            sequence=sequence,
            operation_index=0,
            post_uri=f"at://did:plc:benchmark/app.bsky.feed.post/{sequence}",
            post_cid=f"benchmark-cid-{sequence}",
            author_did="did:plc:benchmark",
            text="Representative spoiler post body used for throughput measurement",
            received_at=1.0,
        )
        for sequence in range(1, event_count + 1)
    ]


def run_pipeline_benchmark(
    directory: str | Path,
    *,
    event_count: int,
    minimum_throughput: float,
    benchmark_version: int = 1,
    profile_path: str | Path | None = None,
) -> ThroughputResult:
    """Run every event through ingestion, detection, and publication.

    Queue schema creation is intentionally outside the timed region. The timed
    workload includes job construction, durable ingestion, production keyword
    classification, detector handoff to the label queue, and publisher
    acknowledgement.
    """
    if event_count < 1:
        raise ValueError("event_count must be positive")
    if minimum_throughput <= 0:
        raise ValueError("minimum_throughput must be positive")

    benchmark_directory = Path(directory)
    benchmark_directory.mkdir(parents=True, exist_ok=True)
    posts = PostQueue(benchmark_directory / "posts.sqlite3")
    labels = LabelQueue(benchmark_directory / "labels.sqlite3")
    # Every event is positive only after executing the production keyword
    # matcher. This retains the worst-case durable label/publisher workload
    # without bypassing classification.
    detector = KeywordSpoilerDetector(["spoiler"], version="throughput-v2")
    client = _NoOpLabelClient()
    profiler = cProfile.Profile() if profile_path is not None else None

    if profiler is not None:
        profiler.enable()
    started_at = time.perf_counter()
    jobs = _jobs(event_count)
    inserted = sum(
        posts.enqueue_and_checkpoint(
            [job],
            stream_id="throughput-benchmark",
            sequence=job.sequence,
        )
        for job in jobs
    )
    if inserted != event_count:
        raise RuntimeError(f"expected {event_count} new post jobs, inserted {inserted}")

    detected = 0
    batch_size = min(100, event_count)
    with SpoilerWorkerInterface(
        posts,
        labels,
        "throughput-detector",
        label_value=detector.label_value,
        detector_version=detector.version,
        max_pending_writes=batch_size,
    ) as worker_interface:
        while detected < event_count:
            counts = detect_once(worker_interface, detector, batch_size=batch_size)
            if counts["claimed"] == 0:
                # The bounded asynchronous sink applies backpressure while its
                # writer commits the previous batch. Wait for that real work,
                # then let the detector claim the next batch.
                worker_interface.flush()
                continue
            detected += counts["claimed"]
        persisted = worker_interface.flush()
    if persisted["positive"] != event_count or persisted["failed"]:
        raise RuntimeError(f"detector handoff was incomplete: {persisted}")

    published = 0
    while published < event_count:
        counts = publish_once(
            labels,
            client,
            "throughput-publisher",
            batch_size=batch_size,
        )
        if counts["claimed"] == 0:
            raise RuntimeError("publisher stopped making progress")
        published += counts["published"]
    elapsed = time.perf_counter() - started_at
    if profiler is not None:
        profiler.disable()
        destination = Path(profile_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        profiler.dump_stats(destination)
        rendered_profile = StringIO()
        pstats.Stats(profiler, stream=rendered_profile).strip_dirs().sort_stats(
            "cumulative"
        ).print_stats(50)
        destination.with_suffix(".txt").write_text(
            rendered_profile.getvalue(),
            encoding="utf-8",
        )

    throughput = event_count / elapsed
    return ThroughputResult(
        benchmark_version=benchmark_version,
        events=event_count,
        elapsed_seconds=elapsed,
        throughput_per_second=throughput,
        minimum_throughput_per_second=minimum_throughput,
        passed=throughput >= minimum_throughput,
    )


def write_result(result: ThroughputResult, destination: str | Path) -> None:
    """Write a stable, machine-readable benchmark result."""
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(result), indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--requirements",
        type=Path,
        default=Path("config/performance.json"),
    )
    parser.add_argument("--output", type=Path, default=Path("performance-results.json"))
    parser.add_argument("--profile", type=Path)
    args = parser.parse_args(argv)
    requirements = load_requirements(args.requirements)

    with tempfile.TemporaryDirectory(prefix="spoiler-throughput-") as temporary:
        result = run_pipeline_benchmark(
            temporary,
            event_count=int(requirements["pipeline_events"]),
            minimum_throughput=float(
                requirements["minimum_pipeline_throughput_per_second"]
            ),
            benchmark_version=int(requirements["benchmark_version"]),
            profile_path=args.profile,
        )
    write_result(result, args.output)
    print(json.dumps(asdict(result), indent=2))
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
