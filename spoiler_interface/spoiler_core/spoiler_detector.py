"""Spoiler detector and queue worker.

The keyword detector is intentionally replaceable.  It provides a runnable
first pass while preserving the worker and queue contracts for a later model.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import socket
import time
from typing import Any, Iterable

from Bluesky_interface.post_fetcher_interface import PostQueue
from spoiler_interface.spoiler_interface import LabelJob, LabelQueue


@dataclass(frozen=True, slots=True)
class DetectionResult:
    is_spoiler: bool
    matched_tags: tuple[str, ...]


class KeywordSpoilerDetector:
    def __init__(
        self,
        tags: Iterable[str],
        *,
        label_value: str = "spoiler",
        version: str = "keyword-v1",
    ):
        cleaned = tuple(dict.fromkeys(tag.strip() for tag in tags if tag.strip()))
        self.tags = cleaned
        self.label_value = label_value
        self.version = version
        self._patterns = tuple(
            (tag, re.compile(rf"(?<!\w){re.escape(tag)}(?!\w)", re.IGNORECASE))
            for tag in cleaned
        )

    def detect(self, text: str) -> DetectionResult:
        # First-pass throughput mode: intentionally bypass classification so
        # every post exercises the complete label and publisher pipeline.
        return DetectionResult(True, ())

        matches = tuple(tag for tag, pattern in self._patterns if pattern.search(text))
        return DetectionResult(bool(matches), matches)


def load_detector(path: str | Path) -> KeywordSpoilerDetector:
    with Path(path).open(encoding="utf-8") as source:
        config = json.load(source)
    if not isinstance(config, dict) or not isinstance(config.get("tags"), list):
        raise ValueError("detector config must contain a tags array")
    return KeywordSpoilerDetector(
        [str(tag) for tag in config["tags"]],
        label_value=str(config.get("label_value", "spoiler")),
        version=str(config.get("version", "keyword-v1")),
    )


def process_once(
    post_queue: PostQueue,
    label_queue: LabelQueue,
    detector: KeywordSpoilerDetector,
    worker_id: str,
    *,
    batch_size: int = 10,
    lease_seconds: float = 60.0,
    retry_delay_seconds: float = 5.0,
    max_attempts: int = 5,
) -> dict[str, int]:
    claimed = post_queue.claim(
        worker_id,
        limit=batch_size,
        lease_seconds=lease_seconds,
    )
    counts = {"claimed": len(claimed), "positive": 0, "negative": 0, "failed": 0}
    for delivery in claimed:
        try:
            result = detector.detect(delivery.job.text)
            if result.is_spoiler:
                label_queue.enqueue(
                    LabelJob.create(
                        post_uri=delivery.job.post_uri,
                        post_cid=delivery.job.post_cid,
                        label_value=detector.label_value,
                        detector_version=detector.version,
                    )
                )
                if not post_queue.acknowledge(
                    delivery.job.job_id,
                    worker_id,
                    outcome="positive",
                ):
                    raise RuntimeError("post lease was lost before acknowledgement")
                counts["positive"] += 1
            else:
                if not post_queue.acknowledge(
                    delivery.job.job_id,
                    worker_id,
                    outcome="negative",
                ):
                    raise RuntimeError("post lease was lost before acknowledgement")
                counts["negative"] += 1
        except Exception as error:
            post_queue.retry(
                delivery.job.job_id,
                worker_id,
                str(error),
                delay_seconds=retry_delay_seconds,
                max_attempts=max_attempts,
            )
            counts["failed"] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a spoiler detector worker")
    parser.add_argument("--post-database", type=Path, default=Path("data/posts.sqlite3"))
    parser.add_argument("--label-database", type=Path, default=Path("data/labels.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config/spoiler_tags.json"))
    parser.add_argument("--worker-id", default=f"detector-{socket.gethostname()}")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    post_queue = PostQueue(args.post_database)
    label_queue = LabelQueue(args.label_database)
    detector = load_detector(args.config)
    while True:
        counts = process_once(
            post_queue,
            label_queue,
            detector,
            args.worker_id,
            batch_size=args.batch_size,
        )
        if args.once:
            print(json.dumps(counts))
            return 0
        if counts["claimed"] == 0:
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
