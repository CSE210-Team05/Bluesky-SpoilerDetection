"""Pure spoiler classification and its thin worker loop.

Database transactions, leases, retries, and result writes belong to the
enclosing spoiler interface. Classification receives a logical item and
returns only its ID and the spoiler decision.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import signal
import socket
import time
from typing import Iterable

from Bluesky_interface.post_fetcher_interface import PostQueue
from spoiler_interface.spoiler_interface import (
    PostIdentifier,
    ShardedLabelQueue,
    SpoilerItem,
    SpoilerWorkerInterface,
)


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


def isSpoiler(
    item: SpoilerItem,
    detector: KeywordSpoilerDetector,
) -> tuple[PostIdentifier, bool]:
    """Classify one item without knowing anything about queue transactions."""
    return item.id, detector.detect(item.text).is_spoiler


def process_once(
    worker_interface: SpoilerWorkerInterface,
    detector: KeywordSpoilerDetector,
    *,
    batch_size: int = 10,
) -> dict[str, int]:
    items = worker_interface.get_items(limit=batch_size)
    counts = {"claimed": len(items), "positive": 0, "negative": 0, "failed": 0}
    for item in items:
        try:
            identifier, result = isSpoiler(item, detector)
            if identifier != item.id:
                raise ValueError("detector returned an ID different from its input")
            worker_interface.submit_result(identifier, result)
            if result:
                counts["positive"] += 1
            else:
                counts["negative"] += 1
        except Exception as error:
            worker_interface.submit_error(item.id, error)
            counts["failed"] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a spoiler detector worker")
    parser.add_argument("--post-database", type=Path, default=Path("data/posts.sqlite3"))
    parser.add_argument("--label-database", type=Path, action="append")
    parser.add_argument("--config", type=Path, default=Path("config/spoiler_tags.json"))
    parser.add_argument("--worker-id", default=f"detector-{socket.gethostname()}")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--write-buffer-size", type=int, default=100)
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    post_queue = PostQueue(args.post_database)
    label_databases = args.label_database or [Path("data/labels.sqlite3")]
    label_queue = ShardedLabelQueue(label_databases)
    detector = load_detector(args.config)
    stop_requested = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    stop_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        stop_signals.append(signal.SIGBREAK)
    previous_handlers = {
        signum: signal.signal(signum, request_stop) for signum in stop_signals
    }
    try:
        with SpoilerWorkerInterface(
            post_queue,
            label_queue,
            args.worker_id,
            label_value=detector.label_value,
            detector_version=detector.version,
            max_pending_writes=args.write_buffer_size,
        ) as worker_interface:
            while not stop_requested:
                counts = process_once(
                    worker_interface,
                    detector,
                    batch_size=args.batch_size,
                )
                if args.once:
                    persisted = worker_interface.flush()
                    counts.update(persisted)
                    print(json.dumps(counts))
                    return 0
                if counts["claimed"] == 0:
                    time.sleep(args.poll_seconds)
        return 0
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
