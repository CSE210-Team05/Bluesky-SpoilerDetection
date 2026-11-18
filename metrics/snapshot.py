"""Build the read-only JSON snapshot consumed by the GitHub Pages dashboard."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import time
from typing import Any

from Bluesky_interface.post_fetcher_interface import PostQueue
from spoiler_interface.spoiler_interface import LabelQueue


def load_tag_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as source:
        config = json.load(source)
    tags = config.get("tags")
    if not isinstance(tags, list):
        raise ValueError("tag config must contain a tags array")
    return {
        "label_value": str(config.get("label_value", "spoiler")),
        "detector_version": str(config.get("version", "unknown")),
        "tags": [str(tag) for tag in tags],
    }


def build_snapshot(
    post_queue: PostQueue,
    label_queue: LabelQueue,
    tag_config: str | Path,
    *,
    window_seconds: int = 60,
    caught_up_lag_seconds: float = 10.0,
    now: float | None = None,
) -> dict[str, Any]:
    timestamp = time.time() if now is None else now
    posts = post_queue.metrics(window_seconds=window_seconds, now=timestamp)
    labels = label_queue.metrics(window_seconds=window_seconds, now=timestamp)
    config = load_tag_config(tag_config)
    source_lag = posts["stream_source_lag_seconds"]
    connected = bool(posts["stream_connected"])
    return {
        "generated_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
        "window_seconds": window_seconds,
        "configuration": config,
        "rates": {
            "ingestion_per_second": posts["ingestion_rate_per_second"],
            "label_per_second": posts["label_rate_per_second"],
            "publish_per_second": labels["publish_rate_per_second"],
            "detection_per_second": posts["detection_rate_per_second"],
        },
        "stream": {
            "id": posts["stream_id"],
            "connected": connected,
            "caught_up": (
                connected
                and source_lag is not None
                and float(source_lag) <= caught_up_lag_seconds
            ),
            "caught_up_lag_seconds": caught_up_lag_seconds,
            "reconnect_total": posts["stream_reconnect_total"],
            "received_total": posts["stream_received_total"],
            "invalid_total": posts["stream_invalid_total"],
            "last_event_age_seconds": posts["stream_last_event_age_seconds"],
            "source_lag_seconds": source_lag,
        },
        "queues": {
            "posts": {
                "available": posts["available"],
                "leased": posts["leased"],
                "dead": posts["dead"],
                "oldest_job_age_seconds": posts["oldest_job_age_seconds"],
            },
            "labels": {
                "available": labels["available"],
                "leased": labels["leased"],
                "dead": labels["dead"],
                "oldest_job_age_seconds": labels["oldest_job_age_seconds"],
            },
        },
        "totals": {
            "ingested": posts["ingested_total"],
            "detected": posts["acknowledged_total"],
            "labels_enqueued": labels["enqueued_total"],
            "published": labels["published_total"],
            "detection_retries": posts["retried_total"],
            "publication_retries": labels["retried_total"],
            "dead_lettered": posts["dead_lettered_total"] + labels["dead_lettered_total"],
        },
    }


def write_snapshot(snapshot: dict[str, Any], output: str | Path) -> None:
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        delete=False,
    ) as temporary:
        temporary.write(payload)
        temporary_path = Path(temporary.name)
    temporary_path.replace(destination)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the public dashboard metrics snapshot")
    parser.add_argument("--post-database", type=Path, default=Path("data/posts.sqlite3"))
    parser.add_argument("--label-database", type=Path, default=Path("data/labels.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config/spoiler_tags.json"))
    parser.add_argument("--output", type=Path, default=Path("data/metrics.json"))
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--caught-up-lag-seconds", type=float, default=10.0)
    args = parser.parse_args(argv)

    snapshot = build_snapshot(
        PostQueue(args.post_database),
        LabelQueue(args.label_database),
        args.config,
        window_seconds=args.window_seconds,
        caught_up_lag_seconds=args.caught_up_lag_seconds,
    )
    write_snapshot(snapshot, args.output)
    print(json.dumps({"output": str(args.output), "generated_at": snapshot["generated_at"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
