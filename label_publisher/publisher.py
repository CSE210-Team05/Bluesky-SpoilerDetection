"""Label publisher worker.

The worker depends on a small client protocol so the eventual AT Protocol
labeling client can be added without changing queue behavior.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import socket
import time
from typing import Protocol

from spoiler_interface.spoiler_interface import LabelJob, LabelQueue


class LabelClient(Protocol):
    def publish(self, job: LabelJob) -> None: ...


@dataclass
class DryRunLabelClient:
    """Constant-memory development client that doesn't publish externally."""

    published_count: int = 0

    def publish(self, job: LabelJob) -> None:
        self.published_count += 1


def process_once(
    queue: LabelQueue,
    client: LabelClient,
    worker_id: str,
    *,
    batch_size: int = 10,
    lease_seconds: float = 60.0,
    retry_delay_seconds: float = 5.0,
    max_attempts: int = 5,
) -> dict[str, int]:
    claimed = queue.claim(worker_id, limit=batch_size, lease_seconds=lease_seconds)
    counts = {"claimed": len(claimed), "published": 0, "failed": 0}
    for delivery in claimed:
        try:
            client.publish(delivery.job)
            if not queue.acknowledge(delivery.job.job_id, worker_id):
                raise RuntimeError("label lease was lost before acknowledgement")
            counts["published"] += 1
        except Exception as error:
            queue.retry(
                delivery.job.job_id,
                worker_id,
                str(error),
                delay_seconds=retry_delay_seconds,
                max_attempts=max_attempts,
            )
            counts["failed"] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a label publisher worker")
    parser.add_argument("--database", type=Path, default=Path("data/labels.sqlite3"))
    parser.add_argument("--worker-id", default=f"publisher-{socket.gethostname()}")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print and acknowledge labels without contacting Bluesky",
    )
    args = parser.parse_args(argv)

    if not args.dry_run:
        parser.error("no live labeling client is configured; pass --dry-run for local verification")

    queue = LabelQueue(args.database)
    client = DryRunLabelClient()
    while True:
        counts = process_once(queue, client, args.worker_id, batch_size=args.batch_size)
        if args.once:
            print(json.dumps(counts))
            return 0
        if counts["claimed"] == 0:
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
