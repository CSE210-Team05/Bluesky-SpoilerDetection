"""Live Jetstream reader with optional JSON Lines development replay."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from Bluesky_interface.post_fetcher_interface import (
    POST_COLLECTION,
    PostJob,
    PostQueue,
    ShardedPostQueue,
)


def parse_jetstream_event(event: dict[str, Any], stream_id: str) -> tuple[int, list[PostJob]]:
    """Extract a post-create job from a Jetstream v1/v2-style event."""
    raw_sequence = event.get("seq", event.get("time_us"))
    if raw_sequence is None:
        raise ValueError("event has no seq or time_us cursor")
    sequence = int(raw_sequence)

    if event.get("kind") != "commit":
        return sequence, []

    commit = event.get("commit")
    if not isinstance(commit, dict):
        return sequence, []
    operation = commit.get("operation", commit.get("action"))
    collection = commit.get("collection")
    if operation != "create" or collection != POST_COLLECTION:
        return sequence, []

    did = event.get("did")
    rkey = commit.get("rkey")
    cid = commit.get("cid")
    record = commit.get("record")
    if not isinstance(did, str) or not isinstance(rkey, str) or not isinstance(cid, str):
        raise ValueError("post-create event is missing did, rkey, or cid")
    if not isinstance(record, dict) or not isinstance(record.get("text"), str):
        raise ValueError("post-create event is missing record text")

    job = PostJob.create(
        stream_id=stream_id,
        sequence=sequence,
        operation_index=0,
        post_uri=f"at://{did}/{collection}/{rkey}",
        post_cid=cid,
        author_did=did,
        text=record["text"],
        created_at=record.get("createdAt"),
    )
    return sequence, [job]


class PostReader:
    def __init__(self, queue: PostQueue | ShardedPostQueue, stream_id: str):
        self.queue = queue
        self.stream_id = stream_id

    def process_event(self, event: dict[str, Any]) -> int:
        sequence, jobs = parse_jetstream_event(event, self.stream_id)
        raw_time_us = event.get("time_us")
        source_event_at = None if raw_time_us is None else int(raw_time_us) / 1_000_000
        return self.queue.enqueue_and_checkpoint(
            jobs,
            stream_id=self.stream_id,
            sequence=sequence,
            source_event_at=source_event_at,
        )

    def run(self, events: Iterable[dict[str, Any]]) -> int:
        accepted = 0
        for event in events:
            accepted += self.process_event(event)
        return accepted


def build_jetstream_url(
    endpoint: str,
    *,
    collection: str = POST_COLLECTION,
    cursor: int | None = None,
) -> str:
    """Build a legacy Jetstream subscription URL without duplicate filters."""
    parts = urlsplit(endpoint)
    if parts.scheme not in {"ws", "wss"}:
        raise ValueError("Jetstream endpoint must use ws:// or wss://")
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key not in {"wantedCollections", "cursor"}
    ]
    query.append(("wantedCollections", collection))
    if cursor is not None:
        query.append(("cursor", str(cursor)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


async def receive_jetstream_message(websocket: Any, idle_timeout: float) -> str | bytes:
    """Receive one event or force a reconnect when the stream stops advancing."""
    if idle_timeout <= 0:
        raise ValueError("Jetstream idle timeout must be positive")
    try:
        return await asyncio.wait_for(websocket.recv(), timeout=idle_timeout)
    except TimeoutError as error:
        raise RuntimeError(
            f"Jetstream delivered no events for {idle_timeout:g} seconds"
        ) from error


async def run_live_reader(
    reader: PostReader,
    endpoint: str | Sequence[str],
    *,
    collection: str = POST_COLLECTION,
    max_events: int | None = None,
    open_timeout: float = 10.0,
    ping_interval: float = 20.0,
    ping_timeout: float = 20.0,
    idle_timeout: float = 30.0,
    max_message_bytes: int = 4 * 1024 * 1024,
    initial_backoff: float = 1.0,
    maximum_backoff: float = 30.0,
) -> dict[str, int]:
    """Consume live Jetstream JSON, reconnecting from the durable cursor."""
    try:
        from websockets.asyncio.client import connect
    except ImportError as error:
        raise RuntimeError(
            "live ingestion requires the 'websockets' package; install the project dependencies"
        ) from error

    endpoints = (endpoint,) if isinstance(endpoint, str) else tuple(endpoint)
    if not endpoints:
        raise ValueError("at least one Jetstream endpoint is required")
    for configured_endpoint in endpoints:
        build_jetstream_url(configured_endpoint, collection=collection)

    received = 0
    accepted = 0
    invalid = 0
    connections = 0
    endpoint_index = 0
    backoff = initial_backoff

    while max_events is None or received < max_events:
        current_endpoint = endpoints[endpoint_index]
        cursor = reader.queue.get_cursor(reader.stream_id)
        url = build_jetstream_url(
            current_endpoint,
            collection=collection,
            cursor=cursor,
        )
        try:
            async with connect(
                url,
                open_timeout=open_timeout,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                max_size=max_message_bytes,
                max_queue=16,
            ) as websocket:
                reader.queue.set_stream_connected(
                    reader.stream_id,
                    True,
                    reconnect=connections > 0 or cursor is not None,
                )
                connections += 1
                backoff = initial_backoff
                print(
                    json.dumps(
                        {
                            "stream": reader.stream_id,
                            "connected": True,
                            "cursor": cursor,
                            "endpoint": current_endpoint,
                        }
                    ),
                    flush=True,
                )
                while max_events is None or received < max_events:
                    message = await receive_jetstream_message(websocket, idle_timeout)
                    received += 1
                    try:
                        if isinstance(message, bytes):
                            message = message.decode("utf-8")
                        event = json.loads(message)
                        if not isinstance(event, dict):
                            raise ValueError("Jetstream message is not a JSON object")
                        accepted += reader.process_event(event)
                    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                        invalid += 1
                        reader.queue.record_invalid_event(reader.stream_id)
                        print(
                            json.dumps({"stream": reader.stream_id, "invalid_event": str(error)}),
                            file=sys.stderr,
                            flush=True,
                        )
                    if max_events is not None and received >= max_events:
                        return {"received": received, "accepted": accepted, "invalid": invalid}
        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(
                json.dumps(
                    {
                        "stream": reader.stream_id,
                        "endpoint": current_endpoint,
                        "connection_error": f"{type(error).__name__}: {error}",
                        "retry_seconds": backoff,
                        "next_endpoint": endpoints[(endpoint_index + 1) % len(endpoints)],
                    }
                ),
                file=sys.stderr,
                flush=True,
            )
        finally:
            reader.queue.set_stream_connected(reader.stream_id, False)

        endpoint_index = (endpoint_index + 1) % len(endpoints)
        await asyncio.sleep(backoff)
        backoff = min(maximum_backoff, max(initial_backoff, backoff * 2))

    return {"received": received, "accepted": accepted, "invalid": invalid}


def json_line_events(lines: Iterable[str]) -> Iterable[dict[str, Any]]:
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid JSON on line {line_number}: {error}") from error
        if not isinstance(event, dict):
            raise ValueError(f"event on line {line_number} is not an object")
        yield event


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read Jetstream JSON Lines into the post queue")
    parser.add_argument("--database", type=Path, action="append")
    parser.add_argument("--stream-id", default="jetstream-jsonl")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--input", type=Path, help="development JSON Lines replay file")
    source.add_argument(
        "--endpoint",
        action="append",
        help="live Jetstream endpoint; repeat to configure failover hosts",
    )
    parser.add_argument("--collection", default=POST_COLLECTION)
    parser.add_argument("--max-events", type=int, help="exit after receiving this many live messages")
    parser.add_argument("--open-timeout", type=float, default=10.0)
    parser.add_argument("--ping-interval", type=float, default=20.0)
    parser.add_argument("--ping-timeout", type=float, default=20.0)
    parser.add_argument("--idle-timeout", type=float, default=30.0)
    parser.add_argument("--max-message-bytes", type=int, default=4 * 1024 * 1024)
    args = parser.parse_args(argv)

    databases = args.database or [Path("data/posts.sqlite3")]
    queue = ShardedPostQueue(databases)
    reader = PostReader(queue, args.stream_id)
    if args.endpoint:
        result = asyncio.run(
            run_live_reader(
                reader,
                args.endpoint,
                collection=args.collection,
                max_events=args.max_events,
                open_timeout=args.open_timeout,
                ping_interval=args.ping_interval,
                ping_timeout=args.ping_timeout,
                idle_timeout=args.idle_timeout,
                max_message_bytes=args.max_message_bytes,
            )
        )
        print(json.dumps({**result, "cursor": queue.get_cursor(args.stream_id)}))
        return 0
    if args.input:
        with args.input.open(encoding="utf-8") as source:
            accepted = reader.run(json_line_events(source))
    else:
        accepted = reader.run(json_line_events(sys.stdin))
    print(json.dumps({"accepted_posts": accepted, "cursor": queue.get_cursor(args.stream_id)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
