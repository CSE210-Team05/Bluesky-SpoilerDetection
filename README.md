# Bluesky Spoiler Detection

[Project documentation](documentation/index.md) covers the pipeline contract,
operations, and enforced quality gates. CI also publishes a rendered HTML copy
as the `rendered-documentation` workflow artifact.

First-pass implementation of the queue-based pipeline described in
[`architecture.md`](architecture.md):

```text
reader -> SQLite post queue -> detector workers -> SQLite label queue -> publishers
```

![Bluesky spoiler detection pipeline workflow](Workflow.png)

The live reader rotates across Bluesky's public Jetstream JSON WebSockets and
asks each server for `app.bsky.feed.post` events only. JSON Lines replay remains
available for development, and the publisher is deliberately dry-run-only until
a real AT Protocol labeling client and credentials are configured.

## Try the pipeline locally

Install the Python package and all development/test dependencies:

```shell
python -m pip install -r requirements.txt
```

Create a sample event:

```json
{"kind":"commit","seq":1,"did":"did:plc:example","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"example","cid":"bafyexample","record":{"text":"This contains a spoiler","createdAt":"2026-01-01T00:00:00Z"}}}
```

Save it as `event.jsonl`, then start the entire pipeline with one command:

```shell
python coordinator.py --input event.jsonl
```

The `--input` argument is optional and exists only for replaying a development
JSON Lines file. The normal live command is simply:

```shell
python coordinator.py
```

That command makes the reader connect to the endpoint in
`config/pipeline.json` without requiring a placeholder file value. For a
bounded live smoke test that drains both queues and exits:

```shell
python coordinator.py --max-events 100
```

The coordinator starts the configured reader, detector workers, publisher
workers, and live metrics gateway. When a finite input file reaches EOF, it waits
for both queues to drain and shuts the remaining components down. Without
`--input`, the reader consumes the live Jetstream connection and resumes from
the cursor stored in the post database after a reconnect or restart.

Worker counts, database locations, metrics settings, restart limits, and the
temporary dry-run publisher setting live in `config/pipeline.json`. Stop the
whole process group with `Ctrl-C`; individual failed workers are restarted up
to the configured limit.

## Local metrics dashboard

The dashboard is deliberately local for now. The coordinator serves the page
and its live JSON data from the same loopback-only HTTP server; it does not use
GitHub Pages, a GitHub token, or a Cloudflare tunnel.

Start the coordinator, then open:

<http://127.0.0.1:8765/>

Once the metrics gateway is ready, the coordinator attempts to open that URL
in the default browser. If no browser can be opened, it prints the same URL so
it can be opened manually. Set `metrics.open_browser` to `false` for automated
or headless environments.

The browser polls `/metrics` once per second on that same origin. The dashboard
is reachable only from the machine running the coordinator. Do not open
`docs/index.html` directly from disk: the coordinator is the web server and the
metrics source.

The local directory, bind address, port, and rate window are configured in
`config/pipeline.json`:

```json
"metrics": {
  "dashboard_directory": "docs",
  "host": "127.0.0.1",
  "port": 8765,
  "open_browser": true,
  "live_rate_window_seconds": 10,
  "caught_up_lag_seconds": 10
}
```

Queue sharding is controlled beside the database paths:

```json
"databases": {
  "posts": "data/posts.sqlite3",
  "post_shards": 4,
  "labels": "data/labels.sqlite3",
  "label_shards": 4
}
```

At every coordinator startup, database rectification treats the configured
shard counts as authoritative. If a legacy database exists, or a shard count
changed, the coordinator creates a complete new generation and redistributes
the current backlog by deterministic job ID. Post rectification preserves
cursors and stream health; label rectification preserves the published-label
history used for idempotency. Both preserve aggregate metrics and reset
abandoned leases to pending. The coordinator atomically switches each shard
manifest only after its complete copy succeeds, then deletes the legacy
database or previous shard generation. A failed copy leaves the prior
generation authoritative. Every post shard must have a detector worker and
every label shard must have a publisher worker.

Dashboard throughput uses counter deltas over a short ten-second in-memory
window. The persistent five-minute SQLite buckets remain available for coarse
history, but no longer make the live display take minutes to reflect recovery
or a change in incoming demand.

The **Worker pools** panel shows busy versus configured detector and publisher
workers, utilization, average queue wait for recently claimed jobs, and average
SQLite claim time. The configured count confirms which worker setting the
coordinator actually started. High utilization plus rising queue wait indicates
insufficient processing capacity; rising SQLite claim time as workers are added
indicates database contention instead of useful parallelism. Idle workers show
as configured but not busy, so low utilization with no backlog is normal.

The gateway computes aggregate snapshots on a background sampler. Dashboard
requests read only the last completed in-memory snapshot, so a browser never
touches the queue databases or waits on their locks. If refresh work is delayed,
the last good values remain available and the dashboard shows their age while
retrying with time-bounded, non-overlapping requests.

Reader health is independent of throughput. Jetstream's `time_us` cursor is
stored with stream health, and the dashboard reports **Up to date** when the
reader is connected and the newest source event is no more than ten seconds
behind wall-clock time. A low rate with an up-to-date cursor means current
demand is low; **Catching up** means a known source timestamp is older than the
configured threshold. A newly connected reader with no source event yet reports
**Connected — waiting for first source event** instead. Empty databases and
queues describe downstream work, not source lag, so they do not imply catch-up.
If the WebSocket remains open but delivers no events for 30 seconds, the reader
forces a reconnect from its durable cursor on the next configured Jetstream
instance instead of remaining silently stuck on one unhealthy host.

The OS socket bind rejects a second gateway on the same host and port. This is
the only exclusivity mechanism needed while the dashboard is local-only. A
cleanly stopped gateway permits the address to be reused immediately, so the
coordinator can be restarted without waiting for TCP connections to expire.

## Test

Install the development dependencies, then run the same checks used by GitHub
Actions:

```shell
python -m pylint coordinator.py Bluesky_interface label_publisher metrics spoiler_interface benchmarks tests
python -m pytest -m "not e2e and not performance" --cov --cov-branch --cov-report=term-missing
python -m pytest tests/performance -m performance -rA
python -m mkdocs build --strict
python -m playwright install chromium
python -m pytest tests/e2e --browser chromium --tracing=retain-on-failure
```

The coverage gate fails below the repository threshold and publishes HTML/XML
reports. The performance test drives durable events through both SQLite queues,
enforces `config/performance.json`, and publishes JSON plus `cProfile` output.
MkDocs renders the repository documentation in strict mode. The E2E test
launches the real coordinator with a temporary replay stream and
uses Chromium to verify that an event reaches the rendered dashboard after
ingestion, detection, and dry-run publication. The `Checks` workflow runs lint,
coverage on Python 3.11 and 3.13, documentation, the throughput regression, and
Chromium E2E as separate jobs. Any Pylint finding enabled in `pyproject.toml`
returns a nonzero status and fails the lint job. The stable `Required quality
gate` status aggregates every job for branch protection.

## Current boundaries

- `Bluesky_interface` owns the post database and stream cursor.
- `spoiler_interface` owns the label database.
- Detector workers receive logical items from `SpoilerWorkerInterface`; the
  pure `isSpoiler` function has no database or transaction knowledge.
- Label results are written through a bounded asynchronous sink, with positive
  source jobs acknowledged only after their label is durable.
- Worker leases expire and are redelivered after crashes.
- Queue IDs and label IDs are deterministic, making repeated queue insertion
  idempotent. A live publishing client must also use the job ID as its
  idempotency key where the labeling service supports one.
- The SQLite databases must live on a local disk, not a network filesystem.
- The post queue is deterministically sharded according to
  `databases.post_shards`; changing the count triggers startup rectification.
- The label queue is deterministically sharded according to
  `databases.label_shards`; changing the count rectifies its backlog and
  published-label history before publishers start.
- The detector is currently in throughput mode and returns positive immediately
  for every post. The keyword implementation remains present but bypassed.
- Live Bluesky label publication remains an explicit integration task.
