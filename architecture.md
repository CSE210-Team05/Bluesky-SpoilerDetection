# Bluesky Spoiler Detection Architecture

## Overview

The system reads posts from the Bluesky firehose, detects whether they contain
spoilers, and publishes labels for posts that are classified as spoilers.

Each major capability is exposed through an interface at a higher directory
level than its core implementation. The core implementation is an internal
detail: its behavior comes through the enclosing interface, and other parts of
the system must use that interface rather than access the core directly.

```text
Coordinator
    |
    +-- starts and supervises the reader
    +-- starts and supervises detector workers
    +-- starts and supervises publisher workers
    `-- starts and supervises metrics

Bluesky event stream
        |
        v
One active reader
        |
        v
Post queue
        |
        v
[Spoiler detector workers]
        |
        v
Label queue
        |
        v
[Label publisher workers]
        |
        v
Bluesky labeling service

Pipeline machine
----------------
Local dashboard --same-origin HTTP--> metrics gateway --read--> snapshots
```

Square brackets denote horizontally scalable worker pools. Detector and
publisher workers are distributed across their respective SQLite queue shards.

Each pipeline stage also reports operational metrics. A lightweight local
dashboard displays the current spoiler tags and pipeline health so bottlenecks
can be identified without accessing worker processes directly. The coordinator
serves both the static dashboard and aggregate metrics from one loopback-only
origin. SQLite databases and pipeline control operations remain private.

## Directory Structure

```text
.
|-- architecture.md
|-- coordinator.py
|-- config/
|   |-- pipeline.json
|   `-- spoiler_tags.json
|-- Bluesky_interface/
|   |-- __init__.py
|   |-- post_fetcher_interface.py
|   `-- post_fetcher_core/
|       `-- post_fetcher.py
|-- spoiler_interface/
|   |-- __init__.py
|   |-- spoiler_interface.py
|   `-- spoiler_core/
|       `-- spoiler_detector.py
|-- label_publisher/
|   `-- publisher.py
|-- metrics/
|   |-- gateway.py
|   `-- snapshot.py
`-- docs/
    |-- index.html
    |-- app.js
    `-- styles.css
```

The `*_core` directories are private implementation areas. Code outside an
interface directory must not import from its core directory.

## Components

### Coordinator

`coordinator.py` is the single operational entry point for the pipeline. It
reads `config/pipeline.json` and starts the reader, configured detector pool,
configured publisher pool, and metrics gateway as independent child
processes.

The coordinator starts the metrics gateway first and waits for its initial
snapshot before starting any pipeline worker. It monitors all child processes,
restarts failed detector and publisher
processes up to a configured limit, and shuts every component down together on
`SIGINT` or `SIGTERM`. The gateway and reader are critical singletons: if
either stops, the entire pipeline stops rather than silently starting a
competing owner.

When a finite reader input ends successfully, the coordinator waits until both
SQLite queues are drained and then performs a graceful shutdown. Configuration
controls worker counts, database paths, polling intervals, metrics exposure,
and supervision timeouts; individual components do not need to be started by
an operator.

### Bluesky Interface

`Bluesky_interface/post_fetcher_interface.py` is the boundary between fetched
Bluesky data and the rest of the application. It is responsible for placing
accepted post-create events onto the durable post queue without exposing the
reader or queue implementation.

`Bluesky_interface/post_fetcher_core/post_fetcher.py` runs as a standalone
process. One active reader consumes the Bluesky event stream. Standby instances
may exist for availability, but only one instance owns the active stream cursor
at a time.

The initial live transport connects to the configured public Jetstream
WebSocket and requests only `app.bsky.feed.post`. On disconnection it rebuilds
the subscription URL using the last cursor committed in the post database and
reconnects with bounded exponential backoff. A JSON Lines file may replace the
live transport explicitly for development replay; it is never required for
normal startup.

The reader keeps only `create` operations for the `app.bsky.feed.post`
collection. It ignores updates, deletes, and all other collections and event
types. It durably enqueues each accepted post before advancing its stored
stream cursor.

Transport pings alone do not prove that the application stream is advancing.
If an open Jetstream WebSocket delivers no events within the configured idle
timeout, the reader marks it disconnected and reconnects from the durable
cursor on the next configured public instance. Connection errors rotate through
the same endpoint pool. This turns a silent upstream stall into an observable
recovery cycle and prevents one unhealthy Jetstream host from stopping the
pipeline.

The reader must remain lightweight: read, filter, extract the post and its
identifiers, enqueue it, and continue. Detection never runs in the reader.

### Post Queue

The post queue separates stream ingestion from spoiler detection. It is a work
queue: each post job is handled by one detector worker rather than broadcast to
every detector. It provides buffering and backpressure when detection is slower
than ingestion.

Jobs remain available until acknowledged so detector workers can be restarted
or scaled without losing accepted posts.

The implementation uses a configurable set of local SQLite shards owned by the
Bluesky interface. The interface owns their schemas, migrations, routing, and
queue operations; other components must not issue SQL against them directly.
The reader routes each job by a stable hash of its deterministic job ID.
Detector workers are assigned across shards, with at least one detector per
shard, so independent shards can sustain concurrent SQLite writers.

Before any worker starts, the coordinator rectifies the on-disk layout to the
configured shard count. It copies active and dead jobs, aggregate counters,
metric buckets, stream cursor, and stream health into a new generation. Leases
from the stopped generation become pending work. A manifest switch makes the
new generation authoritative only after every shard commits successfully;
legacy files and the previous generation are deleted afterward. Changing the
configured count therefore reshards the existing backlog before processing
continues rather than creating an empty parallel queue.

SQLite runs in WAL mode on a local disk. Queue operations use short
transactions, a busy timeout, indexed job states, and expiring worker leases.
The reader inserts accepted post jobs and advances its stored stream cursor in
the same transaction so it cannot checkpoint past work that was not durably
queued.

### Spoiler Interface

`spoiler_interface/spoiler_interface.py` is the only public boundary for
spoiler detection and its results. It is responsible for:

- atomically claiming post jobs and exposing them as logical detector inputs;
- tracking source leases without exposing transactions to detector code;
- asynchronously writing positive results to the durable label queue; and
- acknowledging or retrying source posts after each asynchronous write.

The interface is a library instantiated inside every detector worker, not a
load balancer or separately deployed service. Each worker has an independent
adapter and asynchronous result writer. SQLite arbitrates competing claims.

`spoiler_interface/spoiler_core/spoiler_detector.py` contains the detection
logic as `isSpoiler(item) -> (id, bool)`. It has no queue, transaction, retry,
or label-database responsibilities. Multiple detector worker processes may
classify in parallel. Their bounded in-process result buffers let
classification continue while a background writer performs database I/O. A
negative result is acknowledged and discarded. A positive result is
acknowledged only after its label job has been durably enqueued. A full result
buffer applies backpressure instead of allowing unbounded memory or leases.

The first-pass implementation runs in throughput mode: detection returns
positive immediately for every post so queue and publisher capacity can be
measured without classification cost. Real spoiler logic will replace this
early return later.

The term *detector* is used here instead of *labeler* to distinguish content
classification from the AT Protocol labeling service that publishes labels.

### Label Queue

The label queue contains only posts classified as spoilers. Each job includes,
at minimum, the post's AT URI, its CID, and the spoiler label value. It buffers
publisher outages or rate limits without blocking ingestion or detection.

The implementation uses a configurable set of local SQLite shards owned by the
spoiler interface. The interface owns their schemas, migrations, deterministic
routing, and queue operations. Each publisher is assigned to one shard, with at
least one publisher per shard, while detector output is routed by the stable
hash of the deterministic label job ID.

Before workers start, the coordinator rectifies this queue to
`databases.label_shards` using the same generation-and-manifest process as the
post queue. Rectification redistributes active and dead jobs, resets abandoned
leases, and preserves aggregate metrics and `published_labels` idempotency
history. The old database or generation is deleted only after the new manifest
becomes authoritative.

The two queue databases intentionally have separate owners. A detector cannot
atomically commit across both databases, so a positive result is handed off in
this order:

1. Insert the deterministic label job into the label queue.
2. After that insert commits, acknowledge the source post job.

If the detector fails between those steps, the post may be detected again, but
the label queue's unique job key prevents a second logical label job. The order
must never be reversed because acknowledging first could lose a positive
result. Negative results require only acknowledgement of the post job.

### Label Publisher

`label_publisher/publisher.py` runs as a standalone worker process. Its only job
is to consume label jobs and publish the corresponding Bluesky labels. The
coordinator assigns publisher workers round-robin across label shards; adding
workers gives each shard more competing consumers when measured queue lag calls
for more capacity.

Labels are published with both the post AT URI and CID. The CID scopes the label
to the exact post version that was classified. If the post is later updated,
the new version has a different CID and the old label no longer applies. No
explicit unpublish operation is needed. Deleted posts likewise require no
action.

The publisher acknowledges a job only after the publish operation succeeds. It
must be idempotent because a crash or timeout can cause a successfully
published job to be delivered again.

### Metrics and Dashboard

The reader, detector workers, queues, and publisher workers emit metrics to a
central collector. Metrics reporting must not block or otherwise participate in
the main processing path; a metrics outage must not stop ingestion, detection,
or publication.

The collector creates small snapshots containing no credentials, post contents,
user identifiers, or administrative controls. A dedicated background sampler
updates an in-memory aggregate snapshot; HTTP request threads only read the
last completed snapshot and never query SQLite or acquire a queue lock. A slow
or failed refresh therefore leaves the last good snapshot immediately
available, together with its age and refresh status. A coordinator-managed HTTP
gateway serves both that snapshot and the dashboard assets from a loopback port.
The dashboard polls the same origin once per second. Browser requests are
single-flight and time-bounded; a local watchdog visibly reports delayed or
stale telemetry while reconnecting. Metric values are never written into the
dashboard assets.

The gateway calculates displayed throughput from lifetime-counter deltas over
a short in-memory window. This keeps the dashboard responsive without adding
per-second metric rows or writes to either SQLite queue database. Persistent
minute buckets remain a coarse historical record rather than the live rate
source.

Metrics readers open the existing SQLite schemas without running migrations or
other schema statements. Schema initialization remains a coordinator startup
responsibility, preventing dashboard and diagnostic queries from contending
with live workers for schema locks.

Queue depth is derived from constant-time lifetime counters, while leased-job
counts and oldest active jobs use dedicated indexes. Metrics collection must
not repeatedly scan a backlog-sized jobs table, because observability work must
not compete with ingestion and publication for SQLite I/O.

Throughput is not itself a reader-health signal. Jetstream's source event
timestamp is checkpointed with stream health, allowing the dashboard to show
`Up to date`, `Catching up`, or `Disconnected` from measured cursor lag. A
connected reader without a timestamped source event reports `Waiting for first
source event` because its lag is not measurable yet. Queue depth is not used as
a proxy for source lag. Thus a low arrival rate can be distinguished from a
reader that is falling behind.

The local HTTP bind is exclusive, so a second coordinator cannot claim the same
dashboard address. The server enables address reuse only so a cleanly stopped
gateway can restart while old TCP connections are in `TIME_WAIT`; an active
listener still owns the port exclusively. There is no cross-machine lease
because the dashboard is not published or shared across machines.

The dashboard displays:

- the current tags or label values that classify a post as a spoiler;
- configured and currently busy detector and publisher workers;
- worker utilization, average queue wait, and average SQLite claim time;
- ingestion rate: accepted post-create jobs enqueued per unit of time;
- label rate: positive spoiler classifications enqueued per unit of time;
- publish rate: labels successfully published per unit of time;
- post queue depth and age of the oldest post job;
- label queue depth and age of the oldest label job;
- detection and publication failure or retry rates; and
- the timestamp of the most recent metrics update.

Rates alone show throughput, while queue depth and oldest-job age reveal where
work is accumulating. The dashboard remains informational and cannot change
the active spoiler tags or any runtime configuration.

## Data Flow

1. The active reader consumes the Bluesky event stream.
2. It discards everything except new `app.bsky.feed.post` records.
3. It writes each accepted post to the post queue and then advances its cursor.
4. One detector worker claims and classifies each post job.
5. Negative results are acknowledged and discarded.
6. Positive results are written to the label queue with the post's URI and CID.
7. A publisher claims each label job and publishes the CID-scoped label.
8. The publisher acknowledges the job after publication succeeds.
9. Every stage asynchronously reports counters, errors, and queue health to the
   metrics collector.
10. The metrics gateway serves the local dashboard and aggregate snapshots on
    one HTTP origin.
11. The dashboard polls live values from `/metrics` on that origin.

## Architectural Rules

- The coordinator is the single operational entry point and owns child-process
  lifecycle; operators do not start pipeline components individually.
- An interface module is the public entry point for its capability.
- Core modules remain below their interface and are not accessed directly by
  other components.
- Cross-component communication goes through the relevant interface.
- The reader, detectors, and publishers run as independent processes.
- Exactly one reader is active for a given event stream cursor.
- Detector and publisher pools use competing consumers, not broadcast
  subscriptions.
- Queue delivery is assumed to be at least once; consumers must therefore be
  idempotent.
- Each interface owns its SQLite schema and is the only supported route for
  accessing that database.
- Post jobs are routed deterministically across the configured SQLite shards;
  each shard must have at least one detector worker.
- SQLite databases reside on local disk and are not shared over a network
  filesystem.
- Database ownership does not require a single interface process; worker
  processes may hold their own connections through the owning interface
  adapter.
- Updates and deletes are intentionally outside the processing pipeline.
- Metrics collection must never block the processing pipeline.
- The metrics snapshot contains aggregate operational data only and
  exposes no secrets, post contents, user identifiers, or write operations.
- The local dashboard may read only the aggregate `/metrics` endpoint. SQLite
  files and coordinator control interfaces remain private, and the HTTP server
  listens only on loopback by default.
- Buffering, storage, and transport details stay behind the interfaces so they
  can change without affecting consumers.
- Data passed across a boundary should use an explicit, shared contract rather
  than implementation-specific objects.

## Decisions Still Required

- Define the complete post-job and label-job schemas. The label job must include
  the post AT URI, CID, and label value.
- Define cursor storage and active-reader failover or leader election.
- Define retry limits and dead-letter handling for detection and publication.
- Choose the persistent idempotency key for publication, likely the combination
  of post URI, CID, and label value.
- Define how completed or failed label publications are recorded.
- Establish measured SQLite queue-depth, lock-wait, and write-latency thresholds
  that would trigger migration to an external queue broker.
