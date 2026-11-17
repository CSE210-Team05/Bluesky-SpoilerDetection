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

Pipeline machine               GitHub repository        GitHub Pages
----------------               -----------------        ------------
All pipeline stages
        |
        v
Metrics snapshot publisher -> docs/metrics.json -> Static dashboard
```

Square brackets denote a horizontally scalable worker pool. The detector pool
is expected to scale with demand. A single publisher may be sufficient at
first; more publishers can consume from the same queue if publishing becomes a
bottleneck.

Each pipeline stage also reports operational metrics. A lightweight, static
GitHub Pages dashboard displays the current spoiler tags and pipeline health so
bottlenecks can be identified without accessing the worker processes directly.
The dashboard and pipeline run on separate machines. The pipeline periodically
pushes an aggregate snapshot to GitHub, and the dashboard reads that static
same-origin file. GitHub Pages never connects to the pipeline machine; SQLite
databases and pipeline control operations remain private to the pipeline host.

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
|   |-- publisher.py
|   `-- snapshot.py
`-- docs/
    |-- index.html
    |-- app.js
    |-- styles.css
    `-- metrics.json
```

The `*_core` directories are private implementation areas. Code outside an
interface directory must not import from its core directory.

## Components

### Coordinator

`coordinator.py` is the single operational entry point for the pipeline. It
reads `config/pipeline.json` and starts the reader, configured detector pool,
configured publisher pool, and metrics publisher as independent child
processes.

The coordinator monitors all child processes, restarts failed detector,
publisher, or metrics processes up to a configured limit, and shuts every
component down together on `SIGINT` or `SIGTERM`. The reader remains a single
active process. An unexpected reader failure stops the pipeline rather than
silently starting a competing reader.

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

The reader must remain lightweight: read, filter, extract the post and its
identifiers, enqueue it, and continue. Detection never runs in the reader.

### Post Queue

The post queue separates stream ingestion from spoiler detection. It is a work
queue: each post job is handled by one detector worker rather than broadcast to
every detector. It provides buffering and backpressure when detection is slower
than ingestion.

Jobs remain available until acknowledged so detector workers can be restarted
or scaled without losing accepted posts.

The initial implementation uses a local SQLite database owned by the Bluesky
interface. The interface owns its schema, migrations, and queue operations;
other components must not issue SQL against it directly. The reader and
detector processes use the interface adapter, with each process maintaining its
own short-lived or pooled SQLite connection.

SQLite runs in WAL mode on a local disk. Queue operations use short
transactions, a busy timeout, indexed job states, and expiring worker leases.
The reader inserts accepted post jobs and advances its stored stream cursor in
the same transaction so it cannot checkpoint past work that was not durably
queued.

### Spoiler Interface

`spoiler_interface/spoiler_interface.py` is the only public boundary for
spoiler detection and its results. It is responsible for:

- consuming post jobs from the post queue;
- supplying each post to the spoiler detector core;
- acknowledging posts that have been successfully classified; and
- placing positive results onto the durable label queue.

`spoiler_interface/spoiler_core/spoiler_detector.py` contains the detection
logic. Multiple detector worker processes may consume from the post queue in
parallel. A negative result is acknowledged and discarded. A positive result
is acknowledged only after its label job has been durably enqueued.

The term *detector* is used here instead of *labeler* to distinguish content
classification from the AT Protocol labeling service that publishes labels.

### Label Queue

The label queue contains only posts classified as spoilers. Each job includes,
at minimum, the post's AT URI, its CID, and the spoiler label value. It buffers
publisher outages or rate limits without blocking ingestion or detection.

The initial implementation uses a second local SQLite database owned by the
spoiler interface. The spoiler interface owns its schema, migrations, and queue
operations. Publishers access it only through that interface, using independent
SQLite connections and short lease-based transactions.

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
is to consume label jobs and publish the corresponding Bluesky labels. One
publisher should be used initially. Additional publisher workers may share the
queue if measured queue lag shows that one cannot keep up.

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

The collector periodically creates a small snapshot containing no credentials,
post contents, user identifiers, or administrative controls. A dedicated
publisher sends that snapshot to `docs/metrics.json` through GitHub's Contents
API. The token stays in an environment variable on the pipeline machine and is
restricted to repository Contents write access. GitHub Pages serves the
snapshot as a same-origin static file, so no inbound service is exposed by the
pipeline host.

The dashboard displays:

- the current tags or label values that classify a post as a spoiler;
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
10. The metrics publisher periodically commits the aggregate snapshot to
    GitHub.
11. The GitHub Pages dashboard reads and displays the deployed static snapshot.

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
- SQLite databases reside on local disk and are not shared over a network
  filesystem.
- Database ownership does not require a single interface process; worker
  processes may hold their own connections through the owning interface
  adapter.
- Updates and deletes are intentionally outside the processing pipeline.
- Metrics collection must never block the processing pipeline.
- The public metrics snapshot contains aggregate operational data only and
  exposes no secrets, post contents, user identifiers, or write operations.
- GitHub Pages never connects to the pipeline host. The pipeline pushes only
  aggregate metrics to GitHub, while SQLite files and coordinator interfaces
  remain private.
- The GitHub write token exists only on the pipeline machine, is loaded from an
  environment variable, and is limited to repository Contents write access.
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
- Provision the fine-grained GitHub token on the pipeline machine.
- Revisit the five-minute publication interval if commit volume or Pages build
  latency makes the dashboard impractical.
- Establish measured SQLite queue-depth, lock-wait, and write-latency thresholds
  that would trigger migration to an external queue broker.
