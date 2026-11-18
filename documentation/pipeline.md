# Pipeline

## Component ownership

| Component | Responsibility | Durable state |
| --- | --- | --- |
| Coordinator | Starts, supervises, and stops the complete process group | None |
| Bluesky interface | Filters Jetstream events, checkpoints cursors, and owns post queues | Post SQLite shards |
| Spoiler interface | Claims posts and durably hands positive results to label queues | Label SQLite shards |
| Detector | Pure classification plus a thin worker loop | None |
| Publisher | Publishes CID-scoped labels and acknowledges successes | Publication history in label shards |
| Metrics gateway | Samples aggregate queue state and serves the local dashboard | In-memory snapshot |

The interface modules own their schemas. Other components use those interfaces
instead of issuing SQL against their databases.

## Delivery semantics

Queue delivery is at least once. Jobs have deterministic identifiers so a
crash after a downstream write but before an upstream acknowledgement cannot
create a second logical job.

For a positive detection the handoff order is:

1. Insert the deterministic label job.
2. Commit the label transaction.
3. Acknowledge the leased post job.

Reversing steps 1 and 3 could lose a label. Repeating them is safe because the
label queue has a uniqueness constraint over the label identity.

## Sharding

`databases.post_shards` and `databases.label_shards` define the authoritative
local queue layout. At coordinator startup, rectification creates a complete
new generation, redistributes active work by stable job ID, and switches the
manifest only after every destination shard commits. Each shard must have at
least one corresponding worker.

SQLite databases must be on a local disk. WAL mode, short transactions,
claimable-job indexes, and bounded worker buffers reduce contention, but the
throughput gate should be consulted before raising production demand.
