# Bluesky Spoiler Detection

This project consumes Bluesky post-create events, durably queues them, detects
spoilers, and publishes CID-scoped labels. It is designed as a queue-based,
multi-process pipeline whose capacity and health can be measured locally.

The documentation covers the supported operational path and the checks that
protect it:

- [Pipeline](pipeline.md) describes component boundaries and delivery guarantees.
- [Operations](operations.md) explains local startup, configuration, and metrics.
- [Quality gates](quality-gates.md) documents coverage and throughput requirements.

The full design rationale remains in
[`architecture.md`](https://github.com/CSE210-Team05/Bluesky-SpoilerDetection/blob/main/architecture.md),
and contributor workflow is in
[`CONTRIBUTING.md`](https://github.com/CSE210-Team05/Bluesky-SpoilerDetection/blob/main/CONTRIBUTING.md).

## Data flow

```text
Bluesky Jetstream
        |
        v
 durable post queue -> detector workers -> durable label queue -> publisher workers
        |                    |                    |                  |
        `--------------------+--------------------+------------------'
                                     |
                              metrics snapshot
                                     |
                              local dashboard
```

Every accepted event is checkpointed with its durable post insertion. Positive
detections are acknowledged only after the deterministic label job is durable,
and labels are acknowledged only after publication succeeds.
