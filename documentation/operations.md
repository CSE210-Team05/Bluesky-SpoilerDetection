# Operations

## Install and run

Python 3.11 or newer is required.

```shell
python -m pip install -r requirements.txt
python coordinator.py
```

The live command connects to the configured Jetstream endpoint pool. For a
finite replay, pass a JSON Lines file:

```shell
python coordinator.py --input event.jsonl
```

For a bounded live smoke test, use `--max-events`:

```shell
python coordinator.py --max-events 100
```

## Configuration

Runtime settings live in `config/pipeline.json`. The most consequential
capacity settings are the number of post and label shards, detector workers,
publisher workers, and each worker's batch and polling values.

Changing a shard count triggers startup rectification. Stop all processes and
allow the coordinator to complete this operation; do not edit shard manifests
or move individual SQLite files manually.

## Dashboard and health

The coordinator serves the dashboard and JSON metrics from the same loopback
origin, by default at <http://127.0.0.1:8765/>. The gateway intentionally
rejects non-loopback binds. `/health` reports gateway readiness and `/metrics`
returns the last completed aggregate snapshot.

An **Up to date** stream has an active connection and a recent source cursor.
**Catching up** means events are flowing but their source timestamps lag wall
clock time. A low rate while up to date can simply mean low incoming demand.
Rising queue age or depth means a downstream stage is not keeping pace.

The dashboard separates classifier throughput from spoiler prevalence.
**Detection** counts every post classified per second, while **Positive labels**
counts only posts matching a configured tag. A positive-label rate below one is
normal when matching posts are rare; compare detection with ingestion and queue
age to assess whether the pipeline is keeping up.

Use `Ctrl-C` for coordinated shutdown. A finite replay exits automatically
after the reader reaches EOF and both queues drain.
