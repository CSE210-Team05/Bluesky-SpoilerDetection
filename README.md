# Bluesky Spoiler Detection

First-pass implementation of the queue-based pipeline described in
[`architecture.md`](architecture.md):

```text
reader -> SQLite post queue -> detector workers -> SQLite label queue -> publishers
```

The live reader connects to Bluesky's public Jetstream JSON WebSocket and asks
the server for `app.bsky.feed.post` events only. JSON Lines replay remains
available for development, and the publisher is deliberately dry-run-only until
a real AT Protocol labeling client and credentials are configured.

## Try the pipeline locally

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
workers, and metrics publisher. When a finite input file reaches EOF, it waits
for both queues to drain and shuts the remaining components down. Without
`--input`, the reader consumes the live Jetstream connection and resumes from
the cursor stored in the post database after a reconnect or restart.

Worker counts, database locations, metrics settings, restart limits, and the
temporary dry-run publisher setting live in `config/pipeline.json`. Stop the
whole process group with `Ctrl-C`; individual failed workers are restarted up
to the configured limit.

Configure GitHub Pages to publish from `docs/`. The dashboard runs entirely on
GitHub Pages, while the pipeline and SQLite databases remain on the pipeline
machine. The deployed JavaScript polls the repository's raw
`docs/metrics.json` file every five seconds and never connects to the pipeline
host.

Every five minutes, the pipeline generates a fresh aggregate snapshot and uses
GitHub's Contents API to update `docs/metrics.json`. This creates a small commit,
but does not redeploy GitHub Pages; the already-deployed dashboard reads the new
raw file directly. There is no
inbound firewall rule, metrics listener, CORS configuration, or reverse proxy
on the pipeline machine.

The repository includes `.github/workflows/deploy-pages.yml`. It validates and
deploys `docs/` only when frontend files change on `main`; metrics-only commits
do not trigger a deployment. In the repository's **Settings → Pages**, select
**GitHub Actions** as the publishing source. You can validate the artifact
locally before pushing:

```shell
bash scripts/prepare-pages.sh
```

If the published site displays this repository README, GitHub Pages is still
using **Deploy from a branch** and publishing the repository root. Change the
Pages source to **GitHub Actions**. A root `index.html` redirect is included as
a fallback, but the Actions source is the intended deployment mode.

Create a fine-grained token limited to this repository with **Contents: write**
permission. Store it only on the pipeline machine:

```shell
export GITHUB_METRICS_TOKEN="..."
```

The token name, repository, branch, destination path, publication interval, and
rate window are configured in `config/pipeline.json`:

```json
"metrics": {
  "repository": "CSE210-Team05/Bluesky-SpoilerDetection",
  "branch": "main",
  "path": "docs/metrics.json",
  "token_env": "GITHUB_METRICS_TOKEN",
  "publish_interval_seconds": 300
}
```

## Test

```shell
python -m pytest
```

## Current boundaries

- `Bluesky_interface` owns the post database and stream cursor.
- `spoiler_interface` owns the label database.
- Worker leases expire and are redelivered after crashes.
- Queue IDs and label IDs are deterministic, making repeated queue insertion
  idempotent. A live publishing client must also use the job ID as its
  idempotency key where the labeling service supports one.
- The SQLite databases must live on a local disk, not a network filesystem.
- The detector is currently in throughput mode and returns positive immediately
  for every post. The keyword implementation remains present but bypassed.
- Live Bluesky label publication remains an explicit integration task.
