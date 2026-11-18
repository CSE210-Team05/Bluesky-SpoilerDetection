# Repository Agent Guide

This file is the maintained handoff for coding agents and developers working in
this repository. It applies to the entire repository. Update it in the same pull
request whenever commands, architecture, data contracts, quality gates, or
operational assumptions change.

## Read First

Before making changes, read the files relevant to the task:

- `README.md` for setup and the supported runtime path.
- `architecture.md` for component boundaries and delivery guarantees.
- `CONTRIBUTING.md` for commit, pull-request, and validation requirements.
- `documentation/operations.md` for runtime and dashboard semantics.
- `documentation/quality-gates.md` for coverage and performance requirements.
- `config/pipeline.json` and `config/performance.json` for versioned runtime and
  benchmark settings.

Do not assume that generated files, local databases, or an old feature branch
are authoritative. Use the active configuration and public interfaces.

## System Shape

The supported flow is:

```text
Jetstream reader
  -> durable sharded post queue
  -> detector workers
  -> durable sharded label queue
  -> publisher workers
```

`coordinator.py` is the operational entry point. It validates configuration,
rectifies shard layouts, starts child processes, supervises restarts, serves the
local dashboard, and coordinates shutdown.

Component ownership is strict:

- `Bluesky_interface/` owns Jetstream parsing, the durable cursor, stream health,
  post queue schemas, and post queue sharding.
- `spoiler_interface/` owns detector handoff, label queue schemas, label queue
  sharding, and durable positive-result persistence.
- `spoiler_interface/spoiler_core/spoiler_detector.py` owns pure classification
  and the thin detector worker loop.
- `label_publisher/` owns label publication and publisher acknowledgements.
- `metrics/` owns read-only aggregate snapshots and the loopback HTTP gateway.
- `docs/` contains the local live-dashboard assets. It is not the MkDocs source.
- `documentation/` contains the MkDocs project documentation.
- `benchmarks/` contains the versioned end-to-end throughput workload.

Do not issue SQL from consumers that do not own the corresponding schema. Add
behavior to the owning interface and exercise it through that public API.

## Delivery and Data Invariants

- Queue delivery is at least once.
- Post and label job IDs are deterministic. Preserve their identity inputs.
- A positive detection must be inserted durably into the label queue before its
  source post is acknowledged. Reversing that order can lose labels.
- A publication is acknowledged only after the publishing client succeeds.
- Retries and expired leases must not create a second logical job.
- Stream cursor advancement and accepted post insertion are one transaction.
- The post URI, CID, and label value form the label publication identity.
- SQLite databases must be on local disk, not a network filesystem.
- Configured shard counts are authoritative. Startup rectification must preserve
  active/dead jobs, counters, metric buckets, stream state, cursors, and
  published-label idempotency history before switching manifests.
- Schema changes must migrate existing databases and remain compatible with
  rectification from an older generation. Metrics readers use `migrate=False`
  and must never perform schema writes.
- Treat files under `data/` as user/runtime state. Never delete or rewrite them
  as part of a code change or test.

## Classification and Performance Semantics

The keyword detector performs case-insensitive whole-word matching against
`config/spoiler_tags.json`. Do not add an unconditional result or another
shortcut around classification for benchmarks or demos.

Keep these metrics distinct:

- **Ingestion rate**: posts accepted from Jetstream per second.
- **Detector throughput**: all positive and negative classifications completed
  per second. This is the detector capacity signal.
- **Positive-label rate**: posts matching a configured spoiler tag per second.
  This is an output/prevalence signal, not processing capacity.
- **Publish rate**: labels acknowledged by the publisher per second.
- **Detector traversal**: reader acceptance through durable classification.
- **Publisher traversal**: reader acceptance through publication
  acknowledgement. This is the positive path's front-to-back latency.
- **DB claim time**: SQLite lock plus claim-query time. Keep it separate from
  traversal latency.

The original reader-acceptance timestamp must follow positive jobs into the
label queue so publisher traversal includes queue residence, processing,
asynchronous persistence, retries, and final acknowledgement.

`config/performance.json` is the source of truth for the throughput gate. The
benchmark uses real classification and an all-positive workload so every event
exercises the durable label and publisher path. If the workload or threshold
changes, increment `benchmark_version` and document the rationale. Do not lower
the threshold merely to make a noisy run pass; inspect the uploaded JSON and
profile, then rerun once only when the code path is unchanged and runner noise
is supported by evidence.

## Health and Dashboard Semantics

- Source lag comes from Jetstream event timestamps, not queue depth.
- Empty queues mean no downstream backlog. They do not prove source freshness.
- Queue depth and oldest active-job age indicate downstream pressure.
- The live metrics dashboard is loopback-only and served by the coordinator.
  Do not publish runtime data or database contents to GitHub Pages.
- The public GitHub Pages site contains only rendered MkDocs documentation.

When dashboard JavaScript or CSS changes, increment its cache-busting query
version in `docs/index.html` and update the corresponding test assertion.

## Development Commands

Python 3.11 or newer is required. Install all development dependencies with:

```shell
python -m pip install -r requirements.txt
```

Run the complete local gate before opening or updating a pull request:

```shell
python -m pylint coordinator.py Bluesky_interface label_publisher metrics spoiler_interface benchmarks tests
python -m pytest -m "not e2e and not performance" --cov --cov-branch --cov-report=term-missing
python -m pytest tests/performance -m performance -rA
python -m mkdocs build --strict
python -m playwright install chromium
python -m pytest tests/e2e --browser chromium --tracing=retain-on-failure
```

The combined statement/branch coverage gate is 80%. Add behavioral assertions,
not exclusions or superficial execution, when coverage drops.

Useful runtime commands:

```shell
python coordinator.py
python coordinator.py --input event.jsonl
python coordinator.py --max-events 100
```

## Files and Generated Output

- Do not commit `data/`, `site/`, `coverage-html/`, `coverage.xml`,
  `.coverage`, `performance-results/`, or Playwright traces.
- Do not hand-edit `site/`; it is generated by `mkdocs build`.
- Do not open `docs/index.html` directly and expect live values; the coordinator
  serves it and `/metrics` from the same origin.
- Pull-request documentation is reviewed through branch-relative Markdown.
  Merged `main` documentation is deployed by
  `.github/workflows/documentation.yml` to GitHub Pages.

## Change and Review Workflow

- Preserve unrelated working-tree changes.
- Keep changes focused and include tests proportional to failure risk.
- Use Conventional Commits, for example:
  `fix(metrics): distinguish output from detector throughput`.
- Use the same conventional format for pull-request titles.
- In the PR body, include summary, rationale, validation commands/results, and
  any migration or benchmark-version implications.
- Never bypass or weaken the stable `Required quality gate`. It aggregates lint,
  Python 3.11/3.13 coverage tests, strict docs, throughput, and Chromium E2E.
- Add active pull requests to the public project board and place them in
  `In review`; move them to `Done` only after merge.

## Keeping This Guide Current

Update `AGENTS.md` whenever a change affects any of the following:

- component or schema ownership;
- queue delivery, ordering, or idempotency guarantees;
- configuration paths or supported startup commands;
- metric definitions or dashboard labels;
- coverage, performance, documentation, or branch-protection gates;
- generated artifacts, deployment behavior, or required PR metadata.

If code and this guide disagree, treat that as a documentation defect and fix
both in the same pull request.
