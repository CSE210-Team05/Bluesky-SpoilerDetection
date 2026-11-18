# Quality gates

The `Checks` workflow makes correctness, documentation, and capacity visible on
every pull request. Its stable `Required quality gate` job succeeds only when
lint, coverage-enforced tests, documentation, performance, and browser E2E all
pass; repository branch protection requires that status before merging.

## Test coverage

Unit and integration tests collect statement and branch coverage for the
application packages. The build fails below the 80% combined line/branch
threshold configured in `pyproject.toml`; HTML and XML reports are uploaded as
workflow artifacts.

Run the same gate locally:

```shell
python -m pytest -m "not e2e and not performance" \
  --cov --cov-branch --cov-report=term-missing --cov-report=html --cov-report=xml
```

Coverage is a regression guard, not a substitute for meaningful assertions.
New failure handling and delivery behavior should be tested directly.

## Throughput requirement

`config/performance.json` is the versioned source of truth for the representative
event count and minimum sustained throughput. The benchmark performs durable
post ingestion, detector handoff, durable label enqueue, publisher claim, and
publisher acknowledgement through the public interfaces. Schema creation is
excluded from timing because it is startup work.

Run the requirement locally:

```shell
python -m pytest tests/performance -m performance -rA
```

To retain machine-readable results and a profile:

```shell
mkdir -p performance-results
PERFORMANCE_RESULTS_DIR=performance-results \
  python -m pytest tests/performance -m performance -rA
python -m pstats performance-results/pipeline.prof
```

CI uploads `pipeline.json`, `pipeline.prof`, and a readable `pipeline.txt`
summary, so a failed or slowing change can be compared by function rather than
diagnosed from wall-clock time alone.
The threshold is intentionally a portable single-worker floor. Raising it or
changing the workload requires incrementing `benchmark_version` and recording
the rationale in the same pull request. Benchmark version 2 restores the real
keyword/label match for every event while retaining an all-positive workload,
so the result includes classification plus the worst-case durable label and
publisher path.

## Render documentation

Documentation source is stored under `documentation/`. CI runs MkDocs in strict
mode and uploads the rendered `site/` directory as a workflow artifact on every
pull request. After documentation changes merge to `main`, the separate
`Documentation site` workflow publishes that same strict build to
[GitHub Pages](https://cse210-team05.github.io/Bluesky-SpoilerDetection/).

```shell
python -m mkdocs build --strict
```
