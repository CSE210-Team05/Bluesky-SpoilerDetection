# Contributing Guidelines

Thank you for contributing to the Bluesky Spoiler Detection project!

To maintain a consistent and readable Git history, all contributors should follow the [Conventional Commits 1.0.0 specification](https://www.conventionalcommits.org/en/v1.0.0/).

## Commit Message Format

All commit messages should follow this structure:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Commit Types

| Type | Description |
|---|---|
| `feat` | Introduces a new feature |
| `fix` | Fixes a bug |
| `docs` | Updates documentation |
| `style` | Changes formatting without affecting functionality |
| `refactor` | Restructures code without changing its behavior |
| `test` | Adds or updates tests |
| `build` | Updates the build system or dependencies |
| `ci` | Updates CI/CD configuration |
| `chore` | General maintenance tasks |
| `perf` | Improves performance |

### Examples

Adding a feature:

```
feat(detector): implement spoiler classification
```

Fixing a bug:

```
fix(firehose): handle websocket disconnections
```

Updating documentation:

```
docs: add local setup instructions
```

Adding tests:

```
test(labeler): add spoiler label validation tests
```

A complete commit with a focused summary and an explanatory body:

```
feat(pipeline): shard local work queues

Rectify existing post and label databases to the configured shard counts
before starting workers. Preserve queued work, metrics, stream state, and
published-label idempotency history while replacing the prior generation.

Serve the metrics dashboard locally and report worker utilization and queue
wait times so database contention is visible.
```

### Breaking Changes

Changes that introduce incompatibilities must be indicated by adding `!` before the colon or by including a `BREAKING CHANGE:` footer.

Example:

```
feat(api)!: change spoiler classification response format
```

### Guidelines

- Use a type that accurately describes the change.
- Keep commit descriptions short and descriptive.
- Use lowercase commit types for consistency.
- Use scopes when they help identify the affected component.
- Separate unrelated changes into different commits when practical.
- Reference related GitHub issues when appropriate.

## Pull Requests

Before submitting a pull request:

1. Ensure your changes address the associated GitHub issue.
2. Follow the Conventional Commits format.
3. Install development dependencies with `python -m pip install -r requirements.txt`.
4. Run the required checks:

   ```shell
   python -m pylint coordinator.py Bluesky_interface label_publisher metrics spoiler_interface tests
   python -m pytest -m "not e2e"
   python -m playwright install chromium
   python -m pytest tests/e2e --browser chromium --tracing=retain-on-failure
   ```

5. Verify that your changes work as expected.
6. Include a description of the changes made.
7. Reference the relevant GitHub issue in the pull request.

The GitHub Actions `Checks` workflow runs lint, unit/integration, and browser
E2E jobs for every pull request. A Pylint finding is a check failure, not a
warning.

For additional details, refer to the [official specification](https://www.conventionalcommits.org/en/v1.0.0/).
