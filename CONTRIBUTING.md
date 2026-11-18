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
3. Verify that your changes work as expected.
4. Include a description of the changes made.
5. Reference the relevant GitHub issue in the pull request.

For additional details, refer to the [official specification](https://www.conventionalcommits.org/en/v1.0.0/).
