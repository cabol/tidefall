# Contributing to Tidefall

Thanks for your interest in contributing.

This guide defines the expected contribution workflow for the `tidefall`
repository.

## Before You Start

1. Read `README.md`.
2. Read the latest section of `CHANGELOG.md`.
3. Browse the [documentation on HexDocs](https://hexdocs.pm/tidefall) to
   understand the buffer types and their conventions.

## Issues

Use the issue tracker for bug reports and feature discussions:

- https://github.com/cabol/tidefall/issues

When opening a bug report, include:

- Elixir and OTP versions
- Tidefall version/branch
- Minimal reproduction steps
- Expected vs actual behavior

## Feature Requests

Feature proposals are welcome.

For non-trivial features, open an issue first before implementing code so
scope and design can be aligned early.

When proposing a feature, include:

- Problem statement.
- Real-world use case.
- Proposed API/behavior.
- Alternatives considered.

## Pull Requests

Open pull requests at:

- https://github.com/cabol/tidefall/pulls

### PR Expectations

1. Branch from `main` and keep changes focused — avoid unrelated commits.
2. Add or update tests with code changes.
3. Update documentation when behavior changes.
4. Do not update `CHANGELOG.md` directly; include release-note context in the
   PR description for maintainers.
5. Reference related issues (for example, `Closes #123`).

### Validation

Run targeted checks during development, and run the full CI command before
requesting review:

```bash
# quick targeted checks
mix test test/path/to/changed_test.exs
mix format --check-formatted
mix credo --strict

# full validation (the canonical gate)
mix test.ci
```

`mix test.ci` runs the complete suite: unused-dependency check, compilation
with warnings as errors, formatting check, Credo (strict), test coverage, and
Dialyzer. Green CI is a requirement, not a courtesy check.

## Commit Message Convention

Commit messages must follow
[Conventional Commits](https://www.conventionalcommits.org/):

```text
type(scope): short summary
```

Use the imperative mood, lowercase, no trailing period, and keep the summary
to 72 characters or fewer. Useful scopes include `queue`, `map`, `partition`,
`options`, `telemetry`, and `workflow`.

Examples:

- `feat(queue): add partition_key routing option`
- `fix(map): preserve version on coalesced writes`
- `docs(readme): document module-based buffers`

## Documentation Conventions

For `@doc`, `@moduledoc`, and `@typedoc`, keep the first paragraph short and
summary-oriented. Add examples when possible, ideally doctest-friendly.

## License

By submitting a contribution, you agree that your work is licensed under the
project's MIT license.
