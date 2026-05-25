# Agent Workflow

This file covers the contribution workflow: session bootstrap, PR
flow, commit format, validation.

## Session Bootstrap

At the start of each session, quickly establish context. Read
the project's rule files (per `AGENTS.md`) along with these
context reads:

1. Run `git status --short` and `git diff --name-only` to check
   local modifications and currently touched files.
2. Run `git log --oneline -20` to see recent changes.
3. Run `git branch -a` to see active branches and current branch.
4. Read `README.md` and the latest section of `CHANGELOG.md`
   (project context, not rules — for direction and recent
   user-visible changes).
5. Check the `elixir` version in `mix.exs` for supported
   Elixir/OTP versions.

If on a feature branch, also run:

6. `git log --oneline main..HEAD` to see the branch's commits.
7. `git diff main...HEAD` to understand the branch's full scope.

When relevant to the task:

8. Check open issues and PRs with `gh issue list` and `gh pr list`.
   If `gh` is unavailable or unauthenticated, skip this step.

## Current Project Status

- **Latest release**: check the latest section in `CHANGELOG.md`.
- Read `CHANGELOG.md` for recent features, breaking changes, and
  the project's direction.
- When summarizing changes for the PR description, distinguish
  user-visible behavior from internal refactors — only the former
  typically warrants release-note context.

## PR Workflow

### Reviewing PRs

1. Read the PR description and all comments:
   `gh pr view <number>` and `gh pr view <number> --comments`.
2. Review the diff: `gh pr diff <number>`.
3. Check `CHANGELOG.md` to understand if the change aligns with the
   project's direction.
4. Verify code follows `usage-rules/` conventions (architectural
   non-negotiables first, then Tidefall-specific rules, Elixir
   patterns, and style guidelines).
5. Rely on green CI for the canonical gate; re-run `mix test.ci`
   locally only if you doubt CI's result. Use the fast-iteration
   commands (see below) for spot checks.
6. Provide constructive feedback referencing specific lines and
   conventions.
7. Structure review feedback as:
   - findings first (ordered by severity, with file:line references),
   - open questions/assumptions,
   - brief summary last.

### Opening PRs

1. Branch from `main` with a descriptive branch name
   (e.g., `fix/some-bug`, `feat/new-feature`).
2. Do not update `CHANGELOG.md` directly. Include release-note
   context in the PR description for the maintainer to fold into
   the next release.
3. Run `mix test.ci` before pushing (canonical gate; see below).
4. Reference related GitHub issues in the PR description
   (e.g., "Closes #123").
5. Use `gh pr create` with a clear title and description.

### PR Description Format

Use this structure for the PR body. Keep each section short — the
diff carries the detail; the description carries the *why*.

```text
## Summary
<1-3 bullets — what changed and the motivation>

## Behavior changes
<user-visible changes, if any; release-note context for the
maintainer. Omit this section if the change is internal-only.>

## Test plan
<bulleted checklist of how this was tested>

Closes #<issue> (if applicable)
```

## Commit Messages

Commit messages must follow the
[Conventional Commits](https://www.conventionalcommits.org/) format:

```text
type(scope): short summary
```

### Allowed Types

- `feat`
- `fix`
- `refactor`
- `docs`
- `test`
- `chore`
- `perf`
- `ci`
- `build`

### Rules

1. Use imperative mood in the summary.
2. Keep the summary lowercase and do not end it with a period.
3. Use a scope when it adds clarity (e.g., `queue`, `map`,
   `partition`, `options`, `telemetry`, `workflow`).
4. Keep the first line concise (ideally <= 72 chars).

### Examples

- `feat(map): add conditional delete with version check`
- `fix(partition): handle ets table ownership on timeout`
- `chore(workflow): refine session bootstrap steps`

## Validation Commands

Use these for fast iteration during development:

```bash
# Targeted test
mix test path/to/changed_test.exs

# Format check
mix format --check-formatted

# Static analysis
mix credo --strict

# Documentation (if docs were changed)
mix docs
```

Before pushing for review, the canonical gate is `mix test.ci`. It
runs `deps.unlock --check-unused`, `compile --warnings-as-errors`,
`format --check-formatted`, `credo --strict`, `coveralls.html`, and
`dialyzer --format short`. Green CI is a requirement, not a
courtesy check (see `usage-rules/architecture.md` Non-Negotiable #5).

```bash
mix test.ci
```
