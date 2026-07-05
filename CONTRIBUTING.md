# Contributing to Rubedo

Rubedo is early and the API is unstable. Small fixes and discussion are
welcome; large features should start as an issue or discussion before any
code is written — see `notes/TODO.md` for the current state of design
decisions and open work.

## Setup

```bash
uv sync
```

## Before opening a PR

```bash
uv run pytest -q                              # tests pass, no new warnings
uv run ruff check src/rubedo/ tests/ examples/    # lint
(cd web && npx tsc -b)                        # only if web/ changed
```

Also do a live end-to-end check of whatever you changed (an example script,
or a small inline repro) — passing tests isn't the same as the feature
actually working.

## Conventions

- Small, focused commits with explanatory messages over large ones.
- No backwards-compatibility shims — the project is pre-1.0 and dev-stage,
  so changing behavior outright is preferred to adding flags/shims for it.
- Prefer deleting a concept to adding a knob.

## Reporting bugs / proposing features

Open a GitHub issue. Include a minimal repro for bugs; for features, explain
the problem you're hitting before proposing a specific API.
