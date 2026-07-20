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

### Cloudflare R2 integration test

The normal suite uses moto and needs no cloud credentials. To additionally
exercise a real R2 bucket, create an R2 S3 API token with object read/write
access and set:

```bash
export RUBEDO_TEST_R2_ACCOUNT_ID="<Cloudflare account id>"
export RUBEDO_TEST_R2_ACCESS_KEY_ID="<R2 access key id>"
export RUBEDO_TEST_R2_SECRET_ACCESS_KEY="<R2 secret access key>"
export RUBEDO_TEST_R2_BUCKET="<existing test bucket>"
uv run pytest tests/test_r2_live.py -q
```

`RUBEDO_TEST_R2_ENDPOINT_URL` may override the default
`https://<account-id>.r2.cloudflarestorage.com`. The test skips when any
required variable is absent. It uses a unique `rubedo-live-tests/<uuid>/`
prefix and removes only the objects it wrote.

## Conventions

- Small, focused commits with explanatory messages over large ones.
- No backwards-compatibility shims — the project is pre-1.0 and dev-stage,
  so changing behavior outright is preferred to adding flags/shims for it.
- Prefer deleting a concept to adding a knob.

## Reporting bugs / proposing features

Open a GitHub issue. Include a minimal repro for bugs; for features, explain
the problem you're hitting before proposing a specific API.
