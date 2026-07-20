# Cloud storage

Rubedo can put both spilled outputs and Arrow lane history in an
S3-compatible bucket. AWS S3, Cloudflare R2, Backblaze B2, and MinIO use
the same backend; provider names are configuration, not engine concepts.

## Configure a cloud home

Install the S3 extra and provide credentials through the standard AWS
credential chain:

```bash
pip install "rubedo[s3]"

export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export RUBEDO_STORE_URL="s3://my-bucket/rubedo"
```

For Cloudflare R2, include its endpoint and `auto` region. URL-encode the
endpoint query value:

```bash
export RUBEDO_STORE_URL='s3://my-bucket/rubedo?endpoint_url=https%3A%2F%2F<account-id>.r2.cloudflarestorage.com&region=auto'
```

`Home.default()` reads that variable. Explicit construction takes
precedence:

```python
from rubedo import Home

home = Home(
    ".rubedo",
    store_url=(
        "s3://my-bucket/rubedo"
        "?endpoint_url=https%3A%2F%2F<account-id>.r2.cloudflarestorage.com"
        "&region=auto"
    ),
)
```

Keep a distinct prefix per Rubedo deployment. Under it, Rubedo writes:

- `objects/…` — content-addressed spilled outputs
- `tables/…` — immutable Arrow lane segments
- `leases/…` — renewable single-writer markers per pipeline

Each segment-boundary flush writes one immutable Arrow object. Readers
deduplicate by `row_id`; the active writer periodically compacts long
segment chains. A conditional-put lease prevents two coordinators from
writing the same pipeline simultaneously. Read-only `.plan()` calls do
not take the lease.

The ledger is a separate plane. A local `Home` still uses SQLite even when
its object/lane planes are in a bucket. Multi-machine execution also needs
a shared SQLAlchemy database URL (Postgres coverage is TODO 7b).

## Retention safety

Storage reporting and GC dry-runs work against cloud inventory without one
HEAD request per object. Destructive `gc(delete=True)` intentionally
refuses cloud stores until versioned-bucket gating is implemented.

## Live R2 test

The normal test suite uses moto. See
[Contributing](../development/contributing.md#cloudflare-r2-integration-test)
for the opt-in test against a real R2 bucket.
