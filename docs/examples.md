# Examples

Every example in [`examples/`](https://github.com/dinosaurav/Rubedo/tree/main/examples)
is a self-contained folder — one runnable script plus any data it needs —
that talks to a **real** service (no mocks), using only the Python standard
library. A few need a library already in the dev dependency group (PyMuPDF
for `pdf_digest`), so `uv run python examples/...` just works without extra
installs.

Run them from the **repo root**, not from inside the example's folder:

```bash
uv run python examples/<name>/<name>.py
```

`.rubedo/` is created relative to wherever you run from (see the CWD gotcha
in [Getting Started](getting-started.md)) — run from the repo root and every
example shares the one store `rubedo ls` and the dashboard read. Run from
inside an example's folder and you silently fork a second, stray `.rubedo/`
there.

Re-run any example and watch it reuse everything — the whole point of
Rubedo is that the second run recomputes only what actually changed.

| Example | Service(s) | Shape | Shows off |
|---|---|---|---|
| [`count_lines`](https://github.com/dinosaurav/Rubedo/tree/main/examples/count_lines) | local files | map → aggregate | the basics: `params_model`, an aggregate step |
| [`hn_digest`](https://github.com/dinosaurav/Rubedo/tree/main/examples/hn_digest) | Hacker News + an LLM | filter → LLM → LLM aggregate | a source-shaped `@p.step` root, `Filtered`, caching non-idempotent LLM calls |
| [`github_health`](https://github.com/dinosaurav/Rubedo/tree/main/examples/github_health) | GitHub REST | fan-in diamond | chained retried/rate-limited calls, aggregate |
| [`weather_advisory`](https://github.com/dinosaurav/Rubedo/tree/main/examples/weather_advisory) | Open-Meteo (keyless) | chain → aggregate | two chained APIs, `stale_after` TTL |
| [`gutenberg_stats`](https://github.com/dinosaurav/Rubedo/tree/main/examples/gutenberg_stats) | Project Gutenberg | fetch → clean → analyze → aggregate | `skip_cache` inline util + `executor="process"` CPU parallelism |
| [`orders_rollup`](https://github.com/dinosaurav/Rubedo/tree/main/examples/orders_rollup) | SQLite (self-contained) | map → aggregate | a table recipe: a source-shaped `@p.step` root doing a plain SELECT loop |
| [`executor_showdown`](https://github.com/dinosaurav/Rubedo/tree/main/examples/executor_showdown) | dwyl/english-words (GitHub) | map → aggregate | `executor="thread"` vs `executor="process"` on real CPU-bound work — run both and compare elapsed time |
| [`expand_feed`](https://github.com/dinosaurav/Rubedo/tree/main/examples/expand_feed) | local files (self-contained) | expand | `shape="expand"` (`out_shape="many"`) — one feed fans into a lane per article, the expansion cached so a re-run re-scrapes nothing |
| [`newsroom`](https://github.com/dinosaurav/Rubedo/tree/main/examples/newsroom) | local CSVs (self-contained) | join → expand → aggregate | every producer shape at once: multiple source-shaped `@p.step` roots, N-way `shape="join"`, `shape="expand"`, and a `group_key` aggregate |
| [`pdf_digest`](https://github.com/dinosaurav/Rubedo/tree/main/examples/pdf_digest) | a PDF + a vision & a text LLM | map root → expand → LLM → aggregate → 2× LLM | a source-less `map` root (the PDF path is a param, no `Source`), a cheap vision LLM on figure pages, and a picture-aware vs. text-only summary comparison |

## Detail, by example

**[`count_lines`](https://github.com/dinosaurav/Rubedo/tree/main/examples/count_lines)**
— the flagship. A source-shaped `@p.step` root yields file paths, `read_lines` and
`count_lines` chain off it, and an `aggregate` step (`total_lines`) sums the
line counts across every file. Registers its steps via decorators on a
`pipeline(...)` object and a Pydantic `params_model` (`min_lines`,
`include_text_preview`) to show params flowing into a step and being
validated before the run starts.

**[`hn_digest`](https://github.com/dinosaurav/Rubedo/tree/main/examples/hn_digest)**
— `top_story → screen → classify → digest (aggregate)` over the live Hacker
News Firebase API and an LLM (via OpenRouter). The root only yields a
story's numeric id; `screen` does the actual fetch, so later runs never
re-hit HN for a story once it's been screened. `classify` can return
`Filtered` to drop stories out of scope, and that verdict — like the LLM
call itself — is cached per story, so a second run makes zero LLM calls.
Needs `OPENROUTER_API_KEY`.

**[`github_health`](https://github.com/dinosaurav/Rubedo/tree/main/examples/github_health)**
— a fan-in diamond (`fetch_repo` and `activity` both feed `score`, which
feeds an `aggregate` step) over the GitHub REST API. No LLM — pure REST, to show
the same engine over a different shape of flaky, quota'd work. Works
unauthenticated (60 req/hr) or with `GITHUB_TOKEN` (5000/hr).

**[`weather_advisory`](https://github.com/dinosaurav/Rubedo/tree/main/examples/weather_advisory)**
— `cities.csv → geocode → forecast → advice → briefing (aggregate)` over two
keyless Open-Meteo APIs. `forecast` carries `stale_after="3h"`: a forecast
older than three hours re-fetches on the next run, and if the numbers
actually changed, `advice` recomputes — identical bytes just refresh the
clock instead.

**[`gutenberg_stats`](https://github.com/dinosaurav/Rubedo/tree/main/examples/gutenberg_stats)**
— downloads public-domain books from Project Gutenberg and computes
readability stats. `clean` is `skip_cache=True` (a quick, idempotent
boilerplate-stripper fused into `analyze`'s cache key, never materialized
itself); `analyze` is `executor="process"` because the token-crunching is
genuinely CPU-bound.

**[`orders_rollup`](https://github.com/dinosaurav/Rubedo/tree/main/examples/orders_rollup)**
— self-contained: creates a small SQLite `orders` table in your temp dir,
then rolls it up with a source-shaped `@p.step` root that's a plain `SELECT * FROM
orders` loop, one row per lane. A table recipe like this buffers every row
before the run commits (fine here; see
[Concepts: sources](concepts/sources.md) for the streaming variant a much
larger table would need).

**[`executor_showdown`](https://github.com/dinosaurav/Rubedo/tree/main/examples/executor_showdown)**
— downloads a real ~370k-word English dictionary, splits it into 8 chunks,
and runs the same genuinely CPU-bound analysis (anagram signatures,
per-letter frequency vectors) under `executor="thread"` and
`executor="process"`, timing both. Threads don't parallelize CPU-bound
Python (the GIL); processes do. Pass `--force` to re-pay the full cost and
see the difference — a cached second run reuses everything almost
instantly regardless of executor.

**[`expand_feed`](https://github.com/dinosaurav/Rubedo/tree/main/examples/expand_feed)**
— `tech.json → fetch → articles (expand) → headline (map)`: `shape="expand"`
(`out_shape="many"`) is the 1:N shape, where a step yields a payload per
item and each becomes
its own content-addressed downstream lane. The whole expansion is cached
against its parent, so a re-run of the "scrape" step re-expands nothing.

**[`newsroom`](https://github.com/dinosaurav/Rubedo/tree/main/examples/newsroom)**
— every producer shape at once. Two source-shaped `@p.step` CSV roots (`feeds`,
`publishers`) meet in an N-way `shape="join"` on the publisher name (minting `feed|publisher`
pair lanes), each feed then `shape="expand"`s into a lane per article, and a
final `shape="reduce"` (`in_shape="aggregate"`) step with
`group_key="region"` folds the articles into
one digest per region. Entirely self-contained — no network calls.

**[`pdf_digest`](https://github.com/dinosaurav/Rubedo/tree/main/examples/pdf_digest)**
— the source-less-root example: `load_pdf` takes no `depends_on`, reading
the PDF path out of `params` instead and minting a single `@root` lane. From there: `split_chunks` (expand) fans the document into one
lane per page/figure, `caption` (map) sends only image chunks to a cheap
vision LLM, `rejoin` (aggregate) reassembles reading order into a
picture-aware and a text-only document, and two more LLM steps summarize
each — a side-by-side of what the pictures were worth. Needs
`OPENROUTER_API_KEY`; PyMuPDF (`pymupdf`) is already in the dev dependency
group.

## Keys

Only the LLM examples need one. Put it in a `.env` file at the repo root
(already gitignored) and the examples load it automatically:

```
OPENROUTER_API_KEY=sk-or-...
```

`hn_digest` calls a cheap model through [OpenRouter](https://openrouter.ai)
(default `minimax/minimax-m2.5`; override with `OPENROUTER_MODEL`).
`pdf_digest` uses a cheap vision model (default `google/gemini-2.5-flash-lite`;
override with `OPENROUTER_VISION_MODEL` / `OPENROUTER_TEXT_MODEL`).
`github_health` works unauthenticated but is happier with `GITHUB_TOKEN` set.

See the [tutorial](tutorial.md) for a from-scratch walkthrough of the same
ideas, and the [API reference](reference/api/index.md) for every function and
parameter these examples use.
