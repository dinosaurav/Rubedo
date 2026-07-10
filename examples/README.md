# Examples

Each example is a self-contained folder: one runnable script plus any data it
needs. They talk to **real** services (no mocks) using only the Python standard
library — nothing extra to install.

Run them **from the repo root**, not from inside the example's folder:

```bash
cd <repo root>
uv run python examples/<name>/<name>.py
```

Rubedo's state directory (`.rubedo/`) is created relative to wherever you run
from. Run from the repo root and every example shares the root store — the one
`rubedo ls` and the dashboard read. Run from inside an example's folder and you
silently fork a second, stray `.rubedo/` there that the dashboard never sees.

Re-run any example and watch it reuse everything — the whole point of Rubedo is
that the second run recomputes only what actually changed.

| Example | Service(s) | Shape | Shows off |
|---|---|---|---|
| [`count_lines`](count_lines/) | local files | map → reduce | the basics: `params_model`, a reduce step |
| [`hn_digest`](hn_digest/) | Hacker News + an LLM | filter → LLM → LLM reduce | a custom `Source`, `Filtered`, `index=`, caching non-idempotent LLM calls |
| [`github_health`](github_health/) | GitHub REST | fan-in diamond | chained retried/rate-limited calls, `ProcessResult`, reduce |
| [`weather_advisory`](weather_advisory/) | Open-Meteo (keyless) | chain → reduce | two chained APIs, `stale_after` TTL |
| [`gutenberg_stats`](gutenberg_stats/) | Project Gutenberg | fetch → clean → analyze → reduce | `skip_cache` inline util + `executor="process"` CPU parallelism |
| [`orders_rollup`](orders_rollup/) | SQLite (self-contained) | map → reduce | `TableSource` in streaming (`batch_size`) mode |
| [`executor_showdown`](executor_showdown/) | dwyl/english-words (GitHub) | map → reduce | `executor="thread"` vs `executor="process"` on real CPU-bound work — run both and compare the elapsed time |
| [`expand_feed`](expand_feed/) | local files (self-contained) | expand | `shape="expand"` — one feed fans into a lane per article, the expansion cached so a re-run re-scrapes nothing |
| [`newsroom`](newsroom/) | local CSVs (self-contained) | join → expand → reduce | every producer shape at once: multi-source `sources={}`, N-way `shape="join"`, `shape="expand"`, and a `group_key` reduce |
| [`pdf_digest`](pdf_digest/) | a PDF + a vision & a text LLM | map root → expand → LLM → reduce → 2× LLM | a **source-less `map` root** (the PDF path is a param, no `Source`), a cheap vision LLM on figure pages, and a picture-aware vs text-only summary comparison |

## Keys

Only the LLM examples need a key. Put it in a `.env` at the repo root (it is
already gitignored) and the examples load it automatically:

```
OPENROUTER_API_KEY=sk-or-...
```

`hn_digest` calls a cheap model through [OpenRouter](https://openrouter.ai)
(default `minimax/minimax-m2.5`; set `OPENROUTER_MODEL` to try another).
`pdf_digest` uses a cheap vision model (default `google/gemini-2.5-flash-lite`;
set `OPENROUTER_VISION_MODEL` / `OPENROUTER_TEXT_MODEL` to try others) and needs
`pymupdf` (already in the dev dependency group, so `uv run` just works).
`github_health` works unauthenticated but is happier with `GITHUB_TOKEN` set.
