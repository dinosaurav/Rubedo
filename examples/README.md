# Examples

Each example is a self-contained folder: one runnable script plus any data it
needs. They talk to **real** services (no mocks) using only the Python standard
library — nothing extra to install. Run any of them from the repo root:

```bash
uv run python examples/<name>/<name>.py
```

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

## Keys

Only the LLM example needs a key. Put it in a `.env` at the repo root (it is
already gitignored) and the example loads it automatically:

```
OPENROUTER_API_KEY=sk-or-...
```

`hn_digest` calls a cheap model through [OpenRouter](https://openrouter.ai)
(default `minimax/minimax-m2.5`; set `OPENROUTER_MODEL` to try another).
`github_health` works unauthenticated but is happier with `GITHUB_TOKEN` set.
