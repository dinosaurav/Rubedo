import React from 'react'
import { ArrowRight } from 'lucide-react'
import OuroborosLogo from './components/OuroborosLogo'
import CodeBlock from './components/CodeBlock'
import dashboardRun from './assets/dashboard-run.png'
import './index.css'

const GITHUB_URL = 'https://github.com/dinosaurav/Rubedo'
const DOCS_URL = `${import.meta.env.BASE_URL}docs/`
const EXAMPLES_URL = `${GITHUB_URL}/tree/main/examples`

const HERO_CODE = `from rubedo import pipeline, Filtered

p = pipeline(name="triage")

@p.step
def inbox():
    for url in open("urls.txt"):
        yield {"url": url.strip(), "text": download(url)}

@p.step(retries=3, rate_limit="30/min")
def decide(inbox: dict) -> dict | Filtered:
    out = ask_llm(f"Keep or drop?\\n{inbox['text'][:2000]}")
    if out["keep"] is False:
        return Filtered(out["why"])
    return {"url": inbox["url"], "topic": out["topic"]}

p.run()   # second run: only new urls recompute`

const REUSE_PROOF = `# first run          created=8  reused=0
# second run         created=0  reused=8     # nothing recomputed
# edit one file...   created=2  reused=6     # only that file's lanes re-run`

const START_CODE = `import os
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step
def scan():
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):
    return {"line_count": len(scan["text"].splitlines())}

print(p.plan())   # dry-run: what would run, and why
summary = p.run()
print(f"created={summary.created_count} reused={summary.reused_count}")`

const RETRY_CODE = `@p.step(retries=3, retry_on=(TimeoutError, ConnectionError),
        retry_backoff=2, rate_limit="30/min",
        stale_after="24h",
        assertions=[check_price_positive])
def enrich(row: dict): ...`

const COMPARISON = [
  {
    tool: 'Airflow / Prefect / Dagster',
    job: 'Orchestrate and monitor workflows',
    angle: 'Different layer — they schedule services. Rubedo gives row-level, content-addressed reuse inside a local script.',
  },
  {
    tool: 'dbt',
    job: 'Incremental state for SQL',
    angle: 'Same idea, for Python steps over files, rows, and live sources.',
  },
  {
    tool: 'Make / Snakemake',
    job: 'File-level rebuilds',
    angle: 'Rubedo tracks content at row granularity, with a queryable ledger and lineage.',
  },
  {
    tool: 'joblib / diskcache',
    job: 'Function memoization',
    angle: 'No DAG awareness, no plan/invalidate story, no crash-honest history.',
  },
]

const FAQ = [
  {
    q: 'Is this an orchestrator?',
    a: 'No. Rubedo does not schedule services or replace Airflow/Prefect/Dagster. It gives dbt-style incrementality inside a Python batch DAG — recompute only what changed, at row granularity.',
  },
  {
    q: 'Does it need a daemon or server?',
    a: 'No. It is a library: pip install, import, run. State lives in a local .rubedo/ directory. rubedo serve is an optional read-only local dashboard.',
  },
  {
    q: 'How stable is the API?',
    a: 'Pre-1.0. The API is unstable and there are no migrations — schema changes mean deleting .rubedo/ and re-running. The core model (content-addressed lanes, shapes, ledger) is designed and built; polish is ongoing.',
  },
  {
    q: 'When should I bump version?',
    a: 'Bump version for deliberate behavior changes (or edits the engine cannot see, like helpers your step calls). code="auto" folds source edits into the cache key; the default code="warn" never recomputes on edits but warns loudly when reused code has drifted.',
  },
  {
    q: 'What is it especially good at?',
    a: 'Batch DAGs you iterate on — enrichment, scraping, transforms — where you want a real edit-test loop: fix a step, re-run, and keep everything that still holds.',
  },
]

function Eyebrow({ children }) {
  return <div className="section-label">{children}</div>
}

function App() {
  return (
    <div className="landing">
      <header className="landing-nav">
        <a className="brand" href="#top">
          <OuroborosLogo size={28} />
          <span>RUBEDO</span>
        </a>
        <nav className="nav-links">
          <a href="#why">Why</a>
          <a href="#try">Try it</a>
          <a href="#compare">Compare</a>
          <a href={DOCS_URL}>Docs</a>
          <a href={EXAMPLES_URL} target="_blank" rel="noreferrer">Examples</a>
          <a className="btn btn-outline btn-sm" href={GITHUB_URL} target="_blank" rel="noreferrer">
            GitHub <ArrowRight size={14} />
          </a>
        </nav>
      </header>

      {/* -------- Hero -------- */}
      <section className="hero" id="top">
        <div className="hero-inner">
          <h1>
            Reduce. <span className="hero-accent">Reuse.</span> Rubedo.
          </h1>
          <p className="lede">
            Stateful Python pipelines that remember every step —
            and only recompute what actually changed.
          </p>
          <div className="hero-cta">
            <a className="btn btn-primary" href="#try">
              Try it <ArrowRight size={16} />
            </a>
            <a className="btn btn-outline" href={GITHUB_URL} target="_blank" rel="noreferrer">
              View on GitHub
            </a>
          </div>

          <div className="hero-proof">
            <div className="snippet-label">inbox → decide</div>
            <CodeBlock language="python" className="code-step hero-code" code={HERO_CODE} />
            <p className="hero-caption">
              A two-step DAG. Re-run it and only new urls recompute.
            </p>
          </div>
        </div>
      </section>

      {/* -------- Why -------- */}
      <section className="block" id="why">
        <Eyebrow>Why</Eyebrow>
        <h2 className="block-title">An edit-test loop for batch pipelines.</h2>
        <p className="block-lede">
          Rubedo is a <strong>library, not a platform</strong> — no daemon, no
          registry. State lives in <code>.rubedo/</code>, created on first run.
        </p>
        <div className="why-list">
          <div className="why-item">
            <h3>Fix the last step. Re-run.</h3>
            <p>
              Only that step recomputes. Upstream stays put. Downstream follows the
              new inputs. Iteration that feels like a notebook — for a DAG.
            </p>
          </div>
          <div className="why-item">
            <h3>Ad-hoc caches go stale silently.</h3>
            <p>
              A pickle file or <code>functools.cache</code> cannot tell when an upstream
              step&apos;s code changed — and if anything survives at all, the rules are
              whoever wrote the tempfile. Rubedo persists every output to disk, with
              clear, configurable retention.
            </p>
          </div>
          <div className="why-item">
            <h3>Orchestrators are a different tool.</h3>
            <p>
              Airflow, Prefect, and Dagster schedule and monitor services.
              Rubedo is dbt-style incrementality for Python — row by row, content-addressed.
            </p>
          </div>
        </div>
      </section>

      {/* -------- Try it -------- */}
      <section className="block block-tinted" id="try">
        <div className="block-inner">
          <Eyebrow>Try it</Eyebrow>
          <h2 className="block-title">Install. Define. Run. Run again.</h2>
          <ol className="try-steps">
            <li>
              <div className="try-step-label">1. Install</div>
              <CodeBlock code="pip install rubedo" language="bash" />
            </li>
            <li>
              <div className="try-step-label">2. Define a pipeline</div>
              <CodeBlock code={START_CODE} language="python" className="code-step" />
            </li>
            <li>
              <div className="try-step-label">3. Run twice — watch reuse</div>
              <CodeBlock language="text" className="reuse-block" code={REUSE_PROOF} />
            </li>
          </ol>
        </div>
      </section>

      {/* -------- Compare -------- */}
      <section className="block" id="compare">
        <Eyebrow>Where it sits</Eyebrow>
        <h2 className="block-title">dbt-style state for Python batches.</h2>
        <div className="compare-table" role="table" aria-label="How Rubedo compares">
          <div className="compare-row compare-head" role="row">
            <div role="columnheader">Tool</div>
            <div role="columnheader">Job</div>
            <div role="columnheader">Rubedo&apos;s angle</div>
          </div>
          {COMPARISON.map((row) => (
            <div className="compare-row" role="row" key={row.tool}>
              <div role="cell" className="compare-tool">{row.tool}</div>
              <div role="cell">{row.job}</div>
              <div role="cell">{row.angle}</div>
            </div>
          ))}
        </div>
      </section>

      {/* -------- How it works -------- */}
      <section className="block block-tinted" id="how">
        <div className="block-inner">
          <Eyebrow>How it works</Eyebrow>
          <h2 className="block-title">Content-addressed. Crash-honest. Fast to plan.</h2>
          <div className="capability-beats">
            <div className="beat">
              <h3>Content-addressed caching</h3>
              <p>
                Every output lives at <code>hash(step, version, input_hash, …)</code>.
                Re-runs recompute only what changed, at row granularity — surviving
                reordering, dedup, and appends.
              </p>
            </div>
            <div className="beat">
              <h3>An append-only run ledger</h3>
              <p>
                Every run, lane, and event recorded immutably in SQLite. Workers can die
                mid-run without corrupting committed state. Lineage edges connect each
                output to what produced it.
              </p>
            </div>
            <div className="beat">
              <h3>Retries, rate limits, assertions</h3>
              <CodeBlock code={RETRY_CODE} language="python" className="code-step" />
              <p>
                Narrow <code>retry_on</code>, paced workers, <code>stale_after</code> TTLs,
                and assertions that stop bad data before it commits.
              </p>
            </div>
            <div className="beat">
              <h3>A columnar data plane</h3>
              <p>
                Outputs live in per-step, append-only <strong>Arrow IPC</strong> files, so
                the reuse checks that dominate plan time are vectorized scans — not
                row-by-row SQLite. Planning stays fast as history grows.{' '}
                <a href={DOCS_URL}>Details in the docs</a>
                {' · '}
                <a href={`${GITHUB_URL}/tree/main/benchmarks`} target="_blank" rel="noreferrer">
                  benchmarks
                </a>
                .
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* -------- Dashboard -------- */}
      <section className="block" id="dashboard">
        <Eyebrow>Dashboard</Eyebrow>
        <h2 className="block-title">See the whole run — locally.</h2>
        <p className="block-lede">
          <code>rubedo serve</code> opens a read-only browser over your ledger:
          live runs, DAGs, lineage, every lane. No account, no cloud.
        </p>
        <figure className="dashboard-shot">
          <img
            src={dashboardRun}
            alt="Rubedo dashboard run detail: pipeline DAG with every step reused, status cards showing 22 reused and 0 created, and a per-lane coordinates table."
            width={1280}
            height={800}
            loading="lazy"
          />
          <figcaption>
            Second run of <code>examples/count_lines</code> — created 0, reused 22, in 0.1s.
          </figcaption>
        </figure>
      </section>

      {/* -------- FAQ -------- */}
      <section className="block block-tinted" id="faq">
        <div className="block-inner">
          <Eyebrow>FAQ</Eyebrow>
          <h2 className="block-title">Straight answers.</h2>
          <dl className="faq-list">
            {FAQ.map((item) => (
              <div className="faq-item" key={item.q}>
                <dt>{item.q}</dt>
                <dd>{item.a}</dd>
              </div>
            ))}
          </dl>
        </div>
      </section>

      {/* -------- Closing CTA -------- */}
      <section className="closing">
        <div className="closing-inner">
          <h2>
            Reduce. <span className="hero-accent">Reuse.</span> Rubedo.
          </h2>
          <p>A data-science loop for batches. Local today. MIT licensed.</p>
          <div className="hero-cta">
            <a className="btn btn-primary" href={DOCS_URL}>
              Read the docs <ArrowRight size={16} />
            </a>
            <a className="btn btn-outline" href={EXAMPLES_URL} target="_blank" rel="noreferrer">
              Browse the examples
            </a>
          </div>
        </div>
      </section>

      <footer className="landing-footer">
        <div className="footer-inner">
          <div className="brand">
            <OuroborosLogo size={22} />
            <span>RUBEDO</span>
          </div>
          <nav className="footer-links">
            <a href={GITHUB_URL} target="_blank" rel="noreferrer">GitHub</a>
            <a href={`${GITHUB_URL}/blob/main/README.md`} target="_blank" rel="noreferrer">README</a>
            <a href={DOCS_URL}>Docs</a>
            <a href={EXAMPLES_URL} target="_blank" rel="noreferrer">Examples</a>
            <a href={`${DOCS_URL}development/invariants/`}>Invariants</a>
          </nav>
          <div className="footer-meta">
            Pre-1.0. The API is unstable; schema changes mean deleting <code>.rubedo/</code> and
            re-running. MIT licensed.
          </div>
        </div>
      </footer>
    </div>
  )
}

export default App
