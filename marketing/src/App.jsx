import React from 'react'
import { ArrowRight } from 'lucide-react'
import OuroborosLogo from './components/OuroborosLogo'
import CodeBlock from './components/CodeBlock'
import dashboardRun from './assets/dashboard-run.png'
import dashboardRunMobile from './assets/dashboard-run-mobile.png'
import './index.css'

const GITHUB_URL = 'https://github.com/dinosaurav/Rubedo'
const DOCS_URL = `${import.meta.env.BASE_URL}docs/`
const EXAMPLES_URL = `${GITHUB_URL}/tree/main/examples`

const HERO_CODE = `from rubedo import pipeline

p = pipeline(name="triage")

@p.step(check_cache=False)  # rescan urls.txt every run
def inbox():
    for url in open("urls.txt"):
        yield {"url": url.strip(), "text": download(url)}

@p.step(retries=3, rate_limit="30/min")
def decide(inbox: dict):  # argument name = parent step
    out = ask_llm(f"Keep or drop?\\n{inbox['text'][:2000]}")
    return {"url": inbox["url"], "topic": out["topic"]}

p.run()  # second run: only new urls hit the LLM`

const START_CODE = `import os
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step(check_cache=False)  # rescan the folder every run
def scan():
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):  # one call per file
    n = len(scan["text"].splitlines())
    return {"line_count": n}

print(p.plan())
summary = p.run()
print(summary.created_count, summary.reused_count)`

const RETRY_CODE = `@p.step(
    retries=3,
    retry_on=(TimeoutError, ConnectionError),
    retry_backoff=2,
    rate_limit="30/min",
    stale_after="24h",
    assertions=[check_price_positive],
)
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
    q: 'What is Rubedo?',
    a: 'A Python library for batch pipelines. You decorate functions as steps; Rubedo stores every result at a content-addressed address and, on the next run, recomputes only what changed — at row granularity. It is not an orchestrator, not a hosted platform, and not a memoization decorator. State lives in a local .rubedo/ directory (SQLite control plane, Arrow IPC lane store, content-addressed object store).',
  },
  {
    q: 'What is it especially good at?',
    a: 'Batch work you iterate on where re-running is expensive or non-idempotent — LLM enrichment, scraping, paid APIs, transforms. Fix a step, re-run, and keep everything that still holds. That is surgical invalidation: only the changed rows (and their downstream steps) recompute.',
  },
  {
    q: 'Is this an orchestrator?',
    a: 'No. Rubedo does not schedule services or replace Airflow, Prefect, or Dagster. It gives dbt-style incrementality inside a Python batch DAG — recompute only what changed, at row granularity. A library you import, not a platform you operate.',
  },
  {
    q: 'Does it need a daemon or server?',
    a: 'No. pip install, import, run. rubedo serve is an optional read-only local dashboard over the ledger. No account, no cloud required.',
  },
  {
    q: 'Can my team share the cache?',
    a: 'Yes. Point the store at an S3-compatible bucket (AWS S3, Cloudflare R2, Backblaze B2, MinIO) via store_url or RUBEDO_STORE_URL — outputs and Arrow lane history land there as immutable objects, and a second machine against the same bucket and ledger reuses the first’s work. For multi-machine setups the ledger moves to Postgres. Local stays the default; still no daemon.',
  },
  {
    q: 'How stable is the API?',
    a: 'Pre-1.0. The API is unstable and there are no migrations — schema changes mean deleting .rubedo/ and re-running. The core model (content-addressed lanes, shapes, ledger) is designed and built; polish is ongoing.',
  },
  {
    q: 'When should I bump version?',
    a: 'Bump version for deliberate behavior changes (or edits the engine cannot see, like helpers your step calls). code="auto" folds source edits into the cache key; the default code="warn" never recomputes on edits but warns loudly when reused code has drifted.',
  },
]

function Eyebrow({ children }) {
  return <div className="section-label">{children}</div>
}

function Walkthrough({ steps }) {
  return (
    <ol className="walkthrough">
      {steps.map((step) => (
        <li key={step.title}>
          <strong>{step.title}</strong>
          <span>{step.body}</span>
        </li>
      ))}
    </ol>
  )
}

function ReuseStats({ rows }) {
  return (
    <div className="reuse-stats" role="list">
      {rows.map((row) => (
        <div className="reuse-stat" role="listitem" key={row.label}>
          <div className="reuse-stat-label">{row.label}</div>
          <div className="reuse-stat-value">{row.value}</div>
          <div className="reuse-stat-note">{row.note}</div>
        </div>
      ))}
    </div>
  )
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
          <a href="#proof">Proof</a>
          <a href="#try">Try it</a>
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
          <div className="hero-kicker">A Python library for batch pipelines</div>
          <h1>
            Reduce. <span className="hero-accent">Reuse.</span> Rubedo.
          </h1>
          <p className="lede">
            You write steps as ordinary functions. Rubedo stores every
            result and <strong>only recomputes what changed</strong> — so
            fixing the last step doesn&apos;t re-pay a thousand LLM calls,
            scrapes, or APIs.
          </p>
          <div className="hero-cta">
            <a className="btn btn-primary" href="#try">
              Try it <ArrowRight size={16} />
            </a>
            <a className="btn btn-outline" href={GITHUB_URL} target="_blank" rel="noreferrer">
              View on GitHub
            </a>
          </div>

          <ReuseStats
            rows={[
              { label: 'First run', value: '8 LLM calls', note: 'every URL is new' },
              { label: 'Second run', value: '0 LLM calls', note: 'nothing changed' },
              { label: 'One new URL', value: '1 LLM call', note: 'the other 7 reused' },
            ]}
          />

          <div className="hero-proof">
            <div className="hero-split">
              <div className="hero-code-col">
                <div className="snippet-label">Two functions. One pipeline.</div>
                <CodeBlock language="python" className="code-step hero-code" code={HERO_CODE} />
              </div>
              <div className="hero-explain-col">
                <div className="snippet-label">What this is doing</div>
                <Walkthrough
                  steps={[
                    {
                      title: 'inbox',
                      body: 'Reads urls.txt and downloads each page. One item per URL — a source that re-scans every run.',
                    },
                    {
                      title: 'decide',
                      body: 'Calls an LLM on each page. The argument name inbox is the dependency: no YAML, no DAG file.',
                    },
                    {
                      title: 'Run it again',
                      body: 'Already-seen URLs skip the LLM. Only new ones pay. That is the whole product.',
                    },
                  ]}
                />
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* -------- Why -------- */}
      <section className="block" id="why">
        <Eyebrow>Why</Eyebrow>
        <h2 className="block-title">Fix the last step. Don&apos;t re-pay the rest.</h2>
        <p className="block-lede">
          If you&apos;ve processed a thousand rows through an LLM and then
          needed to change the prompt, you know the failure modes.
        </p>
        <div className="why-list">
          <div className="why-item">
            <h3>Re-running re-pays.</h3>
            <p>
              Without durable per-item state, every code tweak or crash means
              re-running every API call before it. Rubedo keeps the rows that
              still hold and only recomputes the ones that don&apos;t —
              iteration that feels like a notebook, for a batch.
            </p>
          </div>
          <div className="why-item">
            <h3>A pickle file cannot see your pipeline.</h3>
            <p>
              <code>functools.cache</code> and ad-hoc caches go stale silently.
              They can&apos;t tell you <em>why</em> something recomputed, and
              they can&apos;t invalidate downstream when an input changes.
              Rubedo persists every output, with clear, configurable retention.
            </p>
          </div>
          <div className="why-item">
            <h3>Orchestrators are a different tool.</h3>
            <p>
              Airflow, Prefect, and Dagster schedule and monitor services.
              Rubedo is the incrementality layer inside a local Python script:
              row by row, only what changed. You import it; you don&apos;t
              operate it.
            </p>
          </div>
        </div>
      </section>

      {/* -------- Proof -------- */}
      <section className="block block-tinted" id="proof">
        <div className="block-inner">
          <Eyebrow>Proof</Eyebrow>
          <h2 className="block-title">Second run: nothing changed, nothing recomputed.</h2>
          <p className="block-lede">
            This is a real dashboard over a real run of the bundled
            example — 22 results kept, 0 re-run, 0.1s. No account, no
            cloud. <code>rubedo serve</code> opens it on your machine.
          </p>
          <figure className="dashboard-shot">
            <picture>
              <source media="(max-width: 860px)" srcSet={dashboardRunMobile} />
              <img
                src={dashboardRun}
                alt="Rubedo dashboard run detail: pipeline DAG with every step reused, status cards showing 22 reused and 0 created, and a per-lane coordinates table."
                width={1280}
                height={800}
                loading="lazy"
              />
            </picture>
            <figcaption>
              Second run of <code>examples/count_lines</code> — created 0, reused 22, in 0.1s.
              Each file is two steps (scan + count); seven files plus one aggregate is 22.
            </figcaption>
          </figure>
        </div>
      </section>

      {/* -------- Try it -------- */}
      <section className="block" id="try">
        <Eyebrow>Try it</Eyebrow>
        <h2 className="block-title">Install. Write two functions. Run twice.</h2>
        <p className="block-lede">
          No API key. A folder of files in, a line count out. Boring on
          purpose — so you can see reuse without paying for it.
        </p>
        <ol className="try-steps">
          <li>
            <div className="try-step-label">1. Install</div>
            <CodeBlock code="pip install rubedo" language="bash" />
          </li>
          <li>
            <div className="try-step-label">2. Define a pipeline</div>
            <div className="try-split">
              <CodeBlock code={START_CODE} language="python" className="code-step" />
              <Walkthrough
                steps={[
                  {
                    title: 'scan',
                    body: 'Lists a folder and yields one item per file. check_cache=False means it re-reads the folder every run, so new and edited files show up.',
                  },
                  {
                    title: 'count_lines',
                    body: 'Runs once per file. The argument name scan is the parent — Rubedo builds the graph from the function signature.',
                  },
                  {
                    title: 'plan, then run',
                    body: 'plan() is a dry-run: what would recompute, and why. run() executes. Print created vs reused to see the point.',
                  },
                ]}
              />
            </div>
          </li>
          <li>
            <div className="try-step-label">3. Run twice — watch reuse</div>
            <ReuseStats
              rows={[
                { label: 'First run', value: 'created 8', note: 'every file is new' },
                { label: 'Second run', value: 'reused 8', note: 'nothing changed' },
                { label: 'Edit one file', value: 'created 2', note: 'scan + count for that file; the rest stay' },
              ]}
            />
            <p className="try-note">
              <code>created=2</code> is two steps for one file, not two files.
              The other files don&apos;t re-run.
            </p>
          </li>
        </ol>
      </section>

      {/* -------- How it works -------- */}
      <section className="block block-tinted" id="how">
        <div className="block-inner">
          <Eyebrow>How it works</Eyebrow>
          <h2 className="block-title">What actually happens when you re-run.</h2>
          <div className="capability-beats">
            <div className="beat">
              <h3>Only changed rows recompute</h3>
              <p>
                Every output lives at a content-addressed address:{' '}
                <code>hash(step, version, input_hash, …)</code>. Re-runs
                recompute only what changed, at row granularity — surviving
                reordering, dedup, and appends. That is surgical invalidation.
              </p>
            </div>
            <div className="beat">
              <h3>History you can trust after a crash</h3>
              <p>
                An append-only run ledger records every run, lane, and event
                immutably in SQLite. Workers can die mid-run without corrupting
                committed state. Lineage edges connect each output to what
                produced it.
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
              <h3>Share the cache via your bucket</h3>
              <p>
                State lives in <code>.rubedo/</code> until you say otherwise. Point the
                store at an S3-compatible bucket (S3, R2, B2, MinIO) and the ledger at
                Postgres, and a second machine reuses the first&apos;s outputs — the
                run-it-twice payoff, across machines.
              </p>
            </div>
            <div className="beat beat-quiet">
              <h3>Run on your cluster, not ours</h3>
              <p>
                <code>executor=</code> takes <code>&quot;thread&quot;</code>,{' '}
                <code>&quot;process&quot;</code>, or a factory returning any Future-shaped
                pool — Dask and Ray examples included. Against a cloud store, workers
                fetch inputs and put results by reference; the coordinator never relays
                the bytes. Executor choice never changes cache identity.
              </p>
            </div>
            <div className="beat beat-quiet">
              <h3>Planning stays fast as history grows</h3>
              <p>
                Outputs live in per-step, append-only <strong>Arrow IPC</strong> files, so
                the reuse checks that dominate plan time are vectorized scans — not
                row-by-row SQLite.{' '}
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

      {/* -------- Compare -------- */}
      <section className="block" id="compare">
        <Eyebrow>Where it sits</Eyebrow>
        <h2 className="block-title">If you already have a tool for this.</h2>
        <p className="block-lede">
          Rubedo is dbt-style incrementality for Python batches — not a
          scheduler, not a file-level rebuild tool, not a memoization decorator.
        </p>
        <div className="compare-table" role="table" aria-label="How Rubedo compares">
          <div className="compare-row compare-head" role="row">
            <div role="columnheader">Tool</div>
            <div role="columnheader">Job</div>
            <div role="columnheader">Rubedo&apos;s angle</div>
          </div>
          {COMPARISON.map((row) => (
            <div className="compare-row" role="row" key={row.tool}>
              <div role="cell" className="compare-tool">{row.tool}</div>
              <div role="cell" className="compare-job" data-label="Job">{row.job}</div>
              <div role="cell" className="compare-angle" data-label="Rubedo">{row.angle}</div>
            </div>
          ))}
        </div>
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
          <p>
            Fix the last step. Keep everything that still holds. Local by
            default, shared via your bucket. MIT licensed.
          </p>
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

      <aside className="agent-aside" aria-label="At a glance">
        <div className="agent-aside-inner">
          <div className="snippet-label">At a glance</div>
          <p>
            Rubedo is a local-first Python library — not an orchestrator —
            for DAG pipelines over keyed collections (files, CSV rows, URLs)
            with content-addressed row-level caching, an append-only run
            ledger, and surgical invalidation. Think dbt-style state for
            Python tasks, built for non-idempotent steps: LLM calls, scraping,
            paid APIs. A library, not a platform: no daemon, no registry.
            State lives in <code>.rubedo/</code> (SQLite + Arrow IPC + object
            store); optional S3-compatible store and Postgres ledger. Pre-1.0,
            MIT licensed.
          </p>
        </div>
      </aside>

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
