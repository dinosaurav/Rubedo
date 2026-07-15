import React from 'react'
import {
  ArrowRight, Database, History, Repeat, ShieldCheck, Eye,
} from 'lucide-react'
import OuroborosLogo from './components/OuroborosLogo'
import DiamondDag from './components/DiamondDag'
import CodeBlock from './components/CodeBlock'
import Tooltip from './components/Tooltip'
import './index.css'

const GITHUB_URL = 'https://github.com/dinosaurav/Rubedo'
const DOCS_URL = `${import.meta.env.BASE_URL}docs/`

function Eyebrow({ children }) {
  return <div className="section-label">{children}</div>
}

function Pillar({ icon, title, children }) {
  return (
    <div className="pillar card">
      <div className="pillar-icon">{icon}</div>
      <h3>{title}</h3>
      <p>{children}</p>
    </div>
  )
}

function Feature({ icon, title, children }) {
  return (
    <li className="feature">
      <span className="feature-icon">{icon}</span>
      <div>
        <h4>{title}</h4>
        <p>{children}</p>
      </div>
    </li>
  )
}

const NEWSROOM_CODE = `from rubedo import pipeline
import csv

p = pipeline(name="newsroom")

@p.step
def feeds():
    with open("feeds.csv") as f:
        yield from csv.DictReader(f)

@p.step
def publishers():
    with open("publishers.csv") as f:
        yield from csv.DictReader(f)

@p.step(index=["publisher"])
def feed(feeds: dict) -> dict:
    return {"feed_id": feeds["feed_id"], "publisher": feeds["publisher"]}

@p.step(index=["publisher"])
def publisher(publishers: dict) -> dict:
    return {"publisher": publishers["publisher"], "region": publishers["region"]}

# two sources meet — one lane per matched pair
@p.step(
        join_on={"feed": "publisher", "publisher": "publisher"})
def feed_meta(feed: dict, publisher: dict) -> dict:
    return {"feed_id": feed["feed_id"], "region": publisher["region"]}

# fan out: a lane per article. cached against its parent,
# so a re-run re-scrapes nothing. this is the flaky one.
@p.step(index=["region"], retries=3,
        retry_on=(TimeoutError, ConnectionError),
        retry_backoff=2, rate_limit="30/min", stale_after="24h")
def articles(feed_meta: dict):
    for title in scrape(feed_meta["feed_id"]):
        yield {"title": title, "region": feed_meta["region"]}

# fold back: one digest per region
@p.step(group_key="region")
def digest(articles: dict) -> dict:
    titles = sorted(a["title"] for a in articles.values())
    return {"count": len(titles), "headlines": titles}`

const DIAGRAM_CODE = `Pipeline 'newsroom'
┌────────────────┐  ┌─────────────────────┐
│ feeds [expand] │  │ publishers [expand] │
└────────────────┘  └─────────────────────┘
    ┌────┘      ┌──────────────┘
┌──────┐  ┌───────────┐
│ feed │  │ publisher │
└──────┘  └───────────┘
    └─────┐     │
          ├─────┘
┌──────────────────┐
│ feed_meta [join] │
└──────────────────┘
          │
┌───────────────────┐
│ articles [expand] │
└───────────────────┘
         ┌┘
┌─────────────────┐
│ digest [reduce] │
└─────────────────┘`

const RETRY_CODE = `@p.step(retries=3, retry_on=(TimeoutError, ConnectionError),
        retry_backoff=2, rate_limit="30/min",
        stale_after="24h",
        assertions=[check_price_positive])
def enrich(row: dict): ...`

const START_CODE = `import os
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step
def scan():
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):
    return {"line_count": len(scan["text"].splitlines())}

print(p.plan())      # dry-run: what would run, and why
summary = p.run()    # execute
print(f"created={summary.created_count} reused={summary.reused_count}")`

function App() {
  return (
    <div className="landing">
      {/* ---------------- Nav ---------------- */}
      <header className="landing-nav">
        <a className="brand" href="#top">
          <OuroborosLogo size={28} />
          <span>RUBEDO</span>
        </a>
        <nav className="nav-links">
          <a href="#why">Why</a>
          <a href="#how">How</a>
          <a href="#start">Get started</a>
          <a href={DOCS_URL}>Docs</a>
          <a className="btn btn-outline btn-sm" href={GITHUB_URL} target="_blank" rel="noreferrer">
            GitHub <ArrowRight size={14} />
          </a>
        </nav>
      </header>

      {/* ---------------- Hero: WHAT ---------------- */}
      <section className="hero" id="top">
        <div className="hero-inner">
          <div className="badges">
            <span className="badge badge-info">Python 3.11+</span>
            <span className="badge badge-success">MIT</span>
            <span className="badge badge-warning">pre-1.0</span>
          </div>
          <h1>
            Reduce. <span className="hero-accent">Reuse.</span> Rubedo.
          </h1>
          <p className="lede">
            Stateful Python pipelines — run, remember, re-run only what changed.
          </p>
          <p className="lede lede-sub">
            For <strong>data enrichment</strong> over live services, where every batch re-run
            re-pays for the same LLM calls and API hits.
          </p>
          <div className="hero-cta">
            <a className="btn btn-primary" href="#start">
              Get started <ArrowRight size={16} />
            </a>
            <a className="btn btn-outline" href={GITHUB_URL} target="_blank" rel="noreferrer">
              View on GitHub
            </a>
          </div>

          <div className="hero-graphic">
            <div className="diamond-frame">
              <DiamondDag />
            </div>
            <div className="hero-reuse">
              <div className="snippet-label">Run it twice. Everything reuses.</div>
              <CodeBlock
                language="text"
                className="reuse-block"
                code={`# first run          created=8  reused=0
# second run         created=0  reused=8     # nothing recomputed
# edit one file...   created=2  reused=6     # only that file's lanes re-run`}
              />
            </div>
          </div>
        </div>
      </section>

      {/* ---------------- Why ---------------- */}
      <section className="block" id="why">
        <Eyebrow>Why</Eyebrow>
        <h2 className="block-title">Built for data enrichment over live services, and the iteration that comes with it.</h2>
        <p className="block-lede">
          Rubedo is a <strong>library, not a platform</strong>. No daemon, no registry; you import the
          engine, it never imports you. It runs from your laptop on day one, and a managed cloud runtime
          is coming so the same pipelines scale without a rewrite. State lives in a{' '}
          <code>.rubedo/</code> directory, created on first run.
        </p>
        <div className="why-grid">
          <div className="why-card">
            <h3>Re-running re-pays.</h3>
            <p>Every LLM call or API hit you redo on a code tweak is money burned. The results may not even match.</p>
          </div>
          <div className="why-card">
            <h3>Ad-hoc caches go stale silently.</h3>
            <p>A pickle file or <code>functools.cache</code> can&apos;t tell when an upstream step&apos;s code changed. You rerun and get yesterday&apos;s results mixed with today&apos;s — no warning.</p>
          </div>
          <div className="why-card">
            <h3>Orchestrators are the wrong tool.</h3>
            <p>Airflow, Prefect, Dagster schedule services, not row-level incrementality. dbt does, but only SQL.</p>
          </div>
          <div className="why-card">
            <h3>Iterate like a data scientist.</h3>
            <p>Fix the last step, re-run. Only that step re-pays. An edit-test loop for batches, not just notebooks.</p>
          </div>
        </div>
      </section>

      {/* ---------------- How ---------------- */}
      <section className="block block-tinted" id="how">
        <div className="block-inner">
          <div className="block-header-center">
            <Eyebrow>How it works</Eyebrow>
            <h2 className="block-title">Three guarantees, one diamond.</h2>
          </div>
          <div className="pillars">
            <Pillar icon={<Database size={22} />} title="Content-addressed caching">
              Every output lives at{' '}
              <Tooltip text="The address is determined by the step name, version, the hash of the input data, and the step's code hash. Same inputs + same code = same address = reused. Change any one and a new output is computed.">
                <code>hash(step, version, input_hash)</code>
              </Tooltip>
              . Re-runs recompute only what changed, at row granularity. Survives reordering, dedup, and appends.
            </Pillar>
            <Pillar icon={<History size={22} />} title="An append-only run ledger">
              Every run, lane, and event recorded immutably in{' '}
              <Tooltip text="A SQLite database inside .rubedo/. Workers can crash mid-run without corrupting already-committed state, because every write is an insert, never an update.">
                SQLite
              </Tooltip>
              . Workers can die at any point without corrupting committed state. Lineage edges connect each output to what produced it.
            </Pillar>
            <Pillar icon={<Eye size={22} />} title="Preview before you run">
              <Tooltip text="A read-only dry-run that walks every lane and prints what would happen: reuse, execute, blocked, stale, code-drift. No writes, no side effects.">
                <code>p.plan()</code>
              </Tooltip>{' '}
              tells you what <code>p.run()</code> would do to every lane and why — without writing anything.
            </Pillar>
          </div>

          <div className="shapes-grid">
            <div className="code-side">
              <div className="snippet-label">newsroom.py</div>
              <CodeBlock code={NEWSROOM_CODE} language="python" className="code-step" />
            </div>
            <div className="diagram-side">
              <div className="snippet-label">p.describe(format=&quot;ascii&quot;): live engine output</div>
              <CodeBlock code={DIAGRAM_CODE} language="text" className="diagram-block" />
              <div className="shape-legend">
                <span className="legend-item">
                  <Tooltip text="1:N fan-out — the step yields multiple payloads, each minting its own content-addressed lane. A folder scan or CSV reader is an expand root.">
                    <span className="legend-tag">expand</span>
                  </Tooltip>
                  1 : N
                </span>
                <span className="legend-item">
                  <Tooltip text="N-way equijoin — matches lanes from parent steps on indexed fields, minting one lane per matched tuple.">
                    <span className="legend-tag">join</span>
                  </Tooltip>
                  N : N
                </span>
                <span className="legend-item">
                  <Tooltip text="N:1 fan-in — folds all surviving lanes from the parent into a single output. Use group_key to partition into one output per unique field value.">
                    <span className="legend-tag">reduce</span>
                  </Tooltip>
                  N : 1
                </span>
              </div>
            </div>
          </div>

          <div className="two-col flaky-row">
            <div className="flaky-code-wrap">
              <CodeBlock code={RETRY_CODE} language="python" className="code-step" />
            </div>
            <ul className="features">
              <Feature icon={<Repeat size={18} />} title="Retries, narrow by type">
                Only exceptions matching{' '}
                <Tooltip text="A tuple of exception classes. Only those retry — a deterministic ValueError won't multiply cost. Every attempt lands in the event log.">
                  <code>retry_on</code>
                </Tooltip>{' '}
                retry, so a deterministic bug doesn&apos;t multiply cost. Every attempt lands in the event log.
              </Feature>
              <Feature icon={<ShieldCheck size={18} />} title="Assertions guard downstream">
                Run against the output before it commits. Bad data never propagates; the lane stops here.
              </Feature>
              <Feature icon={<History size={18} />} title="stale_after refreshes the clock">
                Past the{' '}
                <Tooltip text="A time-to-live like '24h'. Past this age the cached output is considered stale and the step re-executes. Different bytes supersede; identical bytes just refresh the clock.">
                  <code>stale_after</code>
                </Tooltip>{' '}
                TTL the step re-executes. Different bytes supersede; identical bytes just refresh.
              </Feature>
            </ul>
          </div>
        </div>
      </section>

      {/* ---------------- Get started ---------------- */}
      <section className="block" id="start">
        <Eyebrow>Get started</Eyebrow>
        <h2 className="block-title">Install. Define. Run. Run again.</h2>
        <div className="start-steps">
          <CodeBlock code="pip install rubedo" language="bash" />
          <div className="start-arrow"><ArrowRight size={18} /></div>
          <CodeBlock code={START_CODE} language="python" className="code-step" />
        </div>
        <p className="block-lede">
          Ingestion is a parentless{' '}
          <Tooltip text="A step whose parameters name no other step is a root, and a generator root infers shape='expand': it yields multiple payloads, each minting its own content-addressed lane. A folder scan, a csv.DictReader loop, a SQL SELECT all start this way.">
            <code>@p.step</code> generator
          </Tooltip>{' '}
          that yields a payload per item: a folder scan, a <code>csv.DictReader</code> loop, a SQL{' '}
          <code>SELECT</code>. Each row mints its own content-addressed lane. To find it later by a human field,{' '}
          <Tooltip text="Stores the field's value for O(1) lookup by value — used by join (to match lanes), group_key (to partition reduces), and selection queries (to find lanes by indexed field).">
            <code>index</code>
          </Tooltip>{' '}
          that field.
        </p>
      </section>

      {/* ---------------- Closing CTA ---------------- */}
      <section className="closing">
        <div className="closing-inner">
          <h2>Recompute only what changed. Pay for nothing twice.</h2>
          <p>
            A data-science loop for batches. Runs locally today; a managed cloud runtime is coming,
            so the same pipelines scale without a rewrite.
          </p>
          <div className="hero-cta">
            <a className="btn btn-primary" href={DOCS_URL}>
              Read the docs <ArrowRight size={16} />
            </a>
            <a className="btn btn-outline" href={`${GITHUB_URL}/tree/main/examples`} target="_blank" rel="noreferrer">
              Browse the examples
            </a>
          </div>
        </div>
      </section>

      {/* ---------------- Footer ---------------- */}
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
            <a href={`${GITHUB_URL}/tree/main/examples`} target="_blank" rel="noreferrer">Examples</a>
            <a href={`${DOCS_URL}notes/invariants/`} target="_blank" rel="noreferrer">Invariants</a>
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
