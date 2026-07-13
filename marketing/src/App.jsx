import React from 'react'
import {
  ArrowRight, Database, GitBranch, History, Layers, Plug,
  ShieldCheck, Terminal, Zap, Repeat, Filter, Search,
} from 'lucide-react'
import OuroborosLogo from './components/OuroborosLogo'
import './index.css'

const GITHUB_URL = 'https://github.com/dinosaurav/Rubedo'
// import.meta.env.BASE_URL follows Vite's `base` config (see vite.config.js),
// so this resolves correctly whether the site is served at `/`, `/Rubedo/`,
// or a future custom domain — never hardcode `/docs/`.
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
          <a href="#pain">Why</a>
          <a href="#model">The model</a>
          <a href="#shapes">Shapes</a>
          <a href="#flaky">Flaky &amp; expensive</a>
          <a href="#start">Get started</a>
          <a href={DOCS_URL}>Docs</a>
          <a className="btn btn-outline btn-sm" href={GITHUB_URL} target="_blank" rel="noreferrer">
            GitHub <ArrowRight size={14} />
          </a>
        </nav>
      </header>

      {/* ---------------- Hero ---------------- */}
      <section className="hero" id="top">
        <div className="hero-inner">
          <div className="badges">
            <span className="badge badge-info">Python 3.11+</span>
            <span className="badge badge-success">MIT</span>
            <span className="badge badge-warning">pre-1.0</span>
          </div>
          <h1>
            Stop paying to re-run
            <br />
            <span className="hero-accent">non-idempotent</span> pipelines.
          </h1>
          <p className="lede">
            Rubedo is a local-first batch engine that runs a DAG of Python steps over a collection —
            files, CSV rows, SQL rows — with <strong>dbt-style state</strong>. Every output lives at a
            content-addressed address, so re-running recomputes <em>only what actually changed</em>,
            at row granularity. An append-only ledger records what happened to every item in every run.
          </p>
          <p className="lede lede-sub">
            Built for steps you can&apos;t afford to re-run: LLM calls, scraping, paid APIs.
          </p>
          <div className="hero-cta">
            <a className="btn btn-primary" href="#start">
              Get started <ArrowRight size={16} />
            </a>
            <a className="btn btn-outline" href={GITHUB_URL} target="_blank" rel="noreferrer">
              View on GitHub
            </a>
          </div>

          <div className="hero-reuse">
            <div className="snippet-label">Run it twice — watch everything reuse</div>
            <pre className="pre-block reuse-block">
{`# first run          created=8  reused=0
# second run         created=0  reused=8     # nothing changed, nothing recomputed
# edit one file...   created=2  reused=6     # only that file's lanes re-run`}
            </pre>
          </div>
        </div>
      </section>

      {/* ---------------- Pain ---------------- */}
      <section className="block" id="pain">
        <Eyebrow>Why Rubedo exists</Eyebrow>
        <h2 className="block-title">
          If you&apos;ve processed a thousand rows through an LLM and then needed to fix the last step,
          you know the failure modes.
        </h2>
        <div className="why-grid">
          <div className="why-card">
            <h3>Re-running re-pays.</h3>
            <p>
              Without durable per-item state, every code tweak or crash means re-running every paid
              API call before it. A one-line fix costs you the whole run again.
            </p>
          </div>
          <div className="why-card">
            <h3><code>functools.cache</code> doesn&apos;t know your DAG.</h3>
            <p>
              Ad-hoc caches can&apos;t tell you <em>why</em> something recomputed, can&apos;t invalidate
              downstream when an input changes, and silently go stale when the code does.
            </p>
          </div>
          <div className="why-card">
            <h3>Orchestrators are the wrong tool.</h3>
            <p>
              Airflow, Prefect, Dagster schedule and monitor <em>services</em>. They don&apos;t give you
              row-level, content-addressed incrementality inside a local script. dbt does — but only
              for SQL.
            </p>
          </div>
          <div className="why-card">
            <h3>Make tracks files. Rubedo tracks content.</h3>
            <p>
              Row granularity, content-addressed lanes, and a queryable history of every run — not just
              the latest output. Identical rows collapse to one lane; an edited row shows up as
              removed + created.
            </p>
          </div>
        </div>
        <div className="thesis">
          Rubedo is a <strong>library, not a platform</strong>: no daemon, no registry, no magic module.
          The engine never imports your code — you import the engine. State lives in a{' '}
          <code>.rubedo/</code> directory (SQLite ledger + content-addressed object store), created on
          first run and gitignored automatically.
        </div>
      </section>

      {/* ---------------- The model ---------------- */}
      <section className="block" id="model">
        <Eyebrow>The model</Eyebrow>
        <h2 className="block-title">Three guarantees, in one library.</h2>
        <div className="pillars">
          <Pillar icon={<Database size={22} />} title="Content-addressed caching">
            Every output lives at <code>hash(step, version, input_hash)</code>. Re-running recomputes
            only what actually changed — at row granularity, surviving reordering, dedup, and appends
            for free.
          </Pillar>
          <Pillar icon={<History size={22} />} title="An append-only run ledger">
            Every run, every lane, every event is recorded immutably. Workers can die at any point
            without corrupting committed state. Lineage edges connect each output to the outputs it was
            derived from.
          </Pillar>
          <Pillar icon={<GitBranch size={22} />} title="Surgical invalidation">
            A query language selects outputs by what they <em>computed</em> (<code>company:acme</code>).
            Invalidate the match — or widen it <code>--downstream</code> to the full derived closure.
            <code>trace</code> previews the blast radius first. A tombstone, never a delete: history
            stays intact.
          </Pillar>
        </div>
        <div className="plan-callout">
          <Terminal size={18} />
          <span>
            <strong>p.plan()</strong> is a read-only dry-run: it tells you what <code>p.run()</code>{' '}
            would do to every lane and <em>why</em> — reuse, execute, blocked, filtered, stale,
            code-drift — without writing anything.
          </span>
        </div>
      </section>

      {/* ---------------- Shapes ---------------- */}
      <section className="block block-tinted" id="shapes">
        <Eyebrow>Shapes — map, reduce, expand, join</Eyebrow>
        <h2 className="block-title">A diamond in eight lines. Multi-source join, fan-out, fold-back.</h2>
        <p className="block-lede">
          Four shapes cover fan-in, fan-out, and joins. Here a real pipeline: two sources meet in a
          join, fan out into per-article lanes (scraped once, then cached), and fold back into a
          per-region digest. The scraping step carries retry, rate-limit, and a freshness TTL — because
          that&apos;s the flaky, expensive one.
        </p>
        <div className="shapes-grid">
          <div className="code-side">
            <div className="snippet-label">newsroom.py</div>
            <pre className="pre-block code-step">
{`from rubedo import pipeline, step, source
import csv

@source
def feeds():
    with open("feeds.csv") as f:
        yield from csv.DictReader(f)

@source
def publishers():
    with open("publishers.csv") as f:
        yield from csv.DictReader(f)

@step(depends_on=["feeds"], index=["publisher"])
def feed(feeds: dict) -> dict:
    return {"feed_id": feeds["feed_id"], "publisher": feeds["publisher"]}

@step(depends_on=["publishers"], index=["publisher"])
def publisher(publishers: dict) -> dict:
    return {"publisher": publishers["publisher"], "region": publishers["region"]}

# two sources meet — one lane per matched pair
@step(shape="join", depends_on=["feed", "publisher"],
      join_on={"feed": "publisher", "publisher": "publisher"})
def feed_meta(feed: dict, publisher: dict) -> dict:
    return {"feed_id": feed["feed_id"], "region": publisher["region"]}

# fan out: a lane per article. cached against its parent,
# so a re-run re-scrapes nothing. this is the flaky one.
@step(depends_on=["feed_meta"], shape="expand", index=["region"],
      retries=3, retry_on=(TimeoutError, ConnectionError),
      retry_backoff=2, rate_limit="30/min", stale_after="24h")
def articles(feed_meta: dict):
    for title in scrape(feed_meta["feed_id"]):
        yield {"title": title, "region": feed_meta["region"]}

# fold back: one digest per region
@step(depends_on=["articles"], shape="reduce", group_key="region")
def digest(articles: dict) -> dict:
    titles = sorted(a["title"] for a in articles.values())
    return {"count": len(titles), "headlines": titles}

p = pipeline(name="newsroom",
             steps=[feeds, publishers, feed, publisher,
                    feed_meta, articles, digest])`}
            </pre>
          </div>
          <div className="diagram-side">
            <div className="snippet-label">p.describe(format="ascii") — live engine output</div>
            <pre className="pre-block diagram-block">
{`Pipeline 'newsroom'
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
└─────────────────┘`}
            </pre>
            <div className="shape-legend">
              <span className="legend-item"><span className="legend-tag">expand</span> 1 : N</span>
              <span className="legend-item"><span className="legend-tag">join</span> N : N</span>
              <span className="legend-item"><span className="legend-tag">reduce</span> N : 1</span>
            </div>
          </div>
        </div>
        <div className="shape-notes">
          Multiple sources are just several <code>expand</code> roots in one pipeline — nothing extra to
          declare, and <code>join</code> doesn&apos;t care that its parents are roots. A pipeline
          doesn&apos;t even need a source-shaped root: a <code>map</code> step with no{' '}
          <code>depends_on</code> mints a single lane from its params — feed a value <em>into</em> the
          head instead of scanning for one.
        </div>
      </section>

      {/* ---------------- Flaky & expensive ---------------- */}
      <section className="block" id="flaky">
        <Eyebrow>Built for flaky, expensive work</Eyebrow>
        <h2 className="block-title">Steps carry their own execution policies.</h2>
        <div className="two-col">
          <pre className="pre-block code-step">
{`@step(retries=3, retry_on=(TimeoutError, ConnectionError),
      retry_backoff=2, rate_limit="30/min",
      stale_after="24h",
      assertions=[check_price_positive],
      executor="process")
def enrich(row: dict): ...`}
          </pre>
          <ul className="features">
            <Feature icon={<Repeat size={18} />} title="Retries, narrow by type">
              Only exceptions matching <code>retry_on</code> retry — keep it narrow so you don&apos;t
              multiply cost on a deterministic bug. Every attempt lands in the event log.
            </Feature>
            <Feature icon={<Zap size={18} />} title="Rate-limited across all workers">
              <code>rate_limit="30/min"</code> paces the step evenly across its workers, retries
              included. No thundering herd on a paid API.
            </Feature>
            <Feature icon={<History size={18} />} title="stale_after refreshes the clock">
              Past the TTL the step re-executes. Different bytes supersede (downstream recomputes);
              identical bytes just refresh — natural for scraped or time-sensitive data.
            </Feature>
            <Feature icon={<ShieldCheck size={18} />} title="Assertions guard downstream">
              Run against the output before it commits. Bad data never propagates — the step fails and
              the bad lane stops here.
            </Feature>
            <Feature icon={<Filter size={18} />} title="Filter, don&apos;t re-decide">
              <code>Filtered(reason=...)</code> declines an item — the verdict is cached like any
              output, so an expensive LLM-based filter runs once per input, not once per run.
            </Feature>
            <Feature icon={<Terminal size={18} />} title="broad vs deep">
              <code>schedule="broad"</code> completes each step across all lanes before the next —
              inspect a paid step&apos;s full output before the next stage spends.{' '}
              <code>"deep"</code> lets items race ahead through 1:1 steps — first results as early as
              possible.
            </Feature>
          </ul>
        </div>
      </section>

      {/* ---------------- Get started ---------------- */}
      <section className="block block-tinted" id="start">
        <Eyebrow>Easy to get started</Eyebrow>
        <h2 className="block-title">Install. Define. Run. Run again.</h2>
        <div className="start-steps">
          <pre className="pre-block">{`pip install rubedo`}</pre>
          <div className="start-arrow"><ArrowRight size={18} /></div>
          <pre className="pre-block code-step">
{`from rubedo import step, pipeline

@step(shape="expand")
def scan():
    for name in sorted(os.listdir("input")):
        yield {"path": name, "text": open(name).read()}

@step(depends_on=["scan"])
def count_lines(scan: dict):
    return {"line_count": len(scan["text"].splitlines())}

p = pipeline(name="count-lines", steps=[scan, count_lines])
print(p.plan())      # dry-run: what would run, and why
summary = p.run()    # execute
print(f"created={summary.created_count} reused={summary.reused_count}")`}
          </pre>
        </div>
        <p className="block-lede">
          No <code>Source</code> protocol, no source classes. Ingestion is just a parentless{' '}
          <code>@step(shape="expand")</code> (a <code>@source</code> for short) that yields a payload
          per item. A folder scan, a <code>csv.DictReader</code> loop, a SQL <code>SELECT</code>, a
          cloud object store listing — all just generators. Each row mints its own content-addressed
          lane; to find it later by a human field, index that field.
        </p>
      </section>

      {/* ---------------- Local + cloud ---------------- */}
      <section className="block" id="flows">
        <Eyebrow>Local flows &amp; the cloud</Eyebrow>
        <h2 className="block-title">Local-first by default. Cloud-ready by being plain Python.</h2>
        <div className="pillars">
          <Pillar icon={<Terminal size={22} />} title="Easy local flows">
            SQLite ledger + content-addressed object store in <code>.rubedo/</code>. A CLI browses and
            invalidates against the local ledger — <code>rubedo ls</code>,{' '}
            <code>rubedo show &lt;run&gt; --failed</code>,{' '}
            <code>rubedo trace &quot;company:acme&quot;</code>, <code>rubedo du</code>,{' '}
            <code>rubedo gc</code> — and a read-only web dashboard renders runs, materializations, and
            lineage.
          </Pillar>
          <Pillar icon={<Search size={22} />} title="Search &amp; trace what you computed">
            <code>@step(index=["company", "meta.region"])</code> extracts value fields into a search
            index at commit time, so you select by what a step <em>computed</em> — regardless of file
            names or row keys. <code>trace</code> walks lineage both ways: what produced this, and what
            did it contaminate?
          </Pillar>
          <Pillar icon={<Plug size={22} />} title="Connect to the cloud">
            Ingestion is a generator over whatever you already use — a warehouse <code>SELECT</code>, an
            S3 listing, an LLM client. The bundled examples call OpenRouter, GitHub, Hacker News,
            Open-Meteo and Project Gutenberg with only the standard library. The read-only FastAPI
            server deploys wherever the dashboard needs to read from.
          </Pillar>
          <Pillar icon={<Layers size={22} />} title="Retention without losing facts">
            The store keeps every generation forever by default. <code>pipeline(retention=5)</code>{' '}
            prunes by run recency; <code>rubedo gc --max-bytes 2GiB --delete</code> reconciles a global
            budget. Retention deletes <strong>bytes, never facts</strong> — demoted generations keep
            their ledger rows, and a pruned lane restores itself on the next run.
          </Pillar>
        </div>
      </section>

      {/* ---------------- Closing CTA ---------------- */}
      <section className="closing">
        <div className="closing-inner">
          <h2>Recompute only what changed. Pay for nothing twice.</h2>
          <p>
            Pre-1.0 and moving fast. The core model — content-addressed lanes, the four shapes,
            multi-source, the ledger protocol — is designed and built.
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
            Pre-1.0 — the API is unstable; schema changes mean deleting <code>.rubedo/</code> and
            re-running. MIT licensed.
          </div>
        </div>
      </footer>
    </div>
  )
}

export default App
