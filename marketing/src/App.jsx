import React from 'react'
import OuroborosLogo from './components/OuroborosLogo'
import BlueprintDiagram from './components/BlueprintDiagram'
import Tooltip from './components/Tooltip'
import './index.css'

const GITHUB_URL = 'https://github.com/dinosaurav/Rubedo'

function App() {
  return (
    <div className="container">
      <header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '4rem', paddingBottom: '1rem', borderBottom: '1px solid var(--border-color)', flexWrap: 'wrap', gap: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          <OuroborosLogo size={40} />
          <h1 style={{ margin: 0, fontSize: '1.5rem', letterSpacing: '0.1em' }}>RUBEDO</h1>
          <span style={{ fontSize: '0.75rem', padding: '0.1rem 0.4rem', border: '1px solid var(--border-color)', marginLeft: '0.5rem', opacity: 0.8 }}>pre-1.0</span>
        </div>
        <nav style={{ display: 'flex', gap: '1.5rem', alignItems: 'center', flexWrap: 'wrap' }}>
          <a href="#why">Why</a>
          <a href="#quickstart">Quickstart</a>
          <a href="#shapes">Shapes</a>
          <a href="#features">Features</a>
          <div style={{ width: '1px', height: '1.2rem', backgroundColor: 'var(--border-color)', margin: '0 0.5rem' }}></div>
          <a href={GITHUB_URL} target="_blank" rel="noreferrer">GitHub</a>
          <a href="/docs/" className="btn" style={{ padding: '0.4rem 0.9rem' }}>Docs</a>
        </nav>
      </header>

      <main>
        <section style={{ marginBottom: '3rem' }}>
          <h2 style={{ fontSize: '2.75rem', maxWidth: '820px', marginBottom: '1.5rem', color: 'var(--accent-primary)' }}>
            Content-addressed caching and run history for Python batch pipelines.
          </h2>
          <p style={{ fontSize: '1.2rem', maxWidth: '680px', marginBottom: '1.5rem' }}>
            Rubedo is a local-first batch engine: define a DAG of Python steps over files, CSV rows, or SQL
            rows, and run it with dbt-style state. Every step output is stored immutably at a{' '}
            <Tooltip text="hash(step, code_version, input_hash) — identical inputs and code hit the same address; anything else gets a new one.">
              deterministic address
            </Tooltip>
            , so re-running a pipeline recomputes only what actually changed.
          </p>
          <p style={{ fontSize: '1.05rem', maxWidth: '680px', marginBottom: '2.5rem', opacity: 0.9 }}>
            Built for <strong>non-idempotent, expensive steps</strong> — LLM calls, scraping, paid APIs —
            where "just re-run the script" means paying for everything again and hoping the results come
            back the same.
          </p>

          <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', flexWrap: 'wrap', marginBottom: '1rem' }}>
            <code style={{ fontSize: '1.1rem', padding: '0.75rem 1rem', border: '1px solid var(--border-color)', color: 'var(--text-primary)', backgroundColor: 'transparent' }}>
              pip install rubedo
            </code>
            <a href="/docs/" className="btn btn-solid">Read the docs</a>
            <a href={GITHUB_URL} className="btn" target="_blank" rel="noreferrer">GitHub</a>
          </div>
          <p style={{ fontSize: '0.85rem', opacity: 0.7 }}>
            Requires Python 3.11+. MIT licensed. Pre-1.0 — the API is still moving, no migrations or
            compat shims yet.
          </p>
        </section>

        <section style={{ marginBottom: '5rem' }}>
          <pre><code>{`# first run          created=8  reused=0
# second run         created=0  reused=8   ← nothing changed, nothing recomputed
# edit one file...   created=2  reused=6   ← only that file's lanes re-run`}</code></pre>
        </section>

        <section id="why" style={{ marginBottom: '5rem' }}>
          <h2 className="section-heading">Why</h2>
          <p className="section-kicker">
            If you've ever processed a thousand rows through an LLM and then needed to fix the last step,
            you know the failure modes.
          </p>
          <ul style={{ listStyleType: 'square', paddingLeft: '1.2rem', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '1.5rem 2rem' }}>
            <li>
              <strong>Re-running re-pays.</strong> Without durable per-item state, every code tweak or crash
              means re-running every API call before it.
            </li>
            <li>
              <strong><code>functools.cache</code> and pickle files don't know your DAG.</strong> Ad-hoc
              caches can't tell you why something recomputed, can't invalidate downstream when an input
              changes, and silently go stale when the code does.
            </li>
            <li>
              <strong>Orchestrators are the wrong tool.</strong> Airflow/Prefect/Dagster schedule and monitor
              services; they don't give you row-level, content-addressed incrementality inside a local
              script. dbt does — but only for SQL.
            </li>
            <li>
              <strong>Make/Snakemake track files.</strong> Rubedo tracks content, at row granularity, with a
              queryable history of every run.
            </li>
          </ul>
        </section>

        <section id="quickstart" style={{ marginBottom: '5rem' }}>
          <h2 className="section-heading">Quickstart</h2>
          <p className="section-kicker">Pipelines are plain Python objects — define them wherever your code lives.</p>
          <pre><code>{`from rubedo import ProcessResult, step, pipeline, run, plan, describe

@step(name="read_lines", version="read-v1")
def read_lines(path: str):
    return {"lines": open(path).read().splitlines()}

@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    return ProcessResult(value={"line_count": len(read_lines["lines"])})

p = pipeline(id="count-lines", name="Count Lines", folder="input",
             steps=[read_lines, count_lines])

print(describe(p))            # the DAG, before ever running
print(plan(p))                # dry-run: what run() would do, and why
summary = run(p)              # execute
print(f"created={summary.created_count} reused={summary.reused_count}")`}</code></pre>
          <p style={{ fontSize: '0.9rem', opacity: 0.75, marginTop: '1rem' }}>
            See <a href={`${GITHUB_URL}/tree/main/examples/count_lines`} target="_blank" rel="noreferrer">examples/count_lines</a> in
            the repo, or the full walkthrough in the <a href="/docs/">docs</a>.
          </p>
        </section>

        <section id="shapes" style={{ marginBottom: '5rem' }}>
          <h2 className="section-heading">Four shapes</h2>
          <p className="section-kicker">By default a step is <code>map</code> — 1:1 per lane. Three more shapes cover fan-in, fan-out, and joins.</p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '1.5rem' }}>
            <div className="card">
              <h3 style={{ fontSize: '1.05rem', marginBottom: '0.5rem', fontFamily: 'monospace' }}>map <span style={{ opacity: 0.6, fontFamily: 'var(--font-primary)' }}>(default)</span></h3>
              <p style={{ fontSize: '0.9rem', opacity: 0.9 }}>1:1 per lane — the everyday step.</p>
            </div>
            <div className="card">
              <h3 style={{ fontSize: '1.05rem', marginBottom: '0.5rem', fontFamily: 'monospace' }}>reduce</h3>
              <p style={{ fontSize: '0.9rem', opacity: 0.9 }}>
                N:1 fan-in over a parent's surviving lanes. Add <code>group_key</code> to fan in per group
                instead of all at once.
              </p>
            </div>
            <div className="card">
              <h3 style={{ fontSize: '1.05rem', marginBottom: '0.5rem', fontFamily: 'monospace' }}>expand</h3>
              <p style={{ fontSize: '0.9rem', opacity: 0.9 }}>
                1:N — the step yields a payload per item and each becomes its own content-addressed
                downstream lane (fetch a feed → a lane per article).
              </p>
            </div>
            <div className="card">
              <h3 style={{ fontSize: '1.05rem', marginBottom: '0.5rem', fontFamily: 'monospace' }}>join</h3>
              <p style={{ fontSize: '0.9rem', opacity: 0.9 }}>
                An N-way equijoin across multiple sources, matched on an indexed field, minting one lane
                per matched tuple.
              </p>
            </div>
          </div>
        </section>

        <section id="features" style={{ marginBottom: '5rem' }}>
          <h2 className="section-heading">Features</h2>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '2rem' }}>
            <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
              <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Content-addressed caching</h3>
              <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
                Every output is stored at <code>hash(step, code_version, input_hash)</code>. Run twice and
                nothing recomputes; edit one file and only that file's lanes re-run.
              </p>
            </div>
            <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
              <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Search and surgical invalidation</h3>
              <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
                <code>@step(index=[...])</code> makes outputs searchable by what they computed, not just
                file names. <code>invalidate(Selection(...))</code> tombstones exactly the matched lanes —
                or their full downstream closure — and the next run recomputes exactly that set.
              </p>
            </div>
            <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
              <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Full lineage and observability</h3>
              <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
                <code>plan()</code> is a read-only dry-run of what <code>run()</code> would do and why.{' '}
                <code>trace()</code> follows lineage upstream to source items and downstream to everything
                derived from them — recorded in an append-only ledger, no logging statements required.
              </p>
            </div>
            <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
              <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Built for flaky, expensive work</h3>
              <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
                Declarative per-step policies: <code>retries</code>, <code>rate_limit</code>,{' '}
                <code>stale_after</code>, and <code>assertions</code> that stop bad data before it
                propagates downstream.
              </p>
            </div>
            <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
              <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Retention and garbage collection</h3>
              <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
                Every generation is kept forever by default. Set <code>retention=N</code> to prune by run
                recency, or reconcile on demand with <code>rubedo gc</code> — dry-run unless you pass{' '}
                <code>--delete</code>. Deletes bytes, never facts: ledger rows and lineage survive.
              </p>
            </div>
          </div>
        </section>

        <section style={{ marginBottom: '3rem' }}>
          <BlueprintDiagram />
          <p style={{ fontSize: '0.9rem', opacity: 0.75, maxWidth: '680px', margin: '1rem auto 0' }}>
            Rubedo is a library, not a platform: no daemon, no registry, no magic module. The engine never
            imports your code — you import the engine. State lives in a <code>.rubedo/</code> directory
            (SQLite ledger + content-addressed object store), created on first run.
          </p>
        </section>

        <section style={{ marginBottom: '5rem' }}>
          <h2 className="section-heading">Inspecting runs</h2>
          <p className="section-kicker">The CLI browses and invalidates against the local ledger.</p>
          <pre><code>{`rubedo ls                          # recent runs
rubedo show <run_id> --failed      # what broke, per lane
rubedo trace "company:acme"        # what produced it, what it contaminated
rubedo gc --max-bytes 2GiB         # dry-run against a budget, oldest runs first`}</code></pre>
          <p style={{ fontSize: '0.9rem', opacity: 0.75, marginTop: '1rem' }}>
            A read-only web dashboard browses runs, materializations, and lineage:{' '}
            <code>uv run uvicorn rubedo.server:app --reload</code>. Running, recomputing, and invalidation
            always happen from library code or the CLI — the UI never mutates state.
          </p>
        </section>

      </main>

      <footer style={{ marginTop: '6rem', paddingTop: '3rem', borderTop: '1px solid var(--border-color)', fontSize: '0.9rem' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '2rem', marginBottom: '3rem' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1rem' }}>
              <OuroborosLogo size={24} />
              <strong style={{ letterSpacing: '0.1em' }}>RUBEDO</strong>
            </div>
            <p style={{ opacity: 0.8, fontSize: '0.85rem', maxWidth: '250px' }}>
              Local-first batch engine for Python, built for steps you can't afford to re-run.
            </p>
          </div>
          <div>
            <h4 style={{ marginBottom: '1rem', color: 'var(--accent-primary)' }}>Project</h4>
            <ul style={{ listStyle: 'none', padding: 0, display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              <li><a href="#quickstart">Quickstart</a></li>
              <li><a href={GITHUB_URL} target="_blank" rel="noreferrer">GitHub Repository</a></li>
              <li><a href={`${GITHUB_URL}/issues`} target="_blank" rel="noreferrer">Issue Tracker</a></li>
            </ul>
          </div>
          <div>
            <h4 style={{ marginBottom: '1rem', color: 'var(--accent-primary)' }}>Docs</h4>
            <ul style={{ listStyle: 'none', padding: 0, display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              <li><a href="/docs/">Documentation</a></li>
              <li><a href="/docs/notes/producer-model/">Producer Model</a></li>
              <li><a href="/docs/notes/invariants/">Architecture &amp; Invariants</a></li>
            </ul>
          </div>
        </div>
        <div style={{ borderTop: '1px dotted var(--border-color)', paddingTop: '1.5rem', display: 'flex', justifyContent: 'space-between', opacity: 0.7, fontSize: '0.8rem' }}>
          <span>© 2026 The Rubedo Authors.</span>
          <span>MIT License</span>
        </div>
      </footer>
    </div>
  )
}

export default App
