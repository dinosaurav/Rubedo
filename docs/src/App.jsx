import React from 'react'
import OuroborosLogo from './components/OuroborosLogo'
import BlueprintDiagram from './components/BlueprintDiagram'
import Tooltip from './components/Tooltip'
import './index.css'

function App() {
  return (
    <div className="container">
      <header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '4rem', paddingBottom: '1rem', borderBottom: '1px solid var(--border-color)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          <OuroborosLogo size={40} />
          <h1 style={{ margin: 0, fontSize: '1.5rem', letterSpacing: '0.1em' }}>RUBEDO</h1>
          <span style={{ fontSize: '0.75rem', padding: '0.1rem 0.4rem', border: '1px solid var(--border-color)', marginLeft: '0.5rem', opacity: 0.8 }}>v0.1.0</span>
        </div>
        <nav style={{ display: 'flex', gap: '1.5rem', alignItems: 'center' }}>
          <a href="#features">Features</a>
          <a href="#tutorials">Tutorials</a>
          <a href="#guides">How-To</a>
          <a href="#concepts">Concepts</a>
          <a href="#reference">Reference</a>
          <div style={{ width: '1px', height: '1.2rem', backgroundColor: 'var(--border-color)', margin: '0 0.5rem' }}></div>
          <a href="#search" style={{ opacity: 0.7 }}>Search ⌘K</a>
          <a href="https://github.com/dinosaurav/Rubedo" target="_blank" rel="noreferrer">GitHub</a>
        </nav>
      </header>

      <main>
        <section style={{ marginBottom: '4rem' }}>
          <h2 style={{ fontSize: '3rem', maxWidth: '800px', marginBottom: '1.5rem', color: 'var(--accent-primary)' }}>
            Effortless pipeline orchestration in pure Python.
          </h2>
          <p style={{ fontSize: '1.25rem', maxWidth: '600px', marginBottom: '2.5rem' }}>
            Build general-purpose data pipelines with incredible ease. Rubedo uses <Tooltip text="Caching based on the exact hash of the input data and step code, ensuring you only compute what changed.">content-addressed hashing</Tooltip> to manage state for you—perfect for expensive or flaky tasks like parsing, web scraping, and running LLM models. Designed as a local-first engine that scales seamlessly to distributed processing workloads.
          </p>
          
          <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
            <code style={{ fontSize: '1.1rem', padding: '0.75rem 1rem', border: '1px solid var(--border-color)', color: 'var(--text-primary)', backgroundColor: 'transparent' }}>
              pip install rubedo
            </code>
            <a href="#quickstart" className="btn btn-solid">Quickstart</a>
            <a href="https://github.com/dinosaurav/Rubedo" className="btn" target="_blank" rel="noreferrer">GitHub</a>
          </div>
        </section>

        <section id="features" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '2rem', marginBottom: '5rem' }}>
          <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
            <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>The Iterative Loop</h3>
            <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
              Brings a fast, data-science-style workflow to your local environment. Tweak your code, test new prompts, and let the cache instantly skip over the steps you've already perfected. When data is bad, surgically invalidate just that row and recompute exactly what depends on it.
            </p>
          </div>
          <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
            <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Full Lineage & Observability</h3>
            <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
              Every execution is durably recorded in an append-only ledger. Trace the exact inputs, parameters, and code version that produced any output, giving you complete visibility into the lifecycle of your data without writing a single logging statement.
            </p>
          </div>
          <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
            <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Resilient by Default</h3>
            <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
              Built for the real world. Easily handle flaky APIs, connection timeouts, and strict rate limits with declarative policies like <code>retries</code>, <code>rate_limit</code>, and <code>stale_after</code> natively attached to your steps.
            </p>
          </div>
          <div style={{ borderTop: '2px solid var(--accent-primary)', paddingTop: '1rem' }}>
            <h3 style={{ fontSize: '1.2rem', marginBottom: '0.75rem' }}>Scalable Batch Processing</h3>
            <p style={{ fontSize: '0.95rem', opacity: 0.9 }}>
              Process massive collections of files or CSV rows efficiently. Rubedo's local-first executor handles concurrency natively, laying the architectural groundwork for seamless distributed cluster scheduling as your pipelines scale.
            </p>
          </div>
        </section>

        <section style={{ marginBottom: '6rem' }}>
          <BlueprintDiagram />
        </section>

        <section id="diataxis-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '2rem', marginBottom: '4rem' }}>
          <div className="card">
            <h3 style={{ color: 'var(--accent-primary)', borderBottom: '1px solid var(--border-color)', paddingBottom: '0.5rem', marginBottom: '1rem' }}>Tutorials</h3>
            <p style={{ fontSize: '0.9rem', marginBottom: '1rem' }}>Step-by-step guides for absolute beginners.</p>
            <ul style={{ listStyleType: 'square', paddingLeft: '1.2rem', fontSize: '0.9rem' }}>
              <li><a href="#">Building your first pipeline</a></li>
              <li><a href="#">LLM Prompt Processing</a></li>
              <li><a href="#">Resilient Web Scraping</a></li>
            </ul>
          </div>

          <div className="card">
            <h3 style={{ color: 'var(--accent-primary)', borderBottom: '1px solid var(--border-color)', paddingBottom: '0.5rem', marginBottom: '1rem' }}>How-To Guides</h3>
            <p style={{ fontSize: '0.9rem', marginBottom: '1rem' }}>Task-oriented recipes for specific goals.</p>
            <ul style={{ listStyleType: 'square', paddingLeft: '1.2rem', fontSize: '0.9rem' }}>
              <li><a href="#">Handling API Rate Limits</a></li>
              <li><a href="#">Surgical Invalidation</a></li>
              <li><a href="#">Writing Custom Sources</a></li>
            </ul>
          </div>

          <div className="card">
            <h3 style={{ color: 'var(--accent-primary)', borderBottom: '1px solid var(--border-color)', paddingBottom: '0.5rem', marginBottom: '1rem' }}>Concepts</h3>
            <p style={{ fontSize: '0.9rem', marginBottom: '1rem' }}>High-level architecture and explanations.</p>
            <ul style={{ listStyleType: 'square', paddingLeft: '1.2rem', fontSize: '0.9rem' }}>
              <li><a href="#">The Run Ledger</a></li>
              <li><a href="#">Coordinates & Sources</a></li>
              <li><a href="#">Core Invariants</a></li>
            </ul>
          </div>

          <div className="card">
            <h3 style={{ color: 'var(--accent-primary)', borderBottom: '1px solid var(--border-color)', paddingBottom: '0.5rem', marginBottom: '1rem' }}>Reference</h3>
            <p style={{ fontSize: '0.9rem', marginBottom: '1rem' }}>Exhaustive technical API descriptions.</p>
            <ul style={{ listStyleType: 'square', paddingLeft: '1.2rem', fontSize: '0.9rem' }}>
              <li><a href="#">rubedo.spec (@step)</a></li>
              <li><a href="#">rubedo.sources</a></li>
              <li><a href="#">rubedo.runner</a></li>
            </ul>
          </div>
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
              Local-first batch processing engine for Python.
            </p>
          </div>
          <div>
            <h4 style={{ marginBottom: '1rem', color: 'var(--accent-primary)' }}>Project</h4>
            <ul style={{ listStyle: 'none', padding: 0, display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              <li><a href="#quickstart">Quickstart</a></li>
              <li><a href="https://github.com/dinosaurav/Rubedo" target="_blank" rel="noreferrer">GitHub Repository</a></li>
              <li><a href="https://github.com/dinosaurav/Rubedo/issues" target="_blank" rel="noreferrer">Issue Tracker</a></li>
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
