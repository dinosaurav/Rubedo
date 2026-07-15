import { useEffect, useState } from 'react';
import { fmtDuration, durationMs, runStatusClass } from '../format';
import { fetchPipelines } from '../api';
import DagView from '../components/DagView';

export default function Pipelines() {
  const [pipelines, setPipelines] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchPipelines()
      .then(data => setPipelines(data))
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div>Loading pipelines...</div>;
  if (error) return <div className="page-container">API unreachable: {error}</div>;

  return (
    <div className="page-container">
      <div className="page-header" style={{ flexDirection: 'column', alignItems: 'flex-start', gap: '0.5rem' }}>
        <h1 className="page-title">Pipelines</h1>
        <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem' }}>
          Derived from the run ledger — a pipeline appears here once it has run,
          shown with the DAG its latest run recorded.
        </p>
      </div>

      {pipelines.length === 0 && (
        <div className="card">No runs yet — run a pipeline from Python and it will appear here.</div>
      )}

      {pipelines.map(p => (
        <div key={p.id} className="card" style={{ marginBottom: '1.5rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: '0.5rem' }}>
            <h2 style={{ margin: 0 }}>
              {p.definition?.name ?? p.id}{' '}
              <code style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{p.id}</code>
            </h2>
            <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>
              <code>{p.source_id}</code>
              {' · '}{p.run_count} run{p.run_count === 1 ? '' : 's'}
              {p.last_run_at && (
                <>
                  {' · latest: '}
                  <a href={`/runs/${p.last_run_id}`} className={`badge badge-${runStatusClass(p.last_run_status)}`} style={{ textDecoration: 'none' }}>
                    {p.last_run_status}
                  </a>
                  {' '}
                  {new Date(p.last_run_at).toLocaleString()}
                  {p.last_run_finished_at && ` (${fmtDuration(durationMs(p.last_run_at, p.last_run_finished_at))})`}
                </>
              )}
            </div>
          </div>

          {p.definition?.steps?.length ? (
            <div style={{ marginTop: '1rem' }}>
              <DagView steps={p.definition.steps} pipelineId={p.id} />
            </div>
          ) : (
            <p style={{ color: 'var(--text-muted)', marginTop: '1rem' }}>
              No definition snapshot recorded (run predates snapshots).
            </p>
          )}
        </div>
      ))}
    </div>
  );
}
