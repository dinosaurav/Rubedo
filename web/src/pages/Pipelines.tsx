import { useEffect, useState } from 'react';
import { fetchPipelines } from '../api';

function dagSummary(definition: any): string {
  if (!definition?.steps?.length) return '—';
  return definition.steps
    .map((s: any) => {
      const deps = s.depends_on?.length ? `${s.depends_on.join(',')} → ` : '';
      return `${deps}${s.name}@${s.version}${s.skip_cache ? ' (util)' : ''}`;
    })
    .join('  |  ');
}

export default function Pipelines() {
  const [pipelines, setPipelines] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchPipelines().then(data => {
      setPipelines(data);
      setLoading(false);
    });
  }, []);

  if (loading) return <div>Loading pipelines...</div>;

  return (
    <div className="page-container">
      <div className="page-header">
        <h1>Pipelines</h1>
        <p style={{ color: '#666', fontSize: '0.875rem' }}>
          Derived from the run ledger — a pipeline appears here once it has run.
        </p>
      </div>

      <div className="card">
        <table className="data-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Name</th>
              <th>Source</th>
              <th>Steps (latest run)</th>
              <th>Runs</th>
              <th>Last Run</th>
            </tr>
          </thead>
          <tbody>
            {pipelines.map(p => (
              <tr key={p.id}>
                <td><code>{p.id}</code></td>
                <td>{p.definition?.name ?? p.id}</td>
                <td><code>{p.source_id}</code></td>
                <td style={{ fontSize: '0.8rem' }}><code>{dagSummary(p.definition)}</code></td>
                <td>{p.run_count}</td>
                <td>{p.last_run_at ? new Date(p.last_run_at).toLocaleString() : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
