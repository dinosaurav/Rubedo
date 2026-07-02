import { useEffect, useState } from 'react';
import { fetchPipelines } from '../api';

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
      </div>

      <div className="card">
        <table className="data-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Name</th>
              <th>Source</th>
              <th>Code Version</th>
              <th>Workers</th>
            </tr>
          </thead>
          <tbody>
            {pipelines.map(p => (
              <tr key={p.id}>
                <td><code>{p.id}</code></td>
                <td>{p.name}</td>
                <td><code>{p.source_id}</code></td>
                <td><code>{p.code_version}</code></td>
                <td>{p.workers}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
