import { useEffect, useState } from 'react';
import { fetchProcessors } from '../api';

export default function Processors() {
  const [processors, setProcessors] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchProcessors().then(data => {
      setProcessors(data);
      setLoading(false);
    });
  }, []);

  if (loading) return <div>Loading processors...</div>;

  return (
    <div className="page-container">
      <div className="page-header">
        <h1>Processors</h1>
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
              <th>Source Override</th>
            </tr>
          </thead>
          <tbody>
            {processors.map(p => (
              <tr key={p.id}>
                <td><code>{p.id}</code></td>
                <td>{p.name}</td>
                <td><code>{p.source_id}</code></td>
                <td><code>{p.code_version}</code></td>
                <td>{p.workers}</td>
                <td>{p.allow_source_override ? 'allowed' : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
