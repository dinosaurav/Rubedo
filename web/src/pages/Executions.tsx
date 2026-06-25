import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchExecutions } from '../api';
import { CheckCircle, Clock, XCircle, Loader2 } from 'lucide-react';

export default function Executions() {
  const [executions, setExecutions] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchExecutions().then(data => {
      setExecutions(data);
      setLoading(false);
    });
  }, []);

  if (loading) return <div>Loading...</div>;

  return (
    <div className="page-container">
      <div className="page-header">
        <h1>Execution Requests</h1>
      </div>

      <div className="card">
        <table className="data-table">
          <thead>
            <tr>
              <th>Status</th>
              <th>Execution ID</th>
              <th>Processor</th>
              <th>Requested At</th>
              <th>Run ID</th>
            </tr>
          </thead>
          <tbody>
            {executions.map(ex => (
              <tr key={ex.id}>
                <td>
                  <span className={`status-badge ${ex.status}`}>
                    {ex.status === 'succeeded' && <CheckCircle size={14} />}
                    {ex.status === 'failed' && <XCircle size={14} />}
                    {ex.status === 'queued' && <Clock size={14} />}
                    {ex.status === 'running' && <Loader2 size={14} className="spin" />}
                    {ex.status}
                  </span>
                </td>
                <td><Link to={`/executions/${ex.id}`}>{ex.id}</Link></td>
                <td><code>{ex.processor_id}</code></td>
                <td>{new Date(ex.requested_at).toLocaleString()}</td>
                <td>
                  {ex.run_id ? (
                    <Link to={`/runs/${ex.run_id}`}>{ex.run_id.substring(0,8)}...</Link>
                  ) : '-'}
                </td>
              </tr>
            ))}
            {executions.length === 0 && (
              <tr>
                <td colSpan={5} style={{ textAlign: 'center', padding: '2rem' }}>
                  No execution requests found.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
