import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchRuns } from '../api';

export default function Runs() {
  const [runs, setRuns] = useState<any[]>([]);

  useEffect(() => {
    fetchRuns().then(setRuns);
  }, []);

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Runs</h1>
      </div>
      <div className="card table-container">
        <table>
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Kind</th>
              <th>Status</th>
              <th>Started</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {runs.map(r => (
              <tr key={r.id}>
                <td>{r.id}</td>
                <td>{r.kind}</td>
                <td>
                  <span className={`badge badge-${r.status === 'succeeded' ? 'success' : r.status === 'failed' ? 'error' : 'warning'}`}>
                    {r.status}
                  </span>
                </td>
                <td>{new Date(r.started_at).toLocaleString()}</td>
                <td>
                  <Link to={`/runs/${r.id}`} className="btn btn-outline">View</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
