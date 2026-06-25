import React, { useEffect, useState } from 'react';
import { fetchMaterializations } from '../api';
import { Link } from 'react-router-dom';

export default function Materializations() {
  const [mats, setMats] = useState<any[]>([]);

  useEffect(() => {
    fetchMaterializations().then(setMats);
  }, []);

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Materializations</h1>
      </div>
      <div className="card table-container">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Step</th>
              <th>Code Version</th>
              <th>Output Address</th>
              <th>Created</th>
              <th>Status</th>
              <th>Metadata</th>
            </tr>
          </thead>
          <tbody>
            {mats.map(m => (
              <tr key={m.id}>
                <td>{m.id}</td>
                <td>{m.step}</td>
                <td>{m.code_version}</td>
                <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>
                  {m.output_address ? <Link to={`/objects/${m.output_address}`}>{m.output_address.slice(0, 16)}...</Link> : '-'}
                </td>
                <td>{new Date(m.created_at).toLocaleString()}</td>
                <td>
                  {m.invalidated_at ? (
                    <span className="badge badge-error">Invalidated</span>
                  ) : (
                    <span className="badge badge-success">Valid</span>
                  )}
                </td>
                <td style={{ fontSize: '0.75rem', fontFamily: 'monospace' }}>
                  {m.metadata_json}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
