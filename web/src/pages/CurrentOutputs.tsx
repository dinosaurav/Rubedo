import React, { useEffect, useState } from 'react';
import { fetchCurrentOutputs } from '../api';
import { Link } from 'react-router-dom';

export default function CurrentOutputs() {
  const [outputs, setOutputs] = useState<any[]>([]);

  useEffect(() => {
    fetchCurrentOutputs().then(setOutputs);
  }, []);

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Current Outputs</h1>
      </div>
      <div className="card table-container">
        <table>
          <thead>
            <tr>
              <th>Source Folder</th>
              <th>Coordinate</th>
              <th>Step</th>
              <th>Code Version</th>
              <th>Output Address</th>
              <th>Updated At</th>
            </tr>
          </thead>
          <tbody>
            {outputs.map(o => (
              <tr key={o.id}>
                <td>{o.source_folder}</td>
                <td>{o.coordinate}</td>
                <td>{o.step}</td>
                <td>{o.code_version}</td>
                <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>
                  {o.output_address ? <Link to={`/objects/${o.output_address}`}>{o.output_address.slice(0, 16)}...</Link> : '-'}
                </td>
                <td>{new Date(o.updated_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
