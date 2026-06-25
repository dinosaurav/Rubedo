import React, { useEffect, useState } from 'react';
import { fetchRuns, diffRuns } from '../api';
import { Link } from 'react-router-dom';

export default function RunDiff() {
  const [runs, setRuns] = useState<any[]>([]);
  const [leftId, setLeftId] = useState('');
  const [rightId, setRightId] = useState('');
  const [diff, setDiff] = useState<any[]>([]);

  useEffect(() => {
    fetchRuns().then(r => {
      setRuns(r);
      if (r.length >= 2) {
        setLeftId(r[1].id);
        setRightId(r[0].id);
      }
    });
  }, []);

  const handleDiff = async () => {
    if (!leftId || !rightId) return;
    const res = await diffRuns(leftId, rightId);
    setDiff(res);
  };

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Compare Runs</h1>
      </div>

      <div className="card" style={{ marginBottom: '2rem' }}>
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
          <select className="form-control" value={leftId} onChange={e => setLeftId(e.target.value)}>
            <option value="">Select Left Run</option>
            {runs.map(r => <option key={r.id} value={r.id}>{r.id} ({new Date(r.started_at).toLocaleString()})</option>)}
          </select>
          <span>vs</span>
          <select className="form-control" value={rightId} onChange={e => setRightId(e.target.value)}>
            <option value="">Select Right Run</option>
            {runs.map(r => <option key={r.id} value={r.id}>{r.id} ({new Date(r.started_at).toLocaleString()})</option>)}
          </select>
          <button className="btn btn-primary" onClick={handleDiff}>Compare</button>
        </div>
      </div>

      {diff.length > 0 && (
        <div className="card table-container">
          <table>
            <thead>
              <tr>
                <th>Coordinate</th>
                <th>Status</th>
                <th>Left Output Address</th>
                <th>Right Output Address</th>
                <th>Left Status</th>
                <th>Right Status</th>
              </tr>
            </thead>
            <tbody>
              {diff.map((d, i) => (
                <tr key={i}>
                  <td>{d.coordinate}</td>
                  <td>
                    <span className={`badge badge-${d.status === 'unchanged' ? 'info' : d.status === 'changed' ? 'warning' : d.status === 'added' ? 'success' : 'error'}`}>
                      {d.status}
                    </span>
                  </td>
                  <td>
                    {d.left_output_address ? (
                      <Link to={`/objects/${d.left_output_address}`} style={{fontFamily:'monospace'}}>{d.left_output_address.slice(0, 16)}...</Link>
                    ) : '-'}
                  </td>
                  <td>
                    {d.right_output_address ? (
                      <Link to={`/objects/${d.right_output_address}`} style={{fontFamily:'monospace'}}>{d.right_output_address.slice(0, 16)}...</Link>
                    ) : '-'}
                  </td>
                  <td>{d.left_status || '-'}</td>
                  <td>{d.right_status || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
