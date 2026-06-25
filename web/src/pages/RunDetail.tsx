import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { fetchRun, fetchRunCoordinates, fetchRunEvents } from '../api';

export default function RunDetail() {
  const { runId } = useParams();
  const [run, setRun] = useState<any>(null);
  const [coords, setCoords] = useState<any[]>([]);
  const [events, setEvents] = useState<any[]>([]);
  const [tab, setTab] = useState<'coords' | 'events'>('coords');

  useEffect(() => {
    if (runId) {
      fetchRun(runId).then(setRun);
      fetchRunCoordinates(runId).then(setCoords);
      fetchRunEvents(runId).then(setEvents);
    }
  }, [runId]);

  if (!run) return <div>Loading...</div>;

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Run {run.id}</h1>
      </div>
      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-label">Status</div>
          <div className="stat-value">
            <span className={`badge badge-${run.status === 'succeeded' ? 'success' : run.status === 'failed' ? 'error' : 'warning'}`}>
              {run.status}
            </span>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Created</div>
          <div className="stat-value" style={{ color: 'var(--status-success)' }}>{run.created_count}</div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Reused</div>
          <div className="stat-value" style={{ color: 'var(--status-info)' }}>{run.reused_count}</div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Failed</div>
          <div className="stat-value" style={{ color: 'var(--status-error)' }}>{run.failed_count}</div>
        </div>
      </div>

      <div style={{ marginBottom: '1rem', display: 'flex', gap: '1rem' }}>
        <button className={`btn ${tab === 'coords' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setTab('coords')}>Coordinates</button>
        <button className={`btn ${tab === 'events' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setTab('events')}>Events</button>
      </div>

      {tab === 'coords' && (
        <div className="card table-container">
          <table>
            <thead>
              <tr>
                <th>Coordinate</th>
                <th>Status</th>
                <th>Output Address</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {coords.map(c => (
                <tr key={c.id}>
                  <td>{c.coordinate}</td>
                  <td>
                    <span className={`badge badge-${c.status === 'created' ? 'success' : c.status === 'failed' ? 'error' : 'info'}`}>
                      {c.status}
                    </span>
                  </td>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>
                    {c.output_address ? <Link to={`/objects/${c.output_address}`}>{c.output_address.slice(0, 16)}...</Link> : '-'}
                  </td>
                  <td>{c.error_message ? <span style={{ color: 'var(--status-error)' }}>{c.error_message}</span> : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'events' && (
        <div className="card table-container">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Level</th>
                <th>Event Type</th>
                <th>Coordinate</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {events.map(e => (
                <tr key={e.id}>
                  <td>{new Date(e.timestamp).toLocaleString()}</td>
                  <td>
                    <span className={`badge badge-${e.level === 'error' ? 'error' : e.level === 'warning' ? 'warning' : 'info'}`}>
                      {e.level}
                    </span>
                  </td>
                  <td>{e.event_type}</td>
                  <td>{e.coordinate || '-'}</td>
                  <td>{e.message}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
