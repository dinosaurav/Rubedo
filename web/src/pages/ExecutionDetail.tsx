import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { fetchExecution, fetchExecutionStdout, fetchExecutionStderr } from '../api';
import { CheckCircle, Clock, XCircle, Loader2 } from 'lucide-react';

export default function ExecutionDetail() {
  const { executionId } = useParams();
  const [exec, setExec] = useState<any>(null);
  const [stdout, setStdout] = useState('');
  const [stderr, setStderr] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let interval: any;
    
    const load = async () => {
      try {
        const data = await fetchExecution(executionId!);
        setExec(data);
        
        const out = await fetchExecutionStdout(executionId!);
        setStdout(out);
        
        const err = await fetchExecutionStderr(executionId!);
        setStderr(err);
        
        setLoading(false);
        
        if (data.status === 'succeeded' || data.status === 'failed') {
          clearInterval(interval);
        }
      } catch (e) {
        console.error(e);
      }
    };
    
    load();
    interval = setInterval(load, 1000);
    return () => clearInterval(interval);
  }, [executionId]);

  if (loading || !exec) return <div className="page-container">Loading...</div>;

  return (
    <div className="page-container">
      <div className="page-header">
        <h1>Execution: {exec.id}</h1>
        <span className={`status-badge ${exec.status}`} style={{ fontSize: '1rem', padding: '0.25rem 0.75rem' }}>
          {exec.status === 'succeeded' && <CheckCircle size={18} />}
          {exec.status === 'failed' && <XCircle size={18} />}
          {exec.status === 'queued' && <Clock size={18} />}
          {exec.status === 'running' && <Loader2 size={18} className="spin" />}
          {exec.status}
        </span>
      </div>

      <div className="metrics-grid">
        <div className="stat-card">
          <div className="stat-label">Processor ID</div>
          <div className="stat-value" style={{ fontSize: '1.2rem' }}><code>{exec.processor_id}</code></div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Run Link</div>
          <div className="stat-value" style={{ fontSize: '1.2rem' }}>
            {exec.run_id ? <Link to={`/runs/${exec.run_id}`}>{exec.run_id.substring(0,8)}...</Link> : '-'}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Requested At</div>
          <div className="stat-value" style={{ fontSize: '1.2rem' }}>
            {new Date(exec.requested_at).toLocaleString()}
          </div>
        </div>
      </div>

      <div className="card" style={{ marginTop: '2rem' }}>
        <h2>Execution Payload</h2>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem', marginTop: '1rem' }}>
          <div>
            <h3>Input JSON</h3>
            <pre style={{ background: '#f5f5f5', padding: '1rem', borderRadius: '4px' }}>
              {JSON.stringify(JSON.parse(exec.input_json), null, 2)}
            </pre>
          </div>
          <div>
            <h3>Overrides</h3>
            <ul style={{ listStyle: 'none', padding: 0 }}>
              <li><strong>Force:</strong> {exec.force ? 'Yes' : 'No'}</li>
              <li><strong>Folder Override:</strong> {exec.folder_override || 'None'}</li>
              <li><strong>Workers Override:</strong> {exec.workers_override || 'None'}</li>
            </ul>
          </div>
        </div>
      </div>

      {exec.error_message && (
        <div className="card" style={{ marginTop: '2rem', borderLeft: '4px solid var(--danger)' }}>
          <h2 style={{ color: 'var(--danger)' }}>Error</h2>
          <pre style={{ color: 'var(--danger)', whiteSpace: 'pre-wrap', fontFamily: 'monospace' }}>
            {exec.error_message}
          </pre>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem', marginTop: '2rem' }}>
        <div className="card">
          <h2>Standard Output</h2>
          <pre style={{ background: '#1e1e1e', color: '#fff', padding: '1rem', borderRadius: '4px', height: '400px', overflowY: 'auto' }}>
            {stdout || 'No output'}
          </pre>
        </div>
        <div className="card">
          <h2>Standard Error</h2>
          <pre style={{ background: '#1e1e1e', color: '#ff6b6b', padding: '1rem', borderRadius: '4px', height: '400px', overflowY: 'auto' }}>
            {stderr || 'No errors'}
          </pre>
        </div>
      </div>

    </div>
  );
}
