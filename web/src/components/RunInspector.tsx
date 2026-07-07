import React, { useState } from 'react';
import { searchRun, fetchObject } from '../api';
import { coordStatusClass } from '../format';

interface TraceItem {
  step_name: string;
  coordinate: string;
  status: string;
  output_address: string;
  materialization_id: number;
  is_match: boolean;
}

export default function RunInspector({ runId }: { runId: string }) {
  const [query, setQuery] = useState('');
  const [trace, setTrace] = useState<TraceItem[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const [expandedOutput, setExpandedOutput] = useState<string | null>(null);
  const [outputData, setOutputData] = useState<any>(null);

  const fetchOutput = async (address: string) => {
    if (expandedOutput === address) {
      setExpandedOutput(null); // toggle off
      return;
    }
    setExpandedOutput(address);
    setOutputData(null);
    try {
      const data = await fetchObject(address);
      setOutputData(data);
    } catch (e) {
      setOutputData({ error: 'Failed to fetch object data.' });
    }
  };

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) {
      setTrace(null);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await searchRun(runId, query);
      setTrace(res.trace);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card" style={{ marginBottom: '1rem' }}>
      <div className="stat-label" style={{ marginBottom: '1rem' }}>Run Search & Lineage</div>
      
      <form onSubmit={handleSearch} style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.5rem' }}>
        <input 
          className="form-control" 
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Search for an indexed value or coordinate..."
        />
        <button type="submit" className="btn btn-primary" disabled={loading}>
          {loading ? 'Searching...' : 'Search'}
        </button>
      </form>

      {error && <div style={{ color: 'var(--status-error)', marginBottom: '1rem' }}>{error}</div>}

      {trace && trace.length === 0 && (
        <div style={{ color: 'var(--text-muted)' }}>No lineage trace found for "{query}".</div>
      )}

      {trace && trace.length > 0 && (
        <div>
          <h4 style={{ marginBottom: '0.75rem', fontSize: '0.9rem' }}>Lineage Trace</h4>
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Step</th>
                  <th>Status</th>
                  <th>Coordinate</th>
                  <th>Output</th>
                  <th>Match?</th>
                </tr>
              </thead>
              <tbody>
                {trace.map((item, i) => (
                  <React.Fragment key={i}>
                    <tr style={item.is_match ? { backgroundColor: 'rgba(255,0,51,0.05)' } : {}}>
                      <td style={{ fontWeight: 600 }}>{item.step_name}</td>
                      <td>
                        <span className={`badge badge-${coordStatusClass(item.status)}`}>{item.status}</span>
                      </td>
                      <td><code style={{ fontSize: '0.8rem' }}>{item.coordinate}</code></td>
                      <td>
                        {item.output_address ? (
                          <button 
                            className="btn btn-outline" 
                            style={{ fontSize: '0.7rem', padding: '0.2rem 0.5rem' }}
                            onClick={() => fetchOutput(item.output_address)}
                          >
                            {item.output_address.slice(0, 8)}
                          </button>
                        ) : '—'}
                      </td>
                      <td>
                        {item.is_match && <span className="badge badge-success">Direct Match</span>}
                      </td>
                    </tr>
                    {expandedOutput === item.output_address && (
                      <tr>
                        <td colSpan={5} style={{ padding: '1rem', backgroundColor: 'var(--bg-tertiary)' }}>
                          {outputData ? (
                            outputData.error ? (
                              <div style={{ color: 'var(--status-error)' }}>{outputData.error}</div>
                            ) : (
                              <div>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '1rem' }}>
                                  <strong>Output Preview</strong>
                                  <a className="btn btn-primary" style={{ padding: '0.2rem 0.6rem', fontSize: '0.75rem' }} href={`http://localhost:8000/api/objects/${outputData.output_address}/download`} download>
                                    Download Full File
                                  </a>
                                </div>
                                {outputData.preview_json ? (
                                  <pre className="pre-block" style={{ maxHeight: '300px' }}>{JSON.stringify(outputData.preview_json, null, 2)}</pre>
                                ) : outputData.preview_text ? (
                                  <pre className="pre-block" style={{ maxHeight: '300px' }}>{outputData.preview_text}</pre>
                                ) : outputData.preview_kind === 'binary' ? (
                                  <div style={{ color: 'var(--text-secondary)', padding: '1rem', border: '1px dashed var(--border-color)', textAlign: 'center' }}>
                                    Binary file, preview not available. Please download.
                                  </div>
                                ) : (
                                  <div style={{ color: 'var(--text-secondary)' }}>No preview available.</div>
                                )}
                              </div>
                            )
                          ) : (
                            <div>Loading preview...</div>
                          )}
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
