import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { fetchObject, invalidateSelection } from '../api';

export default function OutputDetail() {
  const { address } = useParams();
  const [obj, setObj] = useState<any>(null);

  useEffect(() => {
    if (address) {
      fetchObject(address).then(setObj).catch(err => {
        console.error(err);
        setObj({ error: "Not found or error fetching" });
      });
    }
  }, [address]);

  if (!obj) return <div>Loading...</div>;
  if (obj.error) return <div>{obj.error}</div>;

  const handleInvalidate = async () => {
    if (!confirm("Invalidate this output?")) return;
    await invalidateSelection({
      output_content_hash: obj.output_content_hash
    }, "Invalidated from UI");
    alert("Invalidated. Please reload.");
  };

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Output Detail</h1>
      </div>
      
      <div className="card" style={{ marginBottom: '2rem' }}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
          <div><strong>Address:</strong> <span style={{fontFamily: 'monospace'}}>{obj.output_address}</span></div>
          <div><strong>Content Hash:</strong> <span style={{fontFamily: 'monospace'}}>{obj.output_content_hash}</span></div>
          <div><strong>Step:</strong> {obj.step}</div>
          <div><strong>Code Version:</strong> {obj.code_version}</div>
          <div><strong>Created By Run:</strong> <Link to={`/runs/${obj.created_by_run_id}`}>{obj.created_by_run_id}</Link></div>
          <div><strong>Created At:</strong> {new Date(obj.created_at).toLocaleString()}</div>
          <div>
            <strong>Status:</strong> {obj.invalidated_at ? <span className="badge badge-error">Invalidated</span> : <span className="badge badge-success">Valid</span>}
          </div>
        </div>
        <div style={{ marginTop: '1rem' }}>
          <button className="btn btn-danger" onClick={handleInvalidate} disabled={!!obj.invalidated_at}>Invalidate</button>
          <a className="btn btn-outline" style={{ marginLeft: '1rem' }} href={`http://localhost:8000/api/objects/${obj.output_address}/download`} download>
            Download
          </a>
        </div>
      </div>

      <div className="card">
        <h2 style={{ fontSize: '1.25rem', marginBottom: '1rem' }}>Preview</h2>
        {obj.preview_json ? (
          <pre className="pre-block">{JSON.stringify(obj.preview_json, null, 2)}</pre>
        ) : obj.preview_text ? (
          <pre className="pre-block">{obj.preview_text}</pre>
        ) : obj.preview_kind === 'binary' ? (
          <div style={{ color: 'var(--text-secondary)' }}>Binary file, preview not available. Please download.</div>
        ) : (
          <div style={{ color: 'var(--text-secondary)' }}>No preview available.</div>
        )}
      </div>
    </div>
  );
}
