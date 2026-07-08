import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { fetchObject } from '../api';
import JSONViewer from '../components/JSONViewer';

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
          {obj.index?.length > 0 && (
            <div>
              <strong>Indexed Fields:</strong>{' '}
              {obj.index.map((e: any, i: number) => (
                <code key={i} style={{ marginRight: '0.5rem', background: 'var(--bg-tertiary)', padding: '2px 6px', borderRadius: '4px' }}>
                  {e.field}:{e.value}
                </code>
              ))}
            </div>
          )}
        </div>
        <div style={{ marginTop: '1rem' }}>
          <a className="btn btn-outline" href={`http://localhost:8000/api/objects/${obj.output_address}/download`} download>
            Download
          </a>
        </div>
      </div>

      <div className="card">
        <h2 style={{ fontSize: '1.25rem', marginBottom: '1rem' }}>Preview</h2>
        {obj.preview_json ? (
          <div className="pre-block" style={{ padding: '1rem', fontFamily: 'ui-monospace, monospace', fontSize: '0.9rem' }}>
            <JSONViewer data={obj.preview_json} />
          </div>
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
