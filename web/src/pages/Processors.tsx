import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { fetchProcessors, runProcessor } from '../api';

export default function Processors() {
  const [processors, setProcessors] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedProcessor, setSelectedProcessor] = useState<any>(null);
  const [inputs, setInputs] = useState<any>({});
  const [force, setForce] = useState(false);
  const [workers, setWorkers] = useState<number | ''>('');
  const [folder, setFolder] = useState('');
  const [jsonMode, setJsonMode] = useState(false);
  const [jsonText, setJsonText] = useState('');
  
  const navigate = useNavigate();

  useEffect(() => {
    fetchProcessors().then(data => {
      setProcessors(data);
      setLoading(false);
    });
  }, []);

  const openForm = (proc: any) => {
    setSelectedProcessor(proc);
    setInputs(proc.default_inputs || {});
    setJsonText(JSON.stringify(proc.default_inputs || {}, null, 2));
    setForce(false);
    setWorkers('');
    setFolder('');
    setJsonMode(false);
  };

  const handleRun = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedProcessor) return;
    
    let payloadInputs = inputs;
    if (jsonMode) {
      try {
        payloadInputs = JSON.parse(jsonText);
      } catch (err) {
        alert("Invalid JSON");
        return;
      }
    }
    
    try {
      const res = await runProcessor(selectedProcessor.id, {
        inputs: payloadInputs,
        force,
        folder: folder || null,
        workers: workers || null
      });
      navigate(`/executions/${res.execution_id}`);
    } catch (err: any) {
      alert("Error: " + err.message);
    }
  };

  if (loading) return <div>Loading processors...</div>;

  return (
    <div className="page-container">
      <div className="page-header">
        <h1>Processors</h1>
      </div>
      
      <div className="card">
        <table className="data-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Name</th>
              <th>Folder</th>
              <th>Code Version</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {processors.map(p => (
              <tr key={p.id}>
                <td><code>{p.id}</code></td>
                <td>{p.name}</td>
                <td><code>{p.folder}</code></td>
                <td><code>{p.code_version}</code></td>
                <td>
                  <button onClick={() => openForm(p)} className="btn btn-primary" style={{ padding: '4px 8px', fontSize: '0.875rem' }}>Run</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {selectedProcessor && (
        <div style={{ marginTop: '2rem' }} className="card">
          <h2>Run: {selectedProcessor.name}</h2>
          <form onSubmit={handleRun} style={{ display: 'flex', flexDirection: 'column', gap: '1rem', marginTop: '1rem' }}>
            
            <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
              <label>
                <input type="checkbox" checked={jsonMode} onChange={e => setJsonMode(e.target.checked)} />
                Use Advanced JSON Input
              </label>
            </div>

            {jsonMode ? (
              <div className="stat-card">
                <label>JSON Inputs</label>
                <textarea 
                  value={jsonText} 
                  onChange={e => setJsonText(e.target.value)}
                  style={{ width: '100%', height: '150px', fontFamily: 'monospace', padding: '0.5rem', marginTop: '0.5rem' }}
                />
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                {selectedProcessor.input_schema?.properties && Object.entries(selectedProcessor.input_schema.properties).map(([key, prop]: [string, any]) => (
                  <div key={key} className="stat-card" style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                    <label style={{ fontWeight: 'bold' }}>{key}</label>
                    <span style={{ fontSize: '0.8rem', color: '#666' }}>{prop.description}</span>
                    {prop.type === 'boolean' ? (
                      <input 
                        type="checkbox" 
                        checked={inputs[key] || false}
                        onChange={e => setInputs({...inputs, [key]: e.target.checked})}
                        style={{ alignSelf: 'flex-start', marginTop: '0.5rem' }}
                      />
                    ) : prop.type === 'integer' || prop.type === 'number' ? (
                      <input 
                        type="number"
                        value={inputs[key] ?? ''}
                        onChange={e => setInputs({...inputs, [key]: e.target.value === '' ? '' : Number(e.target.value)})}
                        style={{ padding: '0.5rem', marginTop: '0.5rem' }}
                      />
                    ) : (
                      <input 
                        type="text"
                        value={inputs[key] || ''}
                        onChange={e => setInputs({...inputs, [key]: e.target.value})}
                        style={{ padding: '0.5rem', marginTop: '0.5rem' }}
                      />
                    )}
                  </div>
                ))}
              </div>
            )}

            <div className="stat-card" style={{ display: 'flex', flexDirection: 'column', gap: '1rem', marginTop: '1rem' }}>
              <h3>Operational Settings (does not affect cache)</h3>
              
              <label style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                <input type="checkbox" checked={force} onChange={e => setForce(e.target.checked)} />
                Force Run (ignore cache)
              </label>

              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                <label>Workers Override</label>
                <input 
                  type="number" 
                  value={workers} 
                  onChange={e => setWorkers(e.target.value === '' ? '' : Number(e.target.value))}
                  placeholder={`Default: ${selectedProcessor.workers}`}
                  style={{ padding: '0.5rem' }}
                />
              </div>

              {selectedProcessor.allow_folder_override && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                  <label>Folder Override</label>
                  <input 
                    type="text" 
                    value={folder} 
                    onChange={e => setFolder(e.target.value)}
                    placeholder={`Default: ${selectedProcessor.folder}`}
                    style={{ padding: '0.5rem' }}
                  />
                </div>
              )}
            </div>

            <button type="submit" className="btn btn-primary" style={{ marginTop: '1rem', alignSelf: 'flex-start' }}>
              Submit Execution
            </button>
          </form>
        </div>
      )}
    </div>
  );
}
