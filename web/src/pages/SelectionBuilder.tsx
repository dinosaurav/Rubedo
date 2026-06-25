import React, { useState } from 'react';
import { previewSelection, invalidateSelection } from '../api';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';
import { Link } from 'react-router-dom';

export default function SelectionBuilder() {
  const [selection, setSelection] = useState({
    source_folder: '',
    coordinate_glob: '',
    metadata: [] as any[]
  });
  
  const [metaKey, setMetaKey] = useState('');
  const [metaOp, setMetaOp] = useState('equals');
  const [metaValue, setMetaValue] = useState('');

  const [preview, setPreview] = useState<any[]>([]);

  const addMetaFilter = () => {
    if (!metaKey) return;
    let val: any = metaValue;
    if (val === 'true') val = true;
    if (val === 'false') val = false;
    if (!isNaN(Number(val)) && val.trim() !== '') val = Number(val);
    
    setSelection({
      ...selection,
      metadata: [...selection.metadata, { key: metaKey, op: metaOp, value: val }]
    });
    setMetaKey('');
    setMetaValue('');
  };

  const removeMetaFilter = (idx: number) => {
    const newMeta = [...selection.metadata];
    newMeta.splice(idx, 1);
    setSelection({ ...selection, metadata: newMeta });
  };

  const handlePreview = async () => {
    const cleanSel: any = {};
    if (selection.source_folder) cleanSel.source_folder = selection.source_folder;
    if (selection.coordinate_glob) cleanSel.coordinate_glob = selection.coordinate_glob;
    if (selection.metadata.length > 0) cleanSel.metadata = selection.metadata;
    const res = await previewSelection(cleanSel);
    setPreview(res.items || []);
  };

  const handleInvalidate = async () => {
    if (!confirm("Are you sure you want to invalidate these materializations?")) return;
    const cleanSel: any = {};
    if (selection.source_folder) cleanSel.source_folder = selection.source_folder;
    if (selection.coordinate_glob) cleanSel.coordinate_glob = selection.coordinate_glob;
    if (selection.metadata.length > 0) cleanSel.metadata = selection.metadata;
    
    const res = await invalidateSelection(cleanSel, "Invalidated from UI");
    alert(`Invalidated ${res.invalidated_count} materializations. Run process again to recompute.`);
    setPreview([]);
  };

  const previewColumns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'id',
      header: 'ID',
    },
    {
      accessorKey: 'output_address',
      header: 'Output Address',
      cell: (info) => {
        const val = info.getValue();
        return val ? <Link to={`/objects/${val}`} style={{ fontFamily: 'monospace' }}>{val.slice(0, 16)}...</Link> : '-';
      },
    },
    {
      id: 'status',
      accessorFn: (row) => row.invalidated_at ? 'Invalidated' : 'Valid',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => {
        const val = info.getValue();
        return val === 'Invalidated' ? (
          <span className="badge badge-error">Invalidated</span>
        ) : (
          <span className="badge badge-success">Valid</span>
        );
      },
    }
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Selection Builder</h1>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem' }}>
        <div className="card">
          <h2 style={{ marginBottom: '1rem', fontSize: '1.25rem' }}>Filters</h2>
          
          <div className="form-group">
            <label className="form-label">Source Folder</label>
            <input 
              className="form-control" 
              value={selection.source_folder} 
              onChange={e => setSelection({...selection, source_folder: e.target.value})}
              placeholder="e.g. examples/input"
            />
          </div>

          <div className="form-group">
            <label className="form-label">Coordinate Glob</label>
            <input 
              className="form-control" 
              value={selection.coordinate_glob} 
              onChange={e => setSelection({...selection, coordinate_glob: e.target.value})}
              placeholder="e.g. **/*.txt"
            />
          </div>

          <div className="form-group">
            <label className="form-label">Metadata Filters</label>
            {selection.metadata.map((m, i) => (
              <div key={i} style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.5rem', alignItems: 'center' }}>
                <span className="badge badge-info">{m.key} {m.op} {String(m.value)}</span>
                <button onClick={() => removeMetaFilter(i)} className="btn btn-outline" style={{ padding: '0.2rem 0.5rem' }}>x</button>
              </div>
            ))}
            
            <div style={{ display: 'flex', gap: '0.5rem', marginTop: '1rem' }}>
              <input className="form-control" placeholder="Key" value={metaKey} onChange={e => setMetaKey(e.target.value)} />
              <select className="form-control" value={metaOp} onChange={e => setMetaOp(e.target.value)}>
                <option value="equals">equals</option>
                <option value="not equals">not equals</option>
                <option value="exists">exists</option>
              </select>
              <input className="form-control" placeholder="Value" value={metaValue} onChange={e => setMetaValue(e.target.value)} />
              <button className="btn btn-primary" onClick={addMetaFilter}>Add</button>
            </div>
          </div>

          <div style={{ display: 'flex', gap: '1rem', marginTop: '2rem' }}>
            <button className="btn btn-primary" onClick={handlePreview}>Preview Selection</button>
            <button className="btn btn-danger" onClick={handleInvalidate} disabled={preview.length === 0}>
              Invalidate Selected
            </button>
          </div>
        </div>

        <div>
          <h2 style={{ marginBottom: '1rem', fontSize: '1.25rem' }}>Preview Results ({preview.length})</h2>
          {preview.length > 0 ? (
            <DataTable data={preview} columns={previewColumns} />
          ) : (
            <div className="card" style={{ color: 'var(--text-secondary)' }}>
              No materializations match the selection, or preview not generated yet.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
