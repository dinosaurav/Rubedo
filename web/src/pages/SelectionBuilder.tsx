import { useState } from 'react';
import { previewSelection, invalidateSelection } from '../api';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';
import { Link } from 'react-router-dom';

export default function SelectionBuilder() {
  const [selection, setSelection] = useState({
    source_id: '',
    coordinate_glob: ''
  });
  
  const [query, setQuery] = useState('');

  const [preview, setPreview] = useState<any[]>([]);

  // The query string wins over the form when present
  const buildPayload = (): any => {
    if (query.trim()) return { query: query.trim() };
    const cleanSel: any = {};
    if (selection.source_id) cleanSel.source_id = selection.source_id;
    if (selection.coordinate_glob) cleanSel.coordinate_glob = selection.coordinate_glob;
    return cleanSel;
  };



  const handlePreview = async () => {
    const res = await previewSelection(buildPayload());
    setPreview(res.items || []);
  };

  const handleInvalidate = async () => {
    if (!confirm("Are you sure you want to invalidate these materializations?")) return;
    const res = await invalidateSelection(buildPayload(), "Invalidated from UI");
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
      accessorFn: (row) => row.invalidated ? 'Invalidated' : 'Valid',
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
            <label className="form-label">Query</label>
            <input
              className="form-control"
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder='e.g. step:extract company:acme live:true coord:*.txt'
              style={{ fontFamily: 'monospace' }}
            />
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
              source: coord: step: version: live: — any other
              field:value matches indexed output fields. Overrides the form below.
            </div>
          </div>

          <div className="form-group">
            <label className="form-label">Source</label>
            <input 
              className="form-control" 
              value={selection.source_id} 
              onChange={e => setSelection({...selection, source_id: e.target.value})}
              placeholder="e.g. folder:examples/input"
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
