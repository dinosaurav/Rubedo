import { useEffect, useState } from 'react';
import { fetchRuns, diffRuns } from '../api';
import { Link } from 'react-router-dom';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';

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

  const columns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'coordinate',
      header: 'Coordinate',
    },
    {
      accessorKey: 'status',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => {
        const val = info.getValue();
        return (
          <span className={`badge badge-${val === 'unchanged' ? 'info' : val === 'changed' ? 'warning' : val === 'added' ? 'success' : 'error'}`}>
            {val}
          </span>
        );
      },
    },
    {
      accessorKey: 'left_output_address',
      header: 'Left Output Address',
      cell: (info) => {
        const val = info.getValue();
        return val ? (
          <Link to={`/objects/${val}`} style={{fontFamily:'monospace'}}>{val.slice(0, 16)}...</Link>
        ) : '-';
      },
    },
    {
      accessorKey: 'right_output_address',
      header: 'Right Output Address',
      cell: (info) => {
        const val = info.getValue();
        return val ? (
          <Link to={`/objects/${val}`} style={{fontFamily:'monospace'}}>{val.slice(0, 16)}...</Link>
        ) : '-';
      },
    },
    {
      accessorKey: 'left_status',
      header: 'Left Status',
      meta: { filterVariant: 'select' },
    },
    {
      accessorKey: 'right_status',
      header: 'Right Status',
      meta: { filterVariant: 'select' },
    }
  ];

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

      {diff.length > 0 && <DataTable data={diff} columns={columns} />}
    </div>
  );
}
