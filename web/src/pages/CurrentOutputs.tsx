import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchCurrentOutputs } from '../api';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';

export default function CurrentOutputs() {
  const [outputs, setOutputs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchCurrentOutputs()
      .then(setOutputs)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div>Loading current outputs...</div>;
  if (error) return <div className="page-container">API unreachable: {error}</div>;

  const columns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'source_id',
      header: 'Source',
      meta: { filterVariant: 'select' },
    },
    {
      accessorKey: 'status',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => {
        const val = info.getValue();
        if (val === 'filtered') {
          return <span className="badge" style={{ backgroundColor: 'var(--bg-secondary)', color: 'var(--text-muted)' }}>filtered</span>;
        }
        return <span className="badge badge-success">active</span>;
      }
    },
    {
      accessorKey: 'coordinate',
      header: 'Coordinate',
    },
    {
      accessorKey: 'step_name',
      header: 'Step',
      meta: { filterVariant: 'select' },
    },
    {
      accessorKey: 'code_version',
      header: 'Code Version',
      meta: { filterVariant: 'select' },
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
      accessorKey: 'updated_at',
      header: 'Updated At',
      cell: (info) => {
        const val = info.getValue();
        return val ? new Date(val).toLocaleString() : '-';
      },
    }
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Current Outputs</h1>
      </div>
      <DataTable data={outputs} columns={columns} />
    </div>
  );
}
