import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchCurrentOutputs } from '../api';
import { DataTable, TruncatedText, HashCell } from '../components/DataTable';
import { fmtTime } from '../format';
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
  if (error) return <div>API unreachable: {error}</div>;

  const columns: ColumnDef<any, any>[] = [
    { accessorKey: 'coordinate', header: 'Coordinate', cell: (info) => <TruncatedText value={info.getValue()} /> },
    {
      accessorKey: 'status',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => info.getValue() === 'filtered'
        ? <span className="badge badge-warning">filtered</span>
        : <span className="badge badge-success">active</span>,
    },
    { accessorKey: 'source_id', header: 'Source', meta: { filterVariant: 'select' }, cell: (info) => <TruncatedText value={info.getValue()} maxWidth={200} /> },
    { accessorKey: 'pipeline_id', header: 'Pipeline', cell: (info) => <TruncatedText value={info.getValue()} /> },
    { accessorKey: 'step_name', header: 'Step', meta: { filterVariant: 'select' } },
    { accessorKey: 'code_version', header: 'Code Version', meta: { filterVariant: 'select' } },
    {
      accessorKey: 'output_address',
      header: 'Output Address',
      cell: (info) => info.getValue() ? <HashCell value={info.getValue()} to={`/objects/${info.getValue()}`} /> : '—',
    },
    { accessorKey: 'input_hash', header: 'Input Hash', cell: (info) => <HashCell value={info.getValue()} /> },
    {
      accessorKey: 'run_id',
      header: 'Run',
      cell: (info) => info.getValue()
        ? <Link to={`/runs/${info.getValue()}`} style={{ fontFamily: 'var(--font-mono)', fontSize: '0.8rem' }}>{info.getValue()}</Link>
        : '—',
    },
    { accessorKey: 'updated_at', header: 'Updated', cell: (info) => fmtTime(info.getValue()) },
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Current Outputs</h1>
      </div>
      <DataTable
        data={outputs}
        columns={columns}
        initialColumnVisibility={{ input_hash: false, run_id: false }}
      />
    </div>
  );
}
