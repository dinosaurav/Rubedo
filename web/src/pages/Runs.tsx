import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchRuns } from '../api';
import { DataTable, TruncatedText } from '../components/DataTable';
import { fmtTime, durationMs, fmtDuration, runStatusClass } from '../format';
import type { ColumnDef } from '@tanstack/react-table';

export default function Runs() {
  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchRuns()
      .then(setRuns)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div>Loading runs...</div>;
  if (error) return <div>API unreachable: {error}</div>;

  const columns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'id',
      header: 'Run',
      cell: (info) => (
        <Link to={`/runs/${info.getValue()}`} style={{ fontFamily: 'var(--font-mono)', fontSize: '0.8rem' }}>
          {info.getValue()}
        </Link>
      ),
    },
    { accessorKey: 'pipeline_id', header: 'Pipeline', cell: (info) => <TruncatedText value={info.getValue()} /> },
    { accessorKey: 'source_id', header: 'Source', cell: (info) => <TruncatedText value={info.getValue()} maxWidth={200} /> },
    { accessorKey: 'kind', header: 'Kind', meta: { filterVariant: 'select' } },
    {
      accessorKey: 'status',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => <span className={`badge badge-${runStatusClass(info.getValue())}`}>{info.getValue()}</span>,
    },
    { accessorKey: 'started_at', header: 'Started', cell: (info) => fmtTime(info.getValue()) },
    {
      id: 'duration',
      header: 'Duration',
      enableColumnFilter: false,
      accessorFn: (row) => durationMs(row.started_at, row.finished_at),
      cell: (info) => fmtDuration(info.getValue()),
    },
    { accessorKey: 'created_count', header: 'Created' },
    { accessorKey: 'reused_count', header: 'Reused' },
    { accessorKey: 'failed_count', header: 'Failed' },
    { accessorKey: 'blocked_count', header: 'Blocked' },
    { accessorKey: 'filtered_count', header: 'Filtered' },
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Runs</h1>
      </div>
      <DataTable
        data={runs}
        columns={columns}
        initialColumnVisibility={{ source_id: false, kind: false, blocked_count: false, filtered_count: false }}
      />
    </div>
  );
}
