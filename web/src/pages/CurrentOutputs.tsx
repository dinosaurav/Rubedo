import { useEffect, useState } from 'react';
import { fetchCurrentOutputs } from '../api';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';

export default function CurrentOutputs() {
  const [outputs, setOutputs] = useState<any[]>([]);

  useEffect(() => {
    fetchCurrentOutputs().then(setOutputs);
  }, []);

  const columns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'source_id',
      header: 'Source',
      meta: { filterVariant: 'select' },
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
        return val ? <span style={{ fontFamily: 'monospace' }}>{val.slice(0, 16)}...</span> : '-';
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
