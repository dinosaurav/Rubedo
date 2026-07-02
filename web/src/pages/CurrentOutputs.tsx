import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
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
      accessorKey: 'source_folder',
      header: 'Source Folder',
      meta: { filterVariant: 'select' },
    },
    {
      accessorKey: 'coordinate',
      header: 'Coordinate',
    },
    {
      accessorKey: 'step',
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
      cell: (info) => new Date(info.getValue()).toLocaleString(),
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
