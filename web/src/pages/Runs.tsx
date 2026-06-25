import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchRuns } from '../api';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';

export default function Runs() {
  const [runs, setRuns] = useState<any[]>([]);

  useEffect(() => {
    fetchRuns().then(setRuns);
  }, []);

  const columns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'id',
      header: 'Run ID',
    },
    {
      accessorKey: 'kind',
      header: 'Kind',
    },
    {
      accessorKey: 'status',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => {
        const val = info.getValue();
        return (
          <span className={`badge badge-${val === 'succeeded' ? 'success' : val === 'failed' ? 'error' : 'warning'}`}>
            {val}
          </span>
        );
      },
    },
    {
      accessorKey: 'started_at',
      header: 'Started',
      cell: (info) => new Date(info.getValue()).toLocaleString(),
    },
    {
      id: 'actions',
      header: 'Actions',
      enableColumnFilter: false,
      enableSorting: false,
      cell: (info) => (
        <Link to={`/runs/${info.row.original.id}`} className="btn btn-outline">View</Link>
      ),
    }
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Runs</h1>
      </div>
      <DataTable data={runs} columns={columns} />
    </div>
  );
}
