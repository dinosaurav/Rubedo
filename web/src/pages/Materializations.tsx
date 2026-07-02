import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchMaterializations } from '../api';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';

const PAGE_SIZE = 100;

export default function Materializations() {
  const [mats, setMats] = useState<any[]>([]);
  const [total, setTotal] = useState(0);

  const loadPage = (offset: number) => {
    fetchMaterializations(PAGE_SIZE, offset).then(({ items, total }) => {
      setMats(prev => offset === 0 ? items : [...prev, ...items]);
      setTotal(total);
    });
  };

  useEffect(() => {
    loadPage(0);
  }, []);

  const columns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'id',
      header: 'ID',
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
      accessorKey: 'created_at',
      header: 'Created',
      cell: (info) => new Date(info.getValue()).toLocaleString(),
    },
    {
      id: 'status',
      accessorFn: (row) => row.is_live ? 'Valid' : 'Invalidated',
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
    },
    {
      accessorKey: 'metadata_json',
      header: 'Metadata',
      cell: (info) => (
        <span style={{ fontSize: '0.75rem', fontFamily: 'monospace' }}>
          {info.getValue()}
        </span>
      ),
    }
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Materializations</h1>
      </div>
      <DataTable data={mats} columns={columns} />
      {mats.length < total && (
        <div style={{ marginTop: '1rem' }}>
          <button className="btn btn-outline" onClick={() => loadPage(mats.length)}>
            Load More ({mats.length} of {total})
          </button>
        </div>
      )}
    </div>
  );
}
