import { useEffect, useState } from 'react';
import { fetchMaterializations } from '../api';
import { DataTable, TruncatedText, HashCell } from '../components/DataTable';
import { fmtTime } from '../format';
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

  useEffect(() => { loadPage(0); }, []);

  const columns: ColumnDef<any, any>[] = [
    { accessorKey: 'id', header: 'ID' },
    { accessorKey: 'pipeline_id', header: 'Pipeline', cell: (info) => <TruncatedText value={info.getValue()} /> },
    { accessorKey: 'step_name', header: 'Step', meta: { filterVariant: 'select' } },
    { accessorKey: 'code_version', header: 'Code Version', meta: { filterVariant: 'select' } },
    {
      accessorKey: 'output_address',
      header: 'Output Address',
      cell: (info) => <HashCell value={info.getValue()} to={`/objects/${info.getValue()}`} />,
    },
    { accessorKey: 'output_content_hash', header: 'Content Hash', cell: (info) => <HashCell value={info.getValue()} /> },
    { accessorKey: 'input_hash', header: 'Input Hash', cell: (info) => <HashCell value={info.getValue()} /> },
    { accessorKey: 'content_type', header: 'Type', meta: { filterVariant: 'select' }, cell: (info) => info.getValue() || '—' },
    { accessorKey: 'created_at', header: 'Created', cell: (info) => fmtTime(info.getValue()) },
    {
      id: 'status',
      accessorFn: (row) => row.is_live ? 'Valid' : 'Invalidated',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => <span className={`badge badge-${info.getValue() === 'Invalidated' ? 'error' : 'success'}`}>{info.getValue()}</span>,
    },
    { accessorKey: 'metadata_json', header: 'Metadata', cell: (info) => <TruncatedText value={info.getValue()} mono maxWidth={240} /> },
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Materializations</h1>
      </div>
      <DataTable
        data={mats}
        columns={columns}
        initialColumnVisibility={{ output_content_hash: false, input_hash: false }}
      />
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
