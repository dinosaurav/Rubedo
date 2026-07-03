import { useState, useEffect, useRef } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  flexRender,
} from '@tanstack/react-table';
import type { ColumnDef, Column, Header } from '@tanstack/react-table';
import { ListFilter, ArrowDownAZ, ArrowUpAZ, X } from 'lucide-react';

export function FilterInput({ column, table }: { column: Column<any, unknown>; table: any }) {
  const columnFilterValue = column.getFilterValue();

  // If the meta says this is a 'select' filter
  if ((column.columnDef.meta as any)?.filterVariant === 'select') {
    const options = Array.from(new Set(table.getPreFilteredRowModel().flatRows.map((r: any) => r.getValue(column.id))));
    return (
      <select
        value={(columnFilterValue ?? '') as string}
        onChange={e => column.setFilterValue(e.target.value)}
        className="table-filter-select"
        onClick={e => e.stopPropagation()}
        style={{ marginTop: 0 }}
      >
        <option value="">All {typeof column.columnDef.header === 'string' ? column.columnDef.header : ''}</option>
        {options.map((opt: any) => (
          <option key={opt} value={opt}>{opt}</option>
        ))}
      </select>
    );
  }

  // Text search
  return (
    <input
      type="text"
      value={(columnFilterValue ?? '') as string}
      onChange={e => column.setFilterValue(e.target.value)}
      placeholder={`Filter...`}
      className="table-filter-input"
      onClick={e => e.stopPropagation()}
      style={{ marginTop: 0 }}
    />
  );
}

function ColumnHeader({ header, table, openMenuId, setOpenMenuId }: { header: Header<any, unknown>; table: any; openMenuId: string | null; setOpenMenuId: (id: string | null) => void }) {
  const popoverRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const isOpen = openMenuId === header.id;

  const column = header.column;
  const isSortedAsc = column.getIsSorted() === 'asc';
  const isSortedDesc = column.getIsSorted() === 'desc';
  const isFiltered = column.getIsFiltered();

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        isOpen && 
        popoverRef.current && 
        !popoverRef.current.contains(e.target as Node) &&
        buttonRef.current &&
        !buttonRef.current.contains(e.target as Node)
      ) {
        setOpenMenuId(null);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [isOpen, setOpenMenuId]);

  return (
    <th className="table-header-cell" style={{ verticalAlign: 'middle', minWidth: '120px' }}>
      {header.isPlaceholder ? null : (
        <div className="table-header-content" style={{ cursor: 'default' }}>
          <span style={{ fontWeight: 600 }}>
            {flexRender(column.columnDef.header, header.getContext())}
          </span>
          
          {(column.getCanSort() || column.getCanFilter()) && (
            <button 
              ref={buttonRef}
              className={`table-menu-button ${isOpen || isFiltered || isSortedAsc || isSortedDesc ? 'active' : ''}`}
              onClick={(e) => {
                e.stopPropagation();
                setOpenMenuId(isOpen ? null : header.id);
              }}
            >
              <ListFilter size={14} />
            </button>
          )}

          {isOpen && (
            <div className="table-popover" ref={popoverRef} onClick={(e) => e.stopPropagation()}>
              {column.getCanSort() && (
                <>
                  <div 
                    className={`table-popover-item ${isSortedAsc ? 'active' : ''}`}
                    onClick={() => {
                      column.toggleSorting(false);
                      setOpenMenuId(null);
                    }}
                  >
                    <ArrowDownAZ size={14} /> Sort A-Z
                  </div>
                  <div 
                    className={`table-popover-item ${isSortedDesc ? 'active' : ''}`}
                    onClick={() => {
                      column.toggleSorting(true);
                      setOpenMenuId(null);
                    }}
                  >
                    <ArrowUpAZ size={14} /> Sort Z-A
                  </div>
                  {(isSortedAsc || isSortedDesc) && (
                    <div 
                      className="table-popover-item"
                      onClick={() => {
                        column.clearSorting();
                        setOpenMenuId(null);
                      }}
                    >
                      <X size={14} /> Clear Sort
                    </div>
                  )}
                  {column.getCanFilter() && <div className="table-popover-divider" />}
                </>
              )}

              {column.getCanFilter() && (
                <div style={{ padding: '0.2rem' }}>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginBottom: '0.3rem', fontWeight: 600, textTransform: 'uppercase' }}>
                    Filter By
                  </div>
                  <FilterInput column={column} table={table} />
                  {isFiltered && (
                    <button 
                      className="btn btn-outline" 
                      style={{ width: '100%', marginTop: '0.5rem', padding: '0.2rem', fontSize: '0.75rem' }}
                      onClick={() => column.setFilterValue(undefined)}
                    >
                      Clear Filter
                    </button>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </th>
  );
}

const versionSort = (rowA: any, rowB: any, columnId: string) => {
  const a = rowA.getValue(columnId) as string;
  const b = rowB.getValue(columnId) as string;
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;

  const aParts = a.split('.');
  const bParts = b.split('.');

  for (let i = 0; i < Math.max(aParts.length, bParts.length); i++) {
    const aPart = aParts[i] || '0';
    const bPart = bParts[i] || '0';

    const aNum = parseInt(aPart, 10);
    const bNum = parseInt(bPart, 10);

    if (!isNaN(aNum) && !isNaN(bNum)) {
      if (aNum !== bNum) return aNum - bNum;
    } else {
      if (aPart !== bPart) return aPart.localeCompare(bPart);
    }
  }
  return 0;
};

export function DataTable({ data, columns }: { data: any[]; columns: ColumnDef<any, any>[] }) {
  const [sorting, setSorting] = useState<any[]>([]);
  const [columnFilters, setColumnFilters] = useState<any[]>([]);
  const [openMenuId, setOpenMenuId] = useState<string | null>(null);

  const processedColumns = columns.map(col => {
    if ((col as any).accessorKey === 'code_version') {
      return { ...col, sortingFn: versionSort };
    }
    return col;
  });

  const table = useReactTable({
    data,
    columns: processedColumns,
    state: {
      sorting,
      columnFilters,
    },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  return (
    <div className="card table-container" style={{ padding: 0, overflow: 'visible' }}>
      <table style={{ overflow: 'visible' }}>
        <thead>
          {table.getHeaderGroups().map(headerGroup => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map(header => (
                <ColumnHeader 
                  key={header.id} 
                  header={header} 
                  table={table} 
                  openMenuId={openMenuId} 
                  setOpenMenuId={setOpenMenuId} 
                />
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map(row => (
            <tr key={row.id}>
              {row.getVisibleCells().map(cell => (
                <td key={cell.id}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {table.getRowModel().rows.length === 0 && (
        <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-muted)' }}>
          No data available.
        </div>
      )}
    </div>
  );
}
