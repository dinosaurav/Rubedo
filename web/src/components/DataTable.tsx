import { useState, useEffect, useRef, useLayoutEffect, type MouseEvent as ReactMouseEvent } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  flexRender,
} from '@tanstack/react-table';
import type { ColumnDef, Column, Header } from '@tanstack/react-table';
import { Link } from 'react-router-dom';
import { ListFilter, ArrowDownAZ, ArrowUpAZ, X, EyeOff, Columns3, Copy, Check } from 'lucide-react';

/* ---------------------------------------------------------------------------
   Reusable cells
--------------------------------------------------------------------------- */

// Long free text (messages, metadata, coordinates): ellipsis + click to expand.
export function TruncatedText({ value, mono = false, maxWidth = 260 }: { value: any; mono?: boolean; maxWidth?: number }) {
  const [expanded, setExpanded] = useState(false);
  const text = value === null || value === undefined || value === '' ? '' : String(value);
  if (!text) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  return (
    <span
      onClick={() => setExpanded(e => !e)}
      title={expanded ? 'Click to collapse' : text}
      style={{
        display: 'inline-block',
        maxWidth: expanded ? '640px' : maxWidth,
        whiteSpace: expanded ? 'normal' : 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        verticalAlign: 'bottom',
        cursor: 'pointer',
        fontFamily: mono ? 'var(--font-mono)' : 'inherit',
        fontSize: mono ? '0.8rem' : undefined,
        wordBreak: expanded ? 'break-all' : 'normal',
      }}
    >
      {text}
    </span>
  );
}

// Hashes / addresses: middle-truncated monospace + a copy button (+ optional link).
export function HashCell({ value, head = 10, tail = 6, to }: { value: any; head?: number; tail?: number; to?: string }) {
  const [copied, setCopied] = useState(false);
  const text = value == null ? '' : String(value);
  if (!text) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  const short = text.length > head + tail + 1 ? `${text.slice(0, head)}…${text.slice(-tail)}` : text;
  const copy = (e: ReactMouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    navigator.clipboard?.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    });
  };
  const label = <span title={text}>{short}</span>;
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem', fontFamily: 'var(--font-mono)', fontSize: '0.8rem' }}>
      {to ? <Link to={to}>{label}</Link> : label}
      <button className="table-menu-button" onClick={copy} title="Copy full value" style={{ padding: 2 }}>
        {copied ? <Check size={12} color="var(--status-success)" /> : <Copy size={12} />}
      </button>
    </span>
  );
}

/* ---------------------------------------------------------------------------
   Header filter + per-column menu
--------------------------------------------------------------------------- */

export function FilterInput({ column, table }: { column: Column<any, unknown>; table: any }) {
  const columnFilterValue = column.getFilterValue();

  if ((column.columnDef.meta as any)?.filterVariant === 'select') {
    const options = Array.from(new Set(table.getPreFilteredRowModel().flatRows.map((r: any) => r.getValue(column.id)))).filter(o => o != null);
    return (
      <select
        value={(columnFilterValue ?? '') as string}
        onChange={e => column.setFilterValue(e.target.value)}
        className="table-filter-select"
        onClick={e => e.stopPropagation()}
        style={{ marginTop: 0 }}
      >
        <option value="">All</option>
        {options.map((opt: any) => (
          <option key={String(opt)} value={String(opt)}>{String(opt)}</option>
        ))}
      </select>
    );
  }

  return (
    <input
      type="text"
      value={(columnFilterValue ?? '') as string}
      onChange={e => column.setFilterValue(e.target.value)}
      placeholder="Filter..."
      className="table-filter-input"
      onClick={e => e.stopPropagation()}
      style={{ marginTop: 0 }}
    />
  );
}

function ColumnHeader({ header, table, openMenuId, setOpenMenuId }: { header: Header<any, unknown>; table: any; openMenuId: string | null; setOpenMenuId: (id: string | null) => void }) {
  const popoverRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);
  const isOpen = openMenuId === header.id;

  const column = header.column;
  const isSortedAsc = column.getIsSorted() === 'asc';
  const isSortedDesc = column.getIsSorted() === 'desc';
  const isFiltered = column.getIsFiltered();

  const openMenu = () => {
    const rect = buttonRef.current?.getBoundingClientRect();
    if (rect) {
      const width = 220;
      setPos({ top: rect.bottom + 4, left: Math.min(rect.left, window.innerWidth - width - 12) });
    }
    setOpenMenuId(header.id);
  };

  // Fixed-position popover escapes the horizontal-scroll clip; close it on any
  // scroll or resize rather than trying to reposition.
  useEffect(() => {
    if (!isOpen) return;
    const close = () => setOpenMenuId(null);
    const onClickOutside = (e: MouseEvent) => {
      if (
        popoverRef.current && !popoverRef.current.contains(e.target as Node) &&
        buttonRef.current && !buttonRef.current.contains(e.target as Node)
      ) setOpenMenuId(null);
    };
    document.addEventListener('mousedown', onClickOutside);
    window.addEventListener('scroll', close, true);
    window.addEventListener('resize', close);
    return () => {
      document.removeEventListener('mousedown', onClickOutside);
      window.removeEventListener('scroll', close, true);
      window.removeEventListener('resize', close);
    };
  }, [isOpen, setOpenMenuId]);

  return (
    <th className="table-header-cell" style={{ verticalAlign: 'middle', minWidth: '110px' }}>
      {header.isPlaceholder ? null : (
        <div className="table-header-content" style={{ cursor: 'default' }}>
          <span style={{ fontWeight: 700 }}>
            {flexRender(column.columnDef.header, header.getContext())}
          </span>

          {(column.getCanSort() || column.getCanFilter() || column.getCanHide()) && (
            <button
              ref={buttonRef}
              className={`table-menu-button ${isOpen || isFiltered || isSortedAsc || isSortedDesc ? 'active' : ''}`}
              onClick={(e) => { e.stopPropagation(); isOpen ? setOpenMenuId(null) : openMenu(); }}
            >
              <ListFilter size={14} />
            </button>
          )}

          {isOpen && pos && (
            <div
              className="table-popover"
              ref={popoverRef}
              onClick={(e) => e.stopPropagation()}
              style={{ position: 'fixed', top: pos.top, left: pos.left, marginTop: 0 }}
            >
              {column.getCanSort() && (
                <>
                  <div className={`table-popover-item ${isSortedAsc ? 'active' : ''}`} onClick={() => { column.toggleSorting(false); setOpenMenuId(null); }}>
                    <ArrowDownAZ size={14} /> Sort A–Z
                  </div>
                  <div className={`table-popover-item ${isSortedDesc ? 'active' : ''}`} onClick={() => { column.toggleSorting(true); setOpenMenuId(null); }}>
                    <ArrowUpAZ size={14} /> Sort Z–A
                  </div>
                  {(isSortedAsc || isSortedDesc) && (
                    <div className="table-popover-item" onClick={() => { column.clearSorting(); setOpenMenuId(null); }}>
                      <X size={14} /> Clear Sort
                    </div>
                  )}
                </>
              )}

              {column.getCanFilter() && (
                <>
                  <div className="table-popover-divider" />
                  <div style={{ padding: '0.2rem' }}>
                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginBottom: '0.3rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Filter</div>
                    <FilterInput column={column} table={table} />
                    {isFiltered && (
                      <button className="btn btn-outline" style={{ width: '100%', marginTop: '0.5rem', padding: '0.2rem', fontSize: '0.7rem' }} onClick={() => column.setFilterValue(undefined)}>Clear Filter</button>
                    )}
                  </div>
                </>
              )}

              {column.getCanHide() && (
                <>
                  <div className="table-popover-divider" />
                  <div className="table-popover-item" onClick={() => { column.toggleVisibility(false); setOpenMenuId(null); }}>
                    <EyeOff size={14} /> Hide column
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      )}
    </th>
  );
}

/* ---------------------------------------------------------------------------
   Columns visibility menu
--------------------------------------------------------------------------- */

function ColumnsMenu({ table }: { table: any }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const onClick = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener('mousedown', onClick);
    return () => document.removeEventListener('mousedown', onClick);
  }, []);
  const cols = table.getAllLeafColumns().filter((c: any) => c.getCanHide());
  const hiddenCount = cols.filter((c: any) => !c.getIsVisible()).length;
  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button className="btn btn-outline" style={{ padding: '0.35rem 0.7rem', fontSize: '0.72rem' }} onClick={() => setOpen(o => !o)}>
        <Columns3 size={14} /> Columns{hiddenCount > 0 ? ` (${hiddenCount} hidden)` : ''}
      </button>
      {open && (
        <div className="table-popover" style={{ position: 'absolute', top: '100%', right: 0, left: 'auto', marginTop: 4, minWidth: 190, maxHeight: 320, overflowY: 'auto' }}>
          {cols.map((col: any) => {
            const label = typeof col.columnDef.header === 'string' ? col.columnDef.header : col.id;
            return (
              <label key={col.id} className="table-popover-item" style={{ cursor: 'pointer' }}>
                <input type="checkbox" checked={col.getIsVisible()} onChange={col.getToggleVisibilityHandler()} />
                {label}
              </label>
            );
          })}
        </div>
      )}
    </div>
  );
}

/* ---------------------------------------------------------------------------
   Table
--------------------------------------------------------------------------- */

const versionSort = (rowA: any, rowB: any, columnId: string) => {
  const a = rowA.getValue(columnId) as string;
  const b = rowB.getValue(columnId) as string;
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;
  const aParts = a.split('.');
  const bParts = b.split('.');
  for (let i = 0; i < Math.max(aParts.length, bParts.length); i++) {
    const aNum = parseInt(aParts[i] || '0', 10);
    const bNum = parseInt(bParts[i] || '0', 10);
    if (!isNaN(aNum) && !isNaN(bNum)) { if (aNum !== bNum) return aNum - bNum; }
    else if ((aParts[i] || '') !== (bParts[i] || '')) return (aParts[i] || '').localeCompare(bParts[i] || '');
  }
  return 0;
};

export function DataTable({ data, columns, initialColumnVisibility }: { data: any[]; columns: ColumnDef<any, any>[]; initialColumnVisibility?: Record<string, boolean> }) {
  const [sorting, setSorting] = useState<any[]>([]);
  const [columnFilters, setColumnFilters] = useState<any[]>([]);
  const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>(initialColumnVisibility ?? {});
  const [openMenuId, setOpenMenuId] = useState<string | null>(null);

  const processedColumns = columns.map(col => {
    if ((col as any).accessorKey === 'code_version') return { ...col, sortingFn: versionSort };
    return col;
  });

  const table = useReactTable({
    data,
    columns: processedColumns,
    state: { sorting, columnFilters, columnVisibility },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  // Synced top scrollbar for wide tables.
  const scrollRef = useRef<HTMLDivElement>(null);
  const topRef = useRef<HTMLDivElement>(null);
  const [dims, setDims] = useState({ scroll: 0, client: 0 });
  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const measure = () => setDims({ scroll: el.scrollWidth, client: el.clientWidth });
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, [data, columnVisibility, columnFilters]);
  const overflowing = dims.scroll > dims.client + 1;

  const rows = table.getRowModel().rows;

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.6rem', gap: '1rem' }}>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', fontWeight: 600 }}>
          {rows.length}{rows.length !== data.length ? ` of ${data.length}` : ''} row{rows.length === 1 ? '' : 's'}
        </span>
        <ColumnsMenu table={table} />
      </div>

      {overflowing && (
        <div
          ref={topRef}
          onScroll={() => { if (scrollRef.current && topRef.current) scrollRef.current.scrollLeft = topRef.current.scrollLeft; }}
          style={{ overflowX: 'auto', overflowY: 'hidden', border: '1px solid var(--border-color)', borderBottom: 'none' }}
        >
          <div style={{ width: dims.scroll, height: 1 }} />
        </div>
      )}

      <div
        ref={scrollRef}
        onScroll={() => { if (scrollRef.current && topRef.current) topRef.current.scrollLeft = scrollRef.current.scrollLeft; }}
        className="table-container"
        style={{ overflowX: 'auto' }}
      >
        <table>
          <thead>
            {table.getHeaderGroups().map(headerGroup => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map(header => (
                  <ColumnHeader key={header.id} header={header} table={table} openMenuId={openMenuId} setOpenMenuId={setOpenMenuId} />
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {rows.map(row => (
              <tr key={row.id}>
                {row.getVisibleCells().map(cell => (
                  <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length === 0 && (
          <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-muted)' }}>No data available.</div>
        )}
      </div>
    </div>
  );
}
