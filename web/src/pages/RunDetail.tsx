import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { fetchRun, fetchRunCoordinates, fetchRunEvents } from '../api';
import { DataTable, TruncatedText, HashCell } from '../components/DataTable';
import DagView from '../components/DagView';
import { fmtTime, fmtDuration, durationMs, runStatusClass, coordStatusClass } from '../format';
import type { ColumnDef } from '@tanstack/react-table';

export default function RunDetail() {
  const { runId } = useParams();
  const [run, setRun] = useState<any>(null);
  const [coords, setCoords] = useState<any[]>([]);
  const [events, setEvents] = useState<any[]>([]);
  const [tab, setTab] = useState<'coords' | 'events'>('coords');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (runId) {
      setLoading(true);
      Promise.all([
        fetchRun(runId).then(setRun),
        fetchRunCoordinates(runId).then(setCoords),
        fetchRunEvents(runId).then(setEvents),
      ]).catch(e => setError(String(e))).finally(() => setLoading(false));
    }
  }, [runId]);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>API unreachable: {error}</div>;
  if (!run) return <div>Not found</div>;

  const coordColumns: ColumnDef<any, any>[] = [
    { accessorKey: 'coordinate', header: 'Coordinate', cell: (info) => <TruncatedText value={info.getValue()} /> },
    { accessorKey: 'step_name', header: 'Step', meta: { filterVariant: 'select' } },
    {
      accessorKey: 'status',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => <span className={`badge badge-${coordStatusClass(info.getValue())}`}>{info.getValue()}</span>,
    },
    {
      accessorKey: 'output_address',
      header: 'Output Address',
      cell: (info) => info.getValue() ? <HashCell value={info.getValue()} to={`/objects/${info.getValue()}`} /> : '—',
    },
    { accessorKey: 'input_hash', header: 'Input Hash', cell: (info) => <HashCell value={info.getValue()} /> },
    { accessorKey: 'error_type', header: 'Error Type', meta: { filterVariant: 'select' }, cell: (info) => info.getValue() || '—' },
    {
      accessorKey: 'error_message',
      header: 'Error',
      cell: (info) => info.getValue()
        ? <span style={{ color: 'var(--status-error)' }}><TruncatedText value={info.getValue()} maxWidth={320} /></span>
        : '—',
    },
    { accessorKey: 'created_at', header: 'Time', cell: (info) => fmtTime(info.getValue()) },
  ];

  const eventColumns: ColumnDef<any, any>[] = [
    { accessorKey: 'timestamp', header: 'Time', cell: (info) => fmtTime(info.getValue()) },
    {
      accessorKey: 'level',
      header: 'Level',
      meta: { filterVariant: 'select' },
      cell: (info) => <span className={`badge badge-${info.getValue() === 'error' ? 'error' : info.getValue() === 'warning' ? 'warning' : 'info'}`}>{info.getValue()}</span>,
    },
    { accessorKey: 'event_type', header: 'Event Type', meta: { filterVariant: 'select' } },
    { accessorKey: 'coordinate', header: 'Coordinate', cell: (info) => <TruncatedText value={info.getValue()} maxWidth={200} /> },
    { accessorKey: 'message', header: 'Message', cell: (info) => <TruncatedText value={info.getValue()} maxWidth={420} /> },
    { accessorKey: 'data_json', header: 'Data', cell: (info) => <TruncatedText value={info.getValue()} mono maxWidth={240} /> },
  ];

  const stat = (label: string, value: any, color?: string) => (
    <div className="card stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value" style={color ? { color } : undefined}>{value}</div>
    </div>
  );

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title" style={{ fontFamily: 'var(--font-mono)', fontSize: '1.5rem' }}>{run.id}</h1>
      </div>

      <div style={{ marginBottom: '1.5rem', color: 'var(--text-secondary)', fontSize: '0.9rem', display: 'flex', gap: '1.5rem', flexWrap: 'wrap' }}>
        <span><strong>Pipeline:</strong> {run.pipeline_id ?? '—'}</span>
        <span><strong>Source:</strong> {run.source_id ?? '—'}</span>
        <span><strong>Started:</strong> {fmtTime(run.started_at)}</span>
        <span><strong>Duration:</strong> {fmtDuration(durationMs(run.started_at, run.finished_at))}</span>
      </div>

      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-label">Status</div>
          <div className="stat-value">
            <span className={`badge badge-${runStatusClass(run.status)}`}>{run.status}</span>
          </div>
        </div>
        {stat('Created', run.created_count, 'var(--status-success)')}
        {stat('Reused', run.reused_count, 'var(--status-info)')}
        {stat('Failed', run.failed_count, 'var(--status-error)')}
        {stat('Blocked', run.blocked_count, 'var(--status-warning)')}
        {stat('Filtered', run.filtered_count, 'var(--text-muted)')}
      </div>

      {run.definition?.steps?.length > 0 && (
        <div className="card" style={{ marginBottom: '1rem' }}>
          <div className="stat-label" style={{ marginBottom: '0.5rem' }}>Pipeline DAG (as run)</div>
          <DagView steps={run.definition.steps} stepCounts={run.by_step ?? undefined} />
        </div>
      )}

      <div style={{ marginBottom: '1rem', display: 'flex', gap: '1rem' }}>
        <button className={`btn ${tab === 'coords' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setTab('coords')}>Coordinates</button>
        <button className={`btn ${tab === 'events' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setTab('events')}>Events</button>
      </div>

      {tab === 'coords' && (
        <DataTable data={coords} columns={coordColumns} urlKey="coords" initialColumnVisibility={{ input_hash: false, error_type: false }} />
      )}
      {tab === 'events' && (
        <DataTable data={events} columns={eventColumns} urlKey="events" initialColumnVisibility={{ data_json: false }} />
      )}
    </div>
  );
}
