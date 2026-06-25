import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { fetchRun, fetchRunCoordinates, fetchRunEvents } from '../api';
import { DataTable } from '../components/DataTable';
import type { ColumnDef } from '@tanstack/react-table';

export default function RunDetail() {
  const { runId } = useParams();
  const [run, setRun] = useState<any>(null);
  const [coords, setCoords] = useState<any[]>([]);
  const [events, setEvents] = useState<any[]>([]);
  const [tab, setTab] = useState<'coords' | 'events'>('coords');

  useEffect(() => {
    if (runId) {
      fetchRun(runId).then(setRun);
      fetchRunCoordinates(runId).then(setCoords);
      fetchRunEvents(runId).then(setEvents);
    }
  }, [runId]);

  if (!run) return <div>Loading...</div>;

  const coordColumns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'coordinate',
      header: 'Coordinate',
    },
    {
      accessorKey: 'status',
      header: 'Status',
      meta: { filterVariant: 'select' },
      cell: (info) => {
        const val = info.getValue();
        return (
          <span className={`badge badge-${val === 'created' ? 'success' : val === 'failed' ? 'error' : 'info'}`}>
            {val}
          </span>
        );
      },
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
      accessorKey: 'error_message',
      header: 'Error',
      cell: (info) => {
        const val = info.getValue();
        return val ? <span style={{ color: 'var(--status-error)' }}>{val}</span> : '-';
      },
    }
  ];

  const eventColumns: ColumnDef<any, any>[] = [
    {
      accessorKey: 'timestamp',
      header: 'Time',
      cell: (info) => new Date(info.getValue()).toLocaleString(),
    },
    {
      accessorKey: 'level',
      header: 'Level',
      meta: { filterVariant: 'select' },
      cell: (info) => {
        const val = info.getValue();
        return (
          <span className={`badge badge-${val === 'error' ? 'error' : val === 'warning' ? 'warning' : 'info'}`}>
            {val}
          </span>
        );
      },
    },
    {
      accessorKey: 'event_type',
      header: 'Event Type',
      meta: { filterVariant: 'select' },
    },
    {
      accessorKey: 'coordinate',
      header: 'Coordinate',
      cell: (info) => info.getValue() || '-',
    },
    {
      accessorKey: 'message',
      header: 'Message',
    }
  ];

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Run {run.id}</h1>
      </div>
      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-label">Status</div>
          <div className="stat-value">
            <span className={`badge badge-${run.status === 'succeeded' ? 'success' : run.status === 'failed' ? 'error' : 'warning'}`}>
              {run.status}
            </span>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Created</div>
          <div className="stat-value" style={{ color: 'var(--status-success)' }}>{run.created_count}</div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Reused</div>
          <div className="stat-value" style={{ color: 'var(--status-info)' }}>{run.reused_count}</div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Failed</div>
          <div className="stat-value" style={{ color: 'var(--status-error)' }}>{run.failed_count}</div>
        </div>
      </div>

      <div style={{ marginBottom: '1rem', display: 'flex', gap: '1rem' }}>
        <button className={`btn ${tab === 'coords' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setTab('coords')}>Coordinates</button>
        <button className={`btn ${tab === 'events' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setTab('events')}>Events</button>
      </div>

      {tab === 'coords' && <DataTable data={coords} columns={coordColumns} />}
      {tab === 'events' && <DataTable data={events} columns={eventColumns} />}
    </div>
  );
}
