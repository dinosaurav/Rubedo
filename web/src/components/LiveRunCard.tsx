import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchRun, API_URL } from '../api';
import { fmtTime, fmtDuration, durationMs, runStatusClass } from '../format';
import DagView from './DagView';

export default function LiveRunCard({ runId }: { runId: string }) {
  const [run, setRun] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    let source: EventSource | null = null;

    async function init() {
      try {
        const detail = await fetchRun(runId);
        if (cancelled) return;
        setRun(detail);

        if (detail.status === 'running') {
          source = new EventSource(`${API_URL}/runs/${runId}/stream`);
          source.onmessage = (e) => {
            const data = JSON.parse(e.data);
            setRun((prev: any) => ({
              ...prev,
              status: data.status,
              created_count: data.totals.created,
              reused_count: data.totals.reused,
              failed_count: data.totals.failed,
              blocked_count: data.totals.blocked,
              filtered_count: data.totals.filtered,
              by_step: data.by_step,
            }));
            if (data.status !== 'running') {
              source?.close();
              source = null;
            }
          };
          source.onerror = () => {
            source?.close();
            source = null;
          };
        }
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    }

    init();
    return () => {
      cancelled = true;
      source?.close();
    };
  }, [runId]);

  if (error) return <div className="card">API unreachable: {error}</div>;
  if (!run) return <div className="card">Loading live run…</div>;

  const isRunning = run.status === 'running';

  return (
    <div className="card" style={{ marginBottom: '1.5rem', border: isRunning ? '1px solid var(--accent-primary)' : undefined, boxShadow: isRunning ? '0 0 12px rgba(59,130,246,0.15)' : undefined }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: '0.5rem', marginBottom: '0.75rem' }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.75rem' }}>
          <Link to={`/runs/${run.id}`} style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem', color: 'var(--accent-primary)' }}>
            {run.id}
          </Link>
          <span style={{ fontWeight: 600 }}>{run.pipeline_id ?? '—'}</span>
          <span className={`badge badge-${runStatusClass(run.status)}`}>{run.status}</span>
        </div>
        <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
          started {fmtTime(run.started_at)}
          {!isRunning && run.finished_at && ` · ${fmtDuration(durationMs(run.started_at, run.finished_at))}`}
        </div>
      </div>

      <div className="stats-grid" style={{ marginBottom: '1rem' }}>
        {[
          ['Created', run.created_count, 'var(--status-success)'],
          ['Reused', run.reused_count, 'var(--status-info)'],
          ['Failed', run.failed_count, 'var(--status-error)'],
          ['Blocked', run.blocked_count, 'var(--status-warning)'],
        ].map(([label, val, color]) => (
          <div key={label as string} className="card stat-card" style={{ padding: '0.5rem 0.75rem' }}>
            <div className="stat-label">{label}</div>
            <div className="stat-value" style={{ color: color as string, fontSize: '1.1rem' }}>{val as number}</div>
          </div>
        ))}
      </div>

      {run.definition?.steps?.length > 0 && (
        <DagView steps={run.definition.steps} stepCounts={run.by_step ?? undefined} isLive={isRunning} />
      )}
    </div>
  );
}
