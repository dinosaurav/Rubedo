import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ChevronDown, ChevronRight, X } from 'lucide-react';
import { fetchRun, API_URL } from '../api';
import { fmtTime, fmtDuration, durationMs, runStatusClass } from '../format';
import DagView from './DagView';

export default function LiveRunCard({
  runId,
  onDismiss,
}: {
  runId: string;
  onDismiss: () => void;
}) {
  const [run, setRun] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(true);

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
  const Chevron = expanded ? ChevronDown : ChevronRight;

  return (
    <div className="card" style={{
      marginBottom: '1rem',
      border: isRunning ? '1px solid var(--accent-primary)' : '1px solid var(--border-color)',
      boxShadow: isRunning ? '0 0 12px rgba(59,130,246,0.15)' : undefined,
      transition: 'border-color 0.3s ease, box-shadow 0.3s ease',
    }}>
      {/* Header — always visible, click to expand/collapse */}
      <div
        onClick={() => setExpanded((e) => !e)}
        style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem', cursor: 'pointer', userSelect: 'none' }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
          <Chevron size={18} style={{ color: 'var(--text-muted)', flexShrink: 0 }} />
          <Link
            to={`/runs/${run.id}`}
            onClick={(e) => e.stopPropagation()}
            style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem', color: 'var(--accent-primary)' }}
          >
            {run.id}
          </Link>
          <span style={{ fontWeight: 600 }}>{run.pipeline_id ?? '—'}</span>
          <span className={`badge badge-${runStatusClass(run.status)}`}>{run.status}</span>
          <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
            {isRunning
              ? `started ${fmtTime(run.started_at)}`
              : run.finished_at
                ? `${fmtDuration(durationMs(run.started_at, run.finished_at))} · ${run.created_count} created`
                : fmtTime(run.started_at)}
          </span>
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); onDismiss(); }}
          title="Hide"
          style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', padding: '0.25rem', display: 'flex', alignItems: 'center' }}
        >
          <X size={16} />
        </button>
      </div>

      {/* Expanded view — DAG + stats */}
      {expanded && (
        <div style={{ marginTop: '0.75rem' }}>
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
            <DagView steps={run.definition.steps} stepCounts={run.by_step ?? undefined} isLive={isRunning} pipelineId={run.pipeline_id ?? undefined} />
          )}
        </div>
      )}
    </div>
  );
}
