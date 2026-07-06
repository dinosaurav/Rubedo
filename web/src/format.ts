export function fmtTime(v?: string | null): string {
  return v ? new Date(v).toLocaleString() : '—';
}

export function durationMs(start?: string | null, end?: string | null): number | null {
  if (!start || !end) return null;
  const ms = new Date(end).getTime() - new Date(start).getTime();
  return isNaN(ms) || ms < 0 ? null : ms;
}

export function fmtDuration(ms: number | null): string {
  if (ms == null) return '—';
  const s = ms / 1000;
  if (s < 60) return `${s < 10 ? s.toFixed(1) : Math.round(s)}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${Math.round(s % 60)}s`;
}

// completed | completed_with_failures | failed | running
export function runStatusClass(status: string): string {
  if (status === 'completed') return 'success';
  if (status === 'failed') return 'error';
  if (status === 'running') return 'info';
  return 'warning';
}

// created | reused | failed | blocked | filtered | pending
export function coordStatusClass(status: string): string {
  switch (status) {
    case 'created': return 'success';
    case 'reused': return 'info';
    case 'failed': return 'error';
    case 'blocked': return 'warning';
    default: return 'info';
  }
}
