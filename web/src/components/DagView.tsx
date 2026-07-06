/**
 * Renders a pipeline definition snapshot (Run.definition_json) as an SVG DAG.
 *
 * Layout: longest-path layering — each step sits one column right of its
 * deepest dependency. Pipelines here are small (a handful of steps), so no
 * crossing minimization is attempted.
 */

interface StepDef {
  name: string;
  version: string;
  depends_on: string[];
  skip_cache?: boolean;
  retries?: number;
  rate_limit?: string;
  stale_after_seconds?: number;
  params_schema?: any;
  code?: string;
  shape?: string;
}

type StepCounts = Record<string, Record<string, number>>;

const NODE_W = 168;
const NODE_H = 58;
const COUNTS_H = 16;
const COL_GAP = 72;
const ROW_GAP = 26;
const PAD = 16;

const COUNT_COLORS: Record<string, string> = {
  created: 'var(--status-success, #22c55e)',
  reused: 'var(--status-info, #3b82f6)',
  failed: 'var(--status-error, #ef4444)',
  blocked: 'var(--status-warning, #f59e0b)',
  filtered: 'var(--text-muted)',
};

function countsLine(counts?: Record<string, number>): { label: string; color: string }[] {
  if (!counts) return [];
  return Object.entries(counts)
    .filter(([, v]) => v > 0)
    .map(([k, v]) => ({ label: `${v} ${k}`, color: COUNT_COLORS[k] ?? 'var(--text-muted)' }));
}

function policyBadges(s: StepDef): string[] {
  const badges: string[] = [];
  if (s.shape === 'reduce') badges.push('reduce');
  if (s.skip_cache) badges.push('util');
  if (s.retries) badges.push(`retries ${s.retries}`);
  if (s.rate_limit) badges.push(s.rate_limit);
  if (s.stale_after_seconds) badges.push(`ttl ${s.stale_after_seconds}s`);
  if (s.code === 'auto') badges.push('code:auto');
  if (s.params_schema) badges.push('params');
  return badges;
}

export default function DagView({ steps, stepCounts }: { steps: StepDef[]; stepCounts?: StepCounts }) {
  if (!steps?.length) return null;
  const nodeH = stepCounts ? NODE_H + COUNTS_H : NODE_H;

  // Longest-path layering
  const layerOf: Record<string, number> = {};
  const byName: Record<string, StepDef> = {};
  steps.forEach(s => { byName[s.name] = s; });
  const layer = (name: string): number => {
    if (layerOf[name] !== undefined) return layerOf[name];
    const s = byName[name];
    const deps = (s?.depends_on ?? []).filter(d => byName[d]);
    layerOf[name] = deps.length ? 1 + Math.max(...deps.map(layer)) : 0;
    return layerOf[name];
  };
  steps.forEach(s => layer(s.name));

  const columns: StepDef[][] = [];
  steps.forEach(s => {
    const l = layerOf[s.name];
    (columns[l] ??= []).push(s);
  });

  const pos: Record<string, { x: number; y: number }> = {};
  const maxRows = Math.max(...columns.map(c => c.length));
  const height = PAD * 2 + maxRows * nodeH + (maxRows - 1) * ROW_GAP;
  columns.forEach((col, ci) => {
    const colHeight = col.length * nodeH + (col.length - 1) * ROW_GAP;
    const yStart = (height - colHeight) / 2;
    col.forEach((s, ri) => {
      pos[s.name] = {
        x: PAD + ci * (NODE_W + COL_GAP),
        y: yStart + ri * (nodeH + ROW_GAP),
      };
    });
  });
  const width = PAD * 2 + columns.length * NODE_W + (columns.length - 1) * COL_GAP;

  return (
    <div style={{ overflowX: 'auto' }}>
      <svg width={width} height={height} style={{ display: 'block' }}>
        <defs>
          <marker id="dag-arrow" viewBox="0 0 8 8" refX="7" refY="4"
                  markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 8 4 L 0 8 z" fill="var(--text-muted)" />
          </marker>
        </defs>

        {steps.flatMap(s =>
          (s.depends_on ?? []).filter(d => pos[d]).map(dep => {
            const from = pos[dep];
            const to = pos[s.name];
            const x1 = from.x + NODE_W;
            const y1 = from.y + nodeH / 2;
            const x2 = to.x;
            const y2 = to.y + nodeH / 2;
            const mx = (x1 + x2) / 2;
            return (
              <path key={`${dep}->${s.name}`}
                    d={`M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2 - 3} ${y2}`}
                    fill="none" stroke="var(--text-muted)" strokeWidth={1.5}
                    markerEnd="url(#dag-arrow)" />
            );
          })
        )}

        {steps.map(s => {
          const p = pos[s.name];
          const badges = policyBadges(s);
          return (
            <g key={s.name}>
              <rect x={p.x} y={p.y} width={NODE_W} height={nodeH} rx={8}
                    fill="var(--bg-tertiary)"
                    stroke={s.skip_cache ? 'var(--text-muted)' : (s.shape === 'reduce' ? 'var(--status-warning)' : 'var(--accent-primary)')}
                    strokeWidth={s.shape === 'reduce' ? 2 : 1.5}
                    strokeDasharray={s.skip_cache ? '5 4' : undefined} />
              <text x={p.x + 12} y={p.y + 22} fill="var(--text-primary)"
                    fontSize={13} fontWeight={600} fontFamily="ui-monospace, monospace">
                {s.name}
              </text>
              <text x={p.x + 12} y={p.y + 38} fill="var(--text-secondary)" fontSize={11}>
                {s.version}
              </text>
              {badges.length > 0 && (
                <text x={p.x + 12} y={p.y + 51} fill="var(--text-muted)" fontSize={10}>
                  {badges.join(' · ')}
                </text>
              )}
              {stepCounts && (
                <text x={p.x + 12} y={p.y + NODE_H + 8} fontSize={10}>
                  {countsLine(stepCounts[s.name]).map((c, i) => (
                    <tspan key={c.label} dx={i === 0 ? 0 : 8} fill={c.color}>
                      {c.label}
                    </tspan>
                  ))}
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
