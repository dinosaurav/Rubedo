/**
 * Renders a pipeline definition snapshot (Run.definition_json) as an SVG DAG.
 *
 * Layout: longest-path layering — each step sits one column right of its
 * deepest dependency. Pipelines here are small (a handful of steps), so no
 * crossing minimization is attempted.
 *
 * When stepCounts + isLive are provided, each node shows a live progress
 * bar and is styled as waiting / active / done based on whether lanes have
 * started flowing through it.
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
  source?: string;
  executor?: string;
  group_key?: string;
  join_on?: Record<string, string>;
  on_failed?: string;
  workers?: number;
}

type StepCounts = Record<string, Record<string, number>>;
type StepState = 'waiting' | 'active' | 'done';

const NODE_W = 168;
const NODE_H = 58;
const COUNTS_H = 16;
const PROGRESS_H = 6;
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

const STATE_COLORS: Record<StepState, string> = {
  waiting: 'var(--text-muted)',
  active: 'var(--accent-primary)',
  done: 'var(--status-success, #22c55e)',
};

function sumCounts(counts?: Record<string, number>): number {
  if (!counts) return 0;
  return Object.values(counts).reduce((a, b) => a + b, 0);
}

function survivingLanes(counts?: Record<string, number>): number {
  if (!counts) return 0;
  return (counts.created ?? 0) + (counts.reused ?? 0);
}

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

function computeExpectedTotal(
  step: StepDef,
  stepCounts: StepCounts | undefined,
  byName: Record<string, StepDef>,
): number {
  const parents = (step.depends_on ?? []).filter((d) => byName[d]);
  if (parents.length === 0) {
    if (step.shape === 'expand') return sumCounts(stepCounts?.[step.name]);
    return 1;
  }
  if (step.shape === 'reduce') return 1;
  return parents.reduce((total, p) => total + survivingLanes(stepCounts?.[p]), 0);
}

function computeStepState(
  step: StepDef,
  stepCounts: StepCounts | undefined,
  isLive: boolean,
  children: Record<string, StepDef[]>,
  byName: Record<string, StepDef>,
): StepState {
  if (!isLive) return 'done';
  if (!stepCounts) return 'waiting';

  const finished = sumCounts(stepCounts[step.name]);
  const kids = children[step.name] ?? [];
  const allChildrenStarted =
    kids.length > 0 && kids.every((c) => sumCounts(stepCounts[c.name]) > 0);

  if (finished > 0 && allChildrenStarted) return 'done';
  if (finished > 0) return 'active';

  const parents = (step.depends_on ?? []).filter((d) => byName[d]);
  if (parents.length === 0) return 'active';
  const allParentsDone =
    parents.length > 0 &&
    parents.every((p) => {
      const pFinished = sumCounts(stepCounts[p]);
      const pKids = children[p] ?? [];
      const pAllChildrenStarted =
        pKids.length > 0 && pKids.every((c) => sumCounts(stepCounts[c.name]) > 0);
      return pFinished > 0 && pAllChildrenStarted;
    });
  return allParentsDone ? 'active' : 'waiting';
}

import { useState } from 'react';

export default function DagView({
  steps,
  stepCounts,
  isLive,
  onStepClick,
}: {
  steps: StepDef[];
  stepCounts?: StepCounts;
  isLive?: boolean;
  onStepClick?: (stepName: string) => void;
}) {
  const [selectedStep, setSelectedStep] = useState<string | null>(null);
  if (!steps?.length) return null;
  const hasCounts = !!stepCounts;
  const nodeH = NODE_H + (hasCounts ? COUNTS_H + PROGRESS_H : 0);

  // Build lookup tables
  const byName: Record<string, StepDef> = {};
  steps.forEach((s) => { byName[s.name] = s; });
  const children: Record<string, StepDef[]> = {};
  steps.forEach((s) => {
    (s.depends_on ?? []).forEach((dep) => {
      if (byName[dep]) (children[dep] ??= []).push(s);
    });
  });

  // Compute per-step state and progress
  const stepStates: Record<string, StepState> = {};
  const stepPct: Record<string, number> = {};
  steps.forEach((s) => {
    stepStates[s.name] = computeStepState(s, stepCounts, !!isLive, children, byName);
    const expected = computeExpectedTotal(s, stepCounts, byName);
    const finished = sumCounts(stepCounts?.[s.name]);
    stepPct[s.name] = expected > 0 ? Math.min(100, Math.round((finished / expected) * 100)) : 0;
  });

  // Longest-path layering
  const layerOf: Record<string, number> = {};
  const layer = (name: string): number => {
    if (layerOf[name] !== undefined) return layerOf[name];
    const s = byName[name];
    const deps = (s?.depends_on ?? []).filter((d) => byName[d]);
    layerOf[name] = deps.length ? 1 + Math.max(...deps.map(layer)) : 0;
    return layerOf[name];
  };
  steps.forEach((s) => layer(s.name));

  const columns: StepDef[][] = [];
  steps.forEach((s) => {
    const l = layerOf[s.name];
    (columns[l] ??= []).push(s);
  });

  const pos: Record<string, { x: number; y: number }> = {};
  const maxRows = Math.max(...columns.map((c) => c.length));
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
          <marker id="dag-arrow-active" viewBox="0 0 8 8" refX="7" refY="4"
                  markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 8 4 L 0 8 z" fill="var(--accent-primary)" />
          </marker>
          <marker id="dag-arrow-done" viewBox="0 0 8 8" refX="7" refY="4"
                  markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 8 4 L 0 8 z" fill="var(--status-success, #22c55e)" />
          </marker>
        </defs>

        {steps.flatMap((s) =>
          (s.depends_on ?? []).filter((d) => pos[d]).map((dep) => {
            const from = pos[dep];
            const to = pos[s.name];
            const x1 = from.x + NODE_W;
            const y1 = from.y + nodeH / 2;
            const x2 = to.x;
            const y2 = to.y + nodeH / 2;
            const mx = (x1 + x2) / 2;
            const parentState = stepStates[dep] ?? 'waiting';
            const stroke = STATE_COLORS[parentState];
            const markerEnd =
              parentState === 'done' ? 'url(#dag-arrow-done)'
              : parentState === 'active' ? 'url(#dag-arrow-active)'
              : 'url(#dag-arrow)';
            const style =
              parentState === 'active'
                ? { animation: 'pulse-stroke 2s infinite', opacity: 0.7 }
                : parentState === 'waiting'
                ? { opacity: 0.3 }
                : undefined;
            return (
              <path key={`${dep}->${s.name}`}
                    d={`M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2 - 3} ${y2}`}
                    fill="none" stroke={stroke} strokeWidth={parentState === 'waiting' ? 1 : 1.5}
                    markerEnd={markerEnd}
                    style={style} />
            );
          })
        )}

        {steps.map((s) => {
          const p = pos[s.name];
          const badges = policyBadges(s);
          const state = stepStates[s.name] ?? 'waiting';
          const pct = stepPct[s.name] ?? 0;
          const stateColor = STATE_COLORS[state];
          const finished = sumCounts(stepCounts?.[s.name]);
          const expected = computeExpectedTotal(s, stepCounts, byName);

          return (
            <g key={s.name} data-step={s.name}
               onClick={() => {
                 if (onStepClick) {
                   onStepClick(s.name);
                 } else {
                   setSelectedStep((prev) => (prev === s.name ? null : s.name));
                 }
               }}
               style={{ cursor: 'pointer', opacity: state === 'waiting' ? 0.45 : 1, transition: 'opacity 0.3s ease' }}>
              <rect x={p.x} y={p.y} width={NODE_W} height={nodeH} rx={8}
                    fill="var(--bg-tertiary)"
                    stroke={s.skip_cache ? 'var(--text-muted)' : stateColor}
                    strokeWidth={state === 'done' ? 2 : 1.5}
                    strokeDasharray={s.skip_cache ? '5 4' : state === 'waiting' ? '4 3' : undefined}
                    className={state === 'active' ? 'pulse-border' : ''}
                    style={{ transition: 'stroke 0.3s ease, opacity 0.3s ease' }} />
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
              {hasCounts && (
                <>
                  <text x={p.x + 12} y={p.y + NODE_H + 8} fontSize={10} style={{ transition: 'all 0.3s ease' }}>
                    {countsLine(stepCounts![s.name]).map((c, i) => (
                      <tspan key={c.label} dx={i === 0 ? 0 : 8} fill={c.color} style={{ transition: 'fill 0.3s ease' }}>
                        {c.label}
                      </tspan>
                    ))}
                  </text>
                  {/* Progress bar */}
                  <rect x={p.x + 8} y={p.y + nodeH - PROGRESS_H - 2} width={NODE_W - 16} height={4} rx={2}
                        fill="var(--bg-secondary, #1e293b)" />
                  <rect x={p.x + 8} y={p.y + nodeH - PROGRESS_H - 2}
                        width={Math.max(0, (NODE_W - 16) * pct / 100)} height={4} rx={2}
                        fill={stateColor}
                        style={{ transition: 'width 0.5s ease, fill 0.3s ease' }} />
                  {/* Percentage label */}
                  {state === 'active' && expected > 0 && (
                    <text x={p.x + NODE_W - 12} y={p.y + NODE_H + 8} fontSize={10} fontWeight={600}
                          fill={stateColor} textAnchor="end">
                      {finished}/{expected}
                    </text>
                  )}
                  {state === 'done' && finished > 0 && (
                    <text x={p.x + NODE_W - 12} y={p.y + NODE_H + 8} fontSize={10}
                          fill="var(--text-muted)" textAnchor="end">
                      {finished} {expected === 1 ? '' : `/${expected}`}
                    </text>
                  )}
                </>
              )}
            </g>
          );
        })}
      </svg>

      {selectedStep && byName[selectedStep] && (
        <StepDetail step={byName[selectedStep]} />
      )}
    </div>
  );
}

function StepDetail({ step }: { step: StepDef }) {
  const specs: { label: string; value: string }[] = [
    { label: 'name', value: step.name },
    { label: 'version', value: step.version },
    { label: 'shape', value: step.shape ?? 'map' },
    { label: 'depends_on', value: step.depends_on.length ? step.depends_on.join(', ') : '(root)' },
    { label: 'workers', value: String(step.workers) },
    { label: 'code', value: step.code ?? 'warn' },
  ];
  if (step.skip_cache) specs.push({ label: 'skip_cache', value: 'true' });
  if (step.retries) specs.push({ label: 'retries', value: String(step.retries) });
  if (step.rate_limit) specs.push({ label: 'rate_limit', value: step.rate_limit });
  if (step.stale_after_seconds !== undefined) specs.push({ label: 'stale_after', value: `${step.stale_after_seconds}s` });
  if (step.executor && step.executor !== 'thread') specs.push({ label: 'executor', value: step.executor });
  if (step.group_key) specs.push({ label: 'group_key', value: step.group_key });
  if (step.join_on) specs.push({ label: 'join_on', value: JSON.stringify(step.join_on) });
  if (step.on_failed && step.on_failed !== 'use_passed') specs.push({ label: 'on_failed', value: step.on_failed });

  return (
    <div style={{
      marginTop: '0.75rem',
      padding: '0.75rem 1rem',
      background: 'var(--bg-tertiary)',
      border: '1px solid var(--border-color)',
      borderRadius: '8px',
      fontSize: '0.85rem',
    }}>
      <div style={{ fontWeight: 600, marginBottom: '0.5rem', fontFamily: 'ui-monospace, monospace' }}>
        {step.name}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '0.25rem 1rem', marginBottom: step.source ? '0.75rem' : 0 }}>
        {specs.map((s) => (
          <div key={s.label} style={{ display: 'contents' }}>
            <span style={{ color: 'var(--text-muted)' }}>{s.label}</span>
            <span style={{ fontFamily: 'ui-monospace, monospace', fontSize: '0.8rem' }}>{s.value}</span>
          </div>
        ))}
      </div>
      {step.source && (
        <details>
          <summary style={{ cursor: 'pointer', color: 'var(--text-muted)', fontSize: '0.8rem', marginBottom: '0.25rem' }}>
            source code
          </summary>
          <pre style={{
            marginTop: '0.5rem',
            padding: '0.75rem',
            background: 'var(--bg-secondary, #0f172a)',
            borderRadius: '6px',
            overflow: 'auto',
            fontSize: '0.8rem',
            fontFamily: 'ui-monospace, monospace',
            color: 'var(--text-primary)',
            lineHeight: 1.5,
          }}>
            <code>{step.source}</code>
          </pre>
        </details>
      )}
    </div>
  );
}
