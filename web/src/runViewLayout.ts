/**
 * Pure layout helpers for the grouped Run View.
 */
import type { RunViewScope, RunViewStep } from './runViewTypes';

export function scopeEq(a: RunViewScope, b: RunViewScope): boolean {
  return a.kind === b.kind && a.expand_step === b.expand_step;
}

export function childScope(expandStep: string): RunViewScope {
  return { kind: 'child', expand_step: expandStep };
}

/** Steps whose cells render on a row at ``ownScope`` (not aggregates). */
export function ownCellSteps(steps: RunViewStep[], ownScope: RunViewScope): RunViewStep[] {
  return steps.filter(
    (s) =>
      s.shape !== 'aggregate' &&
      !(s.shape === 'expand' && s.scope.kind === 'child') &&
      scopeEq(s.scope, ownScope),
  );
}

/** Nested expand blocks that open under this scope (never joins). */
export function ownExpandSteps(steps: RunViewStep[], ownScope: RunViewScope): RunViewStep[] {
  return steps.filter(
    (s) =>
      s.shape === 'expand' &&
      s.scope.kind === 'child' &&
      scopeEq(s.source_scope, ownScope),
  );
}

export function formatPreview(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

export function stepsByName(steps: RunViewStep[]): Map<string, RunViewStep> {
  return new Map(steps.map((s) => [s.name, s]));
}
