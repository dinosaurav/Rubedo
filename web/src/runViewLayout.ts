/**
 * Pure layout helpers for the grouped Run View — mirrors the reference
 * ai-table-v2 layout.ts idioms against Rubedo's definition-derived scopes.
 */
import type { RunViewScope, RunViewStep } from './runViewTypes';

export function scopeEq(a: RunViewScope, b: RunViewScope): boolean {
  return a.kind === b.kind && a.expand_step === b.expand_step;
}

export const PARENT_SCOPE: RunViewScope = { kind: 'parent', expand_step: null };

export function childScope(expandStep: string): RunViewScope {
  return { kind: 'child', expand_step: expandStep };
}

/** Steps whose cells render on a row at ``ownScope`` (not aggregates). */
export function ownCellSteps(steps: RunViewStep[], ownScope: RunViewScope): RunViewStep[] {
  return steps.filter(
    (s) => s.shape !== 'aggregate' && scopeEq(s.scope, ownScope),
  );
}

/** Nested expand/join blocks that open under this scope. */
export function ownExpandSteps(steps: RunViewStep[], ownScope: RunViewScope): RunViewStep[] {
  return steps.filter(
    (s) =>
      (s.shape === 'expand' || s.shape === 'join') &&
      !(s.scope.kind === 'parent') &&
      scopeEq(s.source_scope, ownScope),
  );
}

/** Summary-strip aggregates attached at this nesting level's expand. */
export function ownSummarySteps(
  steps: RunViewStep[],
  expandStep: string | null,
): RunViewStep[] {
  return steps.filter(
    (s) => s.shape === 'aggregate' && s.scope.expand_step === expandStep,
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
