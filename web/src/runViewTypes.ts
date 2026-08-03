/** Types for GET /api/runs/{id}/view */

export interface RunViewScope {
  kind: string;
  expand_step: string | null;
}

export interface RunViewStep {
  name: string;
  shape: string;
  depends_on: string[];
  scope: RunViewScope;
  source_scope: RunViewScope;
  group_key: string | null;
  version: string | null;
}

export interface RunViewCell {
  coordinate: string;
  step_name: string;
  status: string;
  output_address: string | null;
  error_message: string | null;
  error_type: string | null;
  preview: unknown;
  created_at: string | null;
}

export interface RunViewGroup {
  coordinate: string;
  cells: Record<string, RunViewCell>;
  children: RunViewChildBlock[];
  summary: RunViewCell[];
}

export interface RunViewChildBlock {
  expand_step: string;
  rows: RunViewGroup[];
}

export interface RunViewSection {
  id: string;
  kind: string;
  title: string;
  column_steps: string[];
  row_scope: RunViewScope;
  groups: RunViewGroup[];
  summary: RunViewCell[];
}

export interface RunView {
  steps: RunViewStep[];
  params: Record<string, unknown>;
  sections: RunViewSection[];
  groups: RunViewGroup[];
  run_summary: RunViewCell[];
  totals: Record<string, number>;
}
