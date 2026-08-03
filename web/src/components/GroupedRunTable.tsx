import { Fragment, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchObject } from '../api';
import {
  PARENT_SCOPE,
  childScope,
  formatPreview,
  ownCellSteps,
  ownExpandSteps,
} from '../runViewLayout';
import type {
  RunView,
  RunViewCell,
  RunViewChildBlock,
  RunViewGroup,
  RunViewScope,
  RunViewStep,
} from '../runViewTypes';
import { coordStatusClass } from '../format';

interface Props {
  view: RunView;
}

function headerSteps(steps: RunViewStep[], ownScope: RunViewScope): RunViewStep[] {
  return ownCellSteps(steps, ownScope);
}

function hasNested(steps: RunViewStep[], ownScope: RunViewScope, group: RunViewGroup): boolean {
  return (
    ownExpandSteps(steps, ownScope).length > 0 ||
    Object.keys(group.summary).length > 0 ||
    group.children.length > 0
  );
}

function CellTd({
  cell,
  step,
}: {
  cell: RunViewCell | undefined;
  step: RunViewStep;
}) {
  const [expanded, setExpanded] = useState(false);
  const [full, setFull] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);

  const status = cell?.status ?? 'pending';
  const failed = status === 'failed';
  const cls = `rv-cell status-${status}${failed ? ' rv-cell-failed' : ''}`;

  async function openFull() {
    if (!cell?.output_address) return;
    if (full !== null) {
      setExpanded((e) => !e);
      return;
    }
    setLoading(true);
    try {
      const obj = await fetchObject(cell.output_address);
      setFull(obj.preview_json ?? obj.preview_text ?? null);
      setExpanded(true);
    } catch (e) {
      setFull(String(e));
      setExpanded(true);
    } finally {
      setLoading(false);
    }
  }

  if (!cell) {
    return <td className="rv-cell rv-cell-blank" title={`${step.name} (no lane)`} />;
  }

  const preview = formatPreview(cell.preview);
  const title = cell.error_message
    ? cell.error_message
    : preview.length > 80
      ? preview
      : undefined;

  return (
    <td className={cls} title={title}>
      <div className="rv-cell-inner">
        <span className={`badge badge-${coordStatusClass(status)} rv-status-badge`}>
          {status}
        </span>
        {failed && cell.error_message ? (
          <span className="rv-error-text">{cell.error_message}</span>
        ) : (
          <button
            type="button"
            className="rv-preview-btn"
            onClick={openFull}
            disabled={!cell.output_address || loading}
          >
            {loading ? '…' : preview || '—'}
          </button>
        )}
        {cell.output_address && (
          <Link
            className="rv-addr-link"
            to={`/objects/${cell.output_address}`}
            title={cell.output_address}
          >
            ↗
          </Link>
        )}
      </div>
      {expanded && (
        <pre className="rv-full-preview">{formatPreview(full)}</pre>
      )}
    </td>
  );
}

function HeaderRow({ steps, ownScope }: { steps: RunViewStep[]; ownScope: RunViewScope }) {
  const cols = headerSteps(steps, ownScope);
  return (
    <thead>
      <tr>
        <th className="rv-lane-col">lane</th>
        {cols.map((s) => (
          <th key={s.name} className="rv-step-header" title={`${s.shape} · ${s.scope.kind}`}>
            <span className="rv-step-name">{s.name}</span>
            <span className="rv-step-shape">{s.shape}</span>
          </th>
        ))}
      </tr>
    </thead>
  );
}

function DataRow({
  steps,
  ownScope,
  group,
}: {
  steps: RunViewStep[];
  ownScope: RunViewScope;
  group: RunViewGroup;
}) {
  const cols = headerSteps(steps, ownScope);
  return (
    <tr className="rv-band-row">
      <td className="rv-lane-col">
        <code>{group.coordinate}</code>
      </td>
      {cols.map((s) => (
        <CellTd key={s.name} cell={group.cells[s.name]} step={s} />
      ))}
    </tr>
  );
}

function SummaryStrip({
  summary,
  label,
}: {
  summary: Record<string, RunViewCell>;
  label?: string;
}) {
  const entries = Object.entries(summary);
  if (entries.length === 0) return null;
  return (
    <div className="rv-summary-strip">
      <span className="rv-summary-label">{label ?? 'Σ summary'}</span>
      {entries.map(([name, cell]) => (
        <span
          key={name}
          className={`rv-summary-chip status-${cell.status}`}
          title={cell.error_message ?? undefined}
        >
          <span className="rv-summary-chip-name">{name}</span>
          <span className={`badge badge-${coordStatusClass(cell.status)}`}>{cell.status}</span>
          <span className="rv-summary-chip-value">{formatPreview(cell.preview)}</span>
          {cell.output_address && (
            <Link to={`/objects/${cell.output_address}`} className="rv-addr-link">
              ↗
            </Link>
          )}
        </span>
      ))}
    </div>
  );
}

function Detail({
  steps,
  ownScope,
  group,
}: {
  steps: RunViewStep[];
  ownScope: RunViewScope;
  group: RunViewGroup;
}) {
  const expandSteps = ownExpandSteps(steps, ownScope);
  return (
    <div className="rv-detail">
      {expandSteps.map((expand) => {
        const block: RunViewChildBlock | undefined = group.children.find(
          (c) => c.expand_step === expand.name,
        );
        const rows = block?.rows ?? [];
        const childOwn = childScope(expand.name);
        const childCols = headerSteps(steps, childOwn);
        return (
          <div className="rv-child-block" key={expand.name}>
            <div className="rv-child-block-label">{expand.name}</div>
            <table className="rv-child-table">
              <HeaderRow steps={steps} ownScope={childOwn} />
              <tbody>
                {rows.length === 0 && (
                  <tr>
                    <td className="rv-lane-col" />
                    <td className="rv-cell rv-empty-note" colSpan={Math.max(childCols.length, 1)}>
                      (no child lanes)
                    </td>
                  </tr>
                )}
                {rows.map((row) => (
                  <Fragment key={row.coordinate}>
                    <DataRow steps={steps} ownScope={childOwn} group={row} />
                    {hasNested(steps, childOwn, row) && (
                      <tr className="rv-detail-row">
                        <td className="rv-lane-col" />
                        <td colSpan={Math.max(childCols.length, 1)}>
                          <Detail steps={steps} ownScope={childOwn} group={row} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                ))}
              </tbody>
            </table>
          </div>
        );
      })}
      <SummaryStrip summary={group.summary} />
    </div>
  );
}

function ParamsBand({ params }: { params: Record<string, unknown> }) {
  const entries = Object.entries(params);
  if (entries.length === 0) {
    return (
      <div className="rv-params-band">
        <span className="rv-params-label">params</span>
        <span className="rv-params-empty">(none)</span>
      </div>
    );
  }
  return (
    <div className="rv-params-band">
      <span className="rv-params-label">params</span>
      {entries.map(([k, v]) => (
        <span key={k} className="rv-param-chip">
          <span className="rv-param-key">{k}</span>
          <span className="rv-param-val">{formatPreview(v)}</span>
        </span>
      ))}
    </div>
  );
}

export default function GroupedRunTable({ view }: Props) {
  const parentCols = headerSteps(view.steps, PARENT_SCOPE);
  return (
    <div className="rv-root">
      <ParamsBand params={view.params} />

      <div className="rv-totals">
        {(['created', 'reused', 'failed', 'blocked', 'filtered'] as const).map((k) => {
          const n = view.totals[k] ?? 0;
          if (!n && k !== 'created' && k !== 'reused' && k !== 'failed') return null;
          return (
            <span key={k} className={`rv-total-chip status-${k}`}>
              <strong>{n}</strong> {k}
            </span>
          );
        })}
      </div>

      <div className="rv-table-scroll">
        <table className="rv-parent-table">
          <HeaderRow steps={view.steps} ownScope={PARENT_SCOPE} />
          <tbody>
            {view.groups.length === 0 && (
              <tr>
                <td className="rv-lane-col" />
                <td className="rv-cell rv-empty-note" colSpan={Math.max(parentCols.length, 1)}>
                  No lanes in this run.
                </td>
              </tr>
            )}
            {view.groups.map((g) => (
              <Fragment key={g.coordinate}>
                <DataRow steps={view.steps} ownScope={PARENT_SCOPE} group={g} />
                {hasNested(view.steps, PARENT_SCOPE, g) && (
                  <tr className="rv-detail-row">
                    <td className="rv-lane-col" />
                    <td colSpan={Math.max(parentCols.length, 1)}>
                      <Detail steps={view.steps} ownScope={PARENT_SCOPE} group={g} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>

      <SummaryStrip summary={view.run_summary} label="Σ run summary" />
    </div>
  );
}
