import { Fragment, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchObject } from '../api';
import {
  childColumnSteps,
  childScope,
  formatPreview,
  ownCellSteps,
  ownExpandSteps,
  stepsByName,
} from '../runViewLayout';
import type {
  RunView,
  RunViewCell,
  RunViewChildBlock,
  RunViewGroup,
  RunViewScope,
  RunViewSection,
  RunViewStep,
} from '../runViewTypes';
import { coordStatusClass } from '../format';

interface Props {
  view: RunView;
}

function headerSteps(
  steps: RunViewStep[],
  ownScope: RunViewScope,
  columnNames?: string[],
): RunViewStep[] {
  if (columnNames && columnNames.length > 0) {
    const byName = stepsByName(steps);
    return columnNames.map((n) => byName.get(n)).filter((s): s is RunViewStep => !!s);
  }
  return ownCellSteps(steps, ownScope);
}

function hasNested(steps: RunViewStep[], ownScope: RunViewScope, group: RunViewGroup): boolean {
  return ownExpandSteps(steps, ownScope).length > 0 || group.children.length > 0 || group.summary.length > 0;
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

function HeaderRow({
  steps,
  ownScope,
  columnNames,
}: {
  steps: RunViewStep[];
  ownScope: RunViewScope;
  columnNames?: string[];
}) {
  const cols = headerSteps(steps, ownScope, columnNames);
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
  columnNames,
}: {
  steps: RunViewStep[];
  ownScope: RunViewScope;
  group: RunViewGroup;
  columnNames?: string[];
}) {
  const cols = headerSteps(steps, ownScope, columnNames);
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
  summary: RunViewCell[];
  label?: string;
}) {
  if (summary.length === 0) return null;
  return (
    <div className="rv-summary-strip">
      <span className="rv-summary-label">{label ?? 'Σ summary'}</span>
      {summary.map((cell) => (
        <span
          key={`${cell.step_name}:${cell.coordinate}`}
          className={`rv-summary-chip status-${cell.status}`}
          title={cell.error_message ?? undefined}
        >
          <span className="rv-summary-chip-name">
            {cell.step_name}
            {cell.coordinate && cell.coordinate !== '@all' ? (
              <span className="rv-summary-coord">[{cell.coordinate}]</span>
            ) : null}
          </span>
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
        const childCols = childColumnSteps(steps, expand);
        const childColNames = childCols.map((c) => c.name);
        return (
          <div className="rv-child-block" key={expand.name}>
            <div className="rv-child-block-label">{expand.name}</div>
            <table className="rv-child-table">
              <HeaderRow steps={steps} ownScope={childOwn} columnNames={childColNames} />
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
                    <DataRow
                      steps={steps}
                      ownScope={childOwn}
                      group={row}
                      columnNames={childColNames}
                    />
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

function SectionTable({
  steps,
  section,
}: {
  steps: RunViewStep[];
  section: RunViewSection;
}) {
  const cols = headerSteps(steps, section.row_scope, section.column_steps);
  return (
    <div className={`rv-section kind-${section.kind}`}>
      <div className="rv-section-header">
        <h3 className="rv-section-title">{section.title}</h3>
        <span className="rv-section-kind">{section.kind}</span>
      </div>
      <div className="rv-table-scroll">
        <table className="rv-parent-table">
          <HeaderRow
            steps={steps}
            ownScope={section.row_scope}
            columnNames={section.column_steps}
          />
          <tbody>
            {section.groups.length === 0 && (
              <tr>
                <td className="rv-lane-col" />
                <td className="rv-cell rv-empty-note" colSpan={Math.max(cols.length, 1)}>
                  No lanes in this section.
                </td>
              </tr>
            )}
            {section.groups.map((g) => (
              <Fragment key={g.coordinate}>
                <DataRow
                  steps={steps}
                  ownScope={section.row_scope}
                  group={g}
                  columnNames={section.column_steps}
                />
                {hasNested(steps, section.row_scope, g) && (
                  <tr className="rv-detail-row">
                    <td className="rv-lane-col" />
                    <td colSpan={Math.max(cols.length, 1)}>
                      <Detail steps={steps} ownScope={section.row_scope} group={g} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
      <SummaryStrip summary={section.summary} label={`Σ ${section.title}`} />
    </div>
  );
}

export default function GroupedRunTable({ view }: Props) {
  const sections =
    view.sections?.length > 0
      ? view.sections
      : view.groups?.length
        ? [
            {
              id: 'main',
              kind: 'branch',
              title: 'lanes',
              column_steps: [],
              row_scope: { kind: 'branch', expand_step: null },
              groups: view.groups,
              summary: [],
            } satisfies RunViewSection,
          ]
        : [];

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

      {sections.map((section) => (
        <SectionTable key={section.id} steps={view.steps} section={section} />
      ))}

      <SummaryStrip summary={view.run_summary ?? []} label="Σ run summary" />
    </div>
  );
}
