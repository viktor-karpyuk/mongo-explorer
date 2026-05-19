import { useState } from 'react';
import type { FindResponse } from '../../../../shared/query.js';
import { ResultTable } from './ResultTable';
import { ResultTree } from './ResultTree';
import { ResultJson } from './ResultJson';
import { ResultError } from './ResultError';

type Tab = 'table' | 'tree' | 'json' | 'error';

interface Props {
  result: FindResponse | null;
  running: boolean;
  skip: number;
  limit: number;
  onPrev: () => void;
  onNext: () => void;
  onEditRow?: (doc: string) => void;
  onDeleteRow?: (doc: string) => void;
}

export function ResultsPane({
  result,
  running,
  skip,
  limit,
  onPrev,
  onNext,
  onEditRow,
  onDeleteRow,
}: Props) {
  const [tab, setTab] = useState<Tab>('table');
  const hasError = result && result.ok === false;
  const hasRows = result && result.ok === true;

  // Auto-switch to Error tab on failure.
  if (hasError && tab !== 'error') {
    setTab('error');
  }

  return (
    <div className="results">
      <div className="results__tabs">
        <button
          className={`tab${tab === 'table' ? ' tab--active' : ''}`}
          onClick={() => setTab('table')}
        >
          Table
        </button>
        <button
          className={`tab${tab === 'tree' ? ' tab--active' : ''}`}
          onClick={() => setTab('tree')}
        >
          Tree
        </button>
        <button
          className={`tab${tab === 'json' ? ' tab--active' : ''}`}
          onClick={() => setTab('json')}
        >
          JSON
        </button>
        {hasError && (
          <button
            className={`tab tab--error${tab === 'error' ? ' tab--active' : ''}`}
            onClick={() => setTab('error')}
          >
            Error
          </button>
        )}
        <div className="spacer" />

        {hasRows && (
          <span className="muted">
            {result.rows.length} row{result.rows.length === 1 ? '' : 's'}
            {' · '}
            {result.durationMs} ms
            {result.hasMore && ' · more available'}
          </span>
        )}
      </div>

      <div className="results__body">
        {running && !result && <div className="muted">Running…</div>}
        {!running && !result && <div className="muted">Run a query to see results.</div>}
        {hasError && tab === 'error' && <ResultError message={result.error} />}
        {hasRows && tab === 'table' && <ResultTable rows={result.rows} />}
        {hasRows && tab === 'tree' && (
          <ResultTree rows={result.rows} onEdit={onEditRow} onDelete={onDeleteRow} />
        )}
        {hasRows && tab === 'json' && <ResultJson rows={result.rows} />}
      </div>

      <div className="results__pager">
        <button
          className="btn btn--ghost"
          onClick={onPrev}
          disabled={skip === 0 || running}
        >
          ← Prev
        </button>
        <span className="muted">
          rows {skip + 1}–{skip + (hasRows ? result.rows.length : 0)}
        </span>
        <button
          className="btn btn--ghost"
          onClick={onNext}
          disabled={!hasRows || !result.hasMore || running}
        >
          Next →
        </button>
        <div className="spacer" />
        <span className="muted">page size {limit}</span>
      </div>
    </div>
  );
}
