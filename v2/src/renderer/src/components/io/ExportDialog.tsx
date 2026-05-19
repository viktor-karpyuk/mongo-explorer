import { useState } from 'react';
import type { ExportFormat, IoResult } from '../../../../shared/io.js';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  initialFilter?: string;
  initialProjection?: string;
  initialSort?: string;
  onClose: () => void;
}

export function ExportDialog({
  connectionId,
  db,
  collection,
  initialFilter,
  initialProjection,
  initialSort,
  onClose,
}: Props) {
  const [format, setFormat] = useState<ExportFormat>('ndjson');
  const [filter, setFilter] = useState(initialFilter ?? '{}');
  const [projection, setProjection] = useState(initialProjection ?? '');
  const [sort, setSort] = useState(initialSort ?? '');
  const [limit, setLimit] = useState(0);
  const [path, setPath] = useState('');
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<IoResult | null>(null);

  async function pick() {
    const ext = format === 'csv' ? 'csv' : format === 'json' ? 'json' : 'jsonl';
    const p = await window.mex.io.pickSave(`${collection}.${ext}`);
    if (p) setPath(p);
  }

  async function run() {
    if (!path) return;
    setBusy(true);
    setResult(null);
    try {
      const r = await window.mex.io.export({
        connectionId,
        db,
        collection,
        format,
        filter,
        projection,
        sort,
        limit: limit > 0 ? limit : undefined,
        filePath: path,
      });
      setResult(r);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal__header">
          <h2>Export · {db}.{collection}</h2>
          <button className="btn btn--ghost" onClick={onClose}>
            ✕
          </button>
        </header>
        <div className="modal__body">
          <div className="seg-control">
            {(['ndjson', 'json', 'csv'] as ExportFormat[]).map((f) => (
              <button
                key={f}
                className={`seg${format === f ? ' seg--active' : ''}`}
                onClick={() => setFormat(f)}
              >
                {f.toUpperCase()}
              </button>
            ))}
          </div>

          <label className="field">
            <span>Filter</span>
            <textarea
              className="json-editor"
              rows={3}
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              spellCheck={false}
            />
          </label>
          <label className="field">
            <span>Projection (optional)</span>
            <textarea
              className="json-editor"
              rows={2}
              value={projection}
              onChange={(e) => setProjection(e.target.value)}
              spellCheck={false}
            />
          </label>
          <label className="field">
            <span>Sort (optional)</span>
            <textarea
              className="json-editor"
              rows={2}
              value={sort}
              onChange={(e) => setSort(e.target.value)}
              spellCheck={false}
            />
          </label>
          <label className="field">
            <span>Limit (0 = all)</span>
            <input
              type="number"
              min={0}
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value) || 0)}
            />
          </label>
          <div className="field">
            <span>Target file</span>
            <div style={{ display: 'flex', gap: 8 }}>
              <input value={path} readOnly style={{ flex: 1 }} placeholder="(pick a file)" />
              <button className="btn btn--ghost" onClick={() => void pick()}>
                Choose…
              </button>
            </div>
          </div>

          {result?.ok && (
            <div className="alert alert--success">
              Wrote {result.written} document{result.written === 1 ? '' : 's'} in{' '}
              {result.durationMs} ms.
            </div>
          )}
          {result && !result.ok && (
            <div className="alert alert--error">{result.error}</div>
          )}
        </div>
        <footer className="modal__footer">
          <div className="spacer" />
          <button className="btn btn--ghost" onClick={onClose}>
            Close
          </button>
          <button
            className="btn btn--primary"
            onClick={() => void run()}
            disabled={!path || busy}
          >
            {busy ? 'Exporting…' : 'Export'}
          </button>
        </footer>
      </div>
    </div>
  );
}
