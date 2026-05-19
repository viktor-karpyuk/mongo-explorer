import { useState } from 'react';
import type { ImportFormat, IoResult } from '../../../../shared/io.js';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  onClose: () => void;
  onDone?: () => void;
}

export function ImportDialog({ connectionId, db, collection, onClose, onDone }: Props) {
  const [format, setFormat] = useState<ImportFormat>('ndjson');
  const [path, setPath] = useState('');
  const [ordered, setOrdered] = useState(true);
  const [busy, setBusy] = useState(false);
  const [dryRunResult, setDryRunResult] = useState<IoResult | null>(null);
  const [commitResult, setCommitResult] = useState<IoResult | null>(null);

  async function pick() {
    const p = await window.mex.io.pickOpen();
    if (p) setPath(p);
  }

  async function run(dryRun: boolean) {
    if (!path) return;
    setBusy(true);
    try {
      const r = await window.mex.io.import({
        connectionId,
        db,
        collection,
        format,
        filePath: path,
        dryRun,
        ordered,
      });
      if (dryRun) setDryRunResult(r);
      else {
        setCommitResult(r);
        if (r.ok) onDone?.();
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal__header">
          <h2>Import into {db}.{collection}</h2>
          <button className="btn btn--ghost" onClick={onClose}>
            ✕
          </button>
        </header>
        <div className="modal__body">
          <div className="seg-control">
            {(['ndjson', 'json', 'csv'] as ImportFormat[]).map((f) => (
              <button
                key={f}
                className={`seg${format === f ? ' seg--active' : ''}`}
                onClick={() => setFormat(f)}
              >
                {f.toUpperCase()}
              </button>
            ))}
          </div>

          <div className="field">
            <span>Source file</span>
            <div style={{ display: 'flex', gap: 8 }}>
              <input value={path} readOnly style={{ flex: 1 }} placeholder="(pick a file)" />
              <button className="btn btn--ghost" onClick={() => void pick()}>
                Choose…
              </button>
            </div>
          </div>

          <label className="field--row">
            <input
              type="checkbox"
              checked={ordered}
              onChange={(e) => setOrdered(e.target.checked)}
            />
            <span>Ordered (stop at first error)</span>
          </label>

          {dryRunResult && (
            <div
              className={`alert ${
                dryRunResult.ok ? 'alert--success' : 'alert--error'
              }`}
            >
              {dryRunResult.ok
                ? `Parsed ${dryRunResult.read} documents in ${dryRunResult.durationMs} ms (dry run — nothing inserted).`
                : `Dry run failed: ${dryRunResult.error}`}
            </div>
          )}
          {commitResult && (
            <div
              className={`alert ${
                commitResult.ok ? 'alert--success' : 'alert--error'
              }`}
            >
              {commitResult.ok
                ? `Inserted ${commitResult.inserted} of ${commitResult.read} in ${commitResult.durationMs} ms.`
                : `Failed: ${commitResult.error}`}
            </div>
          )}
        </div>
        <footer className="modal__footer">
          <button
            className="btn btn--ghost"
            onClick={() => void run(true)}
            disabled={!path || busy}
          >
            Dry run
          </button>
          <div className="spacer" />
          <button className="btn btn--ghost" onClick={onClose}>
            Close
          </button>
          <button
            className="btn btn--primary"
            onClick={() => void run(false)}
            disabled={!path || busy}
          >
            {busy ? 'Importing…' : 'Import'}
          </button>
        </footer>
      </div>
    </div>
  );
}
