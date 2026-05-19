import { useEffect, useRef, useState } from 'react';
import type { DumpRestoreEvent, DumpRestoreRequest } from '../../../../shared/io.js';

interface Props {
  connectionId: string;
  defaultDb?: string;
  defaultCollection?: string;
  onClose: () => void;
}

export function DumpRestoreDialog({
  connectionId,
  defaultDb,
  defaultCollection,
  onClose,
}: Props) {
  const [kind, setKind] = useState<'dump' | 'restore'>('dump');
  const [db, setDb] = useState(defaultDb ?? '');
  const [coll, setColl] = useState(defaultCollection ?? '');
  const [path, setPath] = useState('');
  const [running, setRunning] = useState(false);
  const [output, setOutput] = useState('');
  const jobId = useRef<string | null>(null);

  useEffect(() => {
    const unsub = window.mex.io.onDumpRestoreEvent((event: DumpRestoreEvent) => {
      if (event.jobId !== jobId.current) return;
      if (event.channel === 'exit') {
        setRunning(false);
        setOutput((s) => s + `\n[process exited with code ${event.exitCode ?? '?'}]\n`);
      } else {
        setOutput((s) => s + event.data);
      }
    });
    return unsub;
  }, []);

  async function pick() {
    const p = await window.mex.io.pickDir();
    if (p) setPath(p);
  }

  async function start() {
    if (!path) return;
    setOutput('');
    setRunning(true);
    const req: DumpRestoreRequest = {
      kind,
      connectionId,
      db: db.trim() || undefined,
      collection: coll.trim() || undefined,
      path,
    };
    jobId.current = await window.mex.io.startDumpRestore(req);
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal__header">
          <h2>mongodump / mongorestore</h2>
          <button className="btn btn--ghost" onClick={onClose}>
            ✕
          </button>
        </header>
        <div className="modal__body">
          <div className="seg-control">
            <button
              className={`seg${kind === 'dump' ? ' seg--active' : ''}`}
              onClick={() => setKind('dump')}
            >
              mongodump
            </button>
            <button
              className={`seg${kind === 'restore' ? ' seg--active' : ''}`}
              onClick={() => setKind('restore')}
            >
              mongorestore
            </button>
          </div>

          <label className="field">
            <span>Database (optional)</span>
            <input value={db} onChange={(e) => setDb(e.target.value)} />
          </label>
          <label className="field">
            <span>Collection (optional)</span>
            <input value={coll} onChange={(e) => setColl(e.target.value)} />
          </label>
          <div className="field">
            <span>{kind === 'dump' ? 'Output directory' : 'Source directory'}</span>
            <div style={{ display: 'flex', gap: 8 }}>
              <input value={path} readOnly style={{ flex: 1 }} placeholder="(pick a folder)" />
              <button className="btn btn--ghost" onClick={() => void pick()}>
                Choose…
              </button>
            </div>
          </div>

          <pre className="subproc-output">{output || '(no output yet)'}</pre>
        </div>
        <footer className="modal__footer">
          <div className="spacer" />
          <button className="btn btn--ghost" onClick={onClose}>
            Close
          </button>
          <button
            className="btn btn--primary"
            onClick={() => void start()}
            disabled={!path || running}
          >
            {running ? 'Running…' : `Run ${kind}`}
          </button>
        </footer>
      </div>
    </div>
  );
}
