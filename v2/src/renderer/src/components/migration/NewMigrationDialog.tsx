import { useEffect, useState } from 'react';
import { useConnectionsStore } from '../../store/connections';
import type {
  MigrationNamespace,
  MigrationSpec,
  PreflightResult,
} from '../../../../shared/migration.js';
import type { DatabaseInfo, CollectionInfo } from '../../../../shared/namespace.js';

interface Props {
  onClose: () => void;
  onCreate: (spec: MigrationSpec) => Promise<void>;
}

export function NewMigrationDialog({ onClose, onCreate }: Props) {
  const list = useConnectionsStore((s) => s.list);
  const states = useConnectionsStore((s) => s.states);

  const connected = list.filter((c) => states[c.id]?.kind === 'connected');

  const [sourceId, setSourceId] = useState(connected[0]?.id ?? '');
  const [targetId, setTargetId] = useState(connected[1]?.id ?? connected[0]?.id ?? '');
  const [dbs, setDbs] = useState<DatabaseInfo[]>([]);
  const [colls, setColls] = useState<Record<string, CollectionInfo[]>>({});
  const [selectedDbs, setSelectedDbs] = useState<Set<string>>(new Set());
  const [selectedColls, setSelectedColls] = useState<Set<string>>(new Set());
  const [preflight, setPreflight] = useState<PreflightResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!sourceId) return;
    void window.mex.ns.listDatabases(sourceId).then(setDbs).catch(() => setDbs([]));
  }, [sourceId]);

  async function toggleDb(name: string) {
    const next = new Set(selectedDbs);
    if (next.has(name)) {
      next.delete(name);
      const nextColls = new Set(selectedColls);
      for (const k of [...nextColls]) if (k.startsWith(`${name}.`)) nextColls.delete(k);
      setSelectedColls(nextColls);
    } else {
      next.add(name);
      if (!colls[name]) {
        const list = await window.mex.ns.listCollections(sourceId, name);
        setColls((c) => ({ ...c, [name]: list }));
        for (const co of list) {
          setSelectedColls((s) => new Set(s).add(`${name}.${co.name}`));
        }
      } else {
        for (const co of colls[name] ?? []) {
          setSelectedColls((s) => new Set(s).add(`${name}.${co.name}`));
        }
      }
    }
    setSelectedDbs(next);
  }

  function toggleColl(db: string, coll: string) {
    const key = `${db}.${coll}`;
    setSelectedColls((s) => {
      const next = new Set(s);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  function selectedNamespaces(): MigrationNamespace[] {
    return [...selectedColls].map((k) => {
      const idx = k.indexOf('.');
      return { db: k.slice(0, idx), collection: k.slice(idx + 1) };
    });
  }

  async function runPreflight() {
    setBusy(true);
    setError(null);
    setPreflight(null);
    try {
      const r = await window.mex.migrate.preflight({
        sourceConnectionId: sourceId,
        targetConnectionId: targetId,
        namespaces: selectedNamespaces(),
      });
      setPreflight(r);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function createJob() {
    setBusy(true);
    setError(null);
    try {
      await onCreate({
        sourceConnectionId: sourceId,
        targetConnectionId: targetId,
        namespaces: selectedNamespaces(),
        ensureCollections: true,
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const canPreflight = sourceId && targetId && selectedColls.size > 0;
  const canCreate = canPreflight && preflight?.ok;

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal modal--wide" onClick={(e) => e.stopPropagation()}>
        <header className="modal__header">
          <h2>New migration</h2>
          <button className="btn btn--ghost" onClick={onClose}>
            ✕
          </button>
        </header>
        <div className="modal__body">
          <div className="migration-form">
            <label className="field">
              <span>Source connection</span>
              <select value={sourceId} onChange={(e) => setSourceId(e.target.value)}>
                {connected.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span>Target connection</span>
              <select value={targetId} onChange={(e) => setTargetId(e.target.value)}>
                {connected.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.name}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <p className="muted">Pick the databases and collections to copy.</p>

          <div className="ns-picker">
            {dbs.map((db) => {
              const isOpen = selectedDbs.has(db.name);
              return (
                <div key={db.name} className="ns-picker__db">
                  <label className="field--row">
                    <input
                      type="checkbox"
                      checked={isOpen}
                      onChange={() => void toggleDb(db.name)}
                    />
                    <span>{db.name}</span>
                  </label>
                  {isOpen && (
                    <div className="ns-picker__colls">
                      {(colls[db.name] ?? []).map((c) => (
                        <label key={c.name} className="field--row">
                          <input
                            type="checkbox"
                            checked={selectedColls.has(`${db.name}.${c.name}`)}
                            onChange={() => toggleColl(db.name, c.name)}
                          />
                          <span>{c.name}</span>
                        </label>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          {preflight && (
            <div className={`alert ${preflight.ok ? 'alert--success' : 'alert--error'}`}>
              {preflight.ok ? 'Preflight passed.' : 'Preflight has failures.'}
              <ul style={{ margin: '6px 0 0', paddingLeft: 18 }}>
                {preflight.checks.map((c, i) => (
                  <li key={i}>
                    {c.ok ? '✓' : '✗'} {c.name}
                    {c.detail && ` — ${c.detail}`}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {error && <div className="alert alert--error">{error}</div>}
        </div>
        <footer className="modal__footer">
          <button
            className="btn btn--ghost"
            onClick={() => void runPreflight()}
            disabled={!canPreflight || busy}
          >
            {busy ? '…' : 'Preflight'}
          </button>
          <div className="spacer" />
          <button className="btn btn--ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            className="btn btn--primary"
            onClick={() => void createJob()}
            disabled={!canCreate || busy}
          >
            Create migration
          </button>
        </footer>
      </div>
    </div>
  );
}
