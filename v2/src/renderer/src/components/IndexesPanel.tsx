import { useEffect, useState } from 'react';
import type {
  CreateIndexInput,
  IndexInfo,
  IndexKey,
  IndexStatRow,
} from '../../../shared/schema.js';
import { formatCount } from '../utils/format';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
}

export function IndexesPanel({ connectionId, db, collection }: Props) {
  const [indexes, setIndexes] = useState<IndexInfo[]>([]);
  const [stats, setStats] = useState<Record<string, IndexStatRow>>({});
  const [error, setError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  async function load() {
    setError(null);
    try {
      const [list, statsRows] = await Promise.all([
        window.mex.schema.listIndexes(connectionId, db, collection),
        window.mex.schema.indexStats(connectionId, db, collection).catch(() => [] as IndexStatRow[]),
      ]);
      setIndexes(list);
      setStats(Object.fromEntries(statsRows.map((r) => [r.name, r])));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectionId, db, collection]);

  async function drop(name: string) {
    if (name === '_id_') return;
    const typed = prompt(`Type the index name to confirm drop: ${name}`);
    if (typed !== name) return;
    await window.mex.schema.dropIndex(connectionId, db, collection, name);
    await load();
  }

  return (
    <div className="view">
      <header className="view__header">
        <h1>Indexes · {db}.{collection}</h1>
        <button className="btn btn--primary" onClick={() => setCreating(true)}>
          + Create index
        </button>
      </header>

      {error && <div className="alert alert--error">{error}</div>}

      <table className="idx-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Keys</th>
            <th>Flags</th>
            <th>Ops</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {indexes.map((idx) => (
            <tr key={idx.name}>
              <td className="idx-name">{idx.name}</td>
              <td className="idx-keys">
                {idx.keys.map((k) => (
                  <span key={k.field} className="idx-key">
                    {k.field}: {String(k.direction)}
                  </span>
                ))}
              </td>
              <td className="muted">
                {[
                  idx.unique && 'unique',
                  idx.sparse && 'sparse',
                  idx.background && 'background',
                  idx.ttlSeconds != null && `ttl ${idx.ttlSeconds}s`,
                  idx.partialFilter && 'partial',
                ]
                  .filter(Boolean)
                  .join(' · ') || '—'}
              </td>
              <td>{stats[idx.name] ? formatCount(stats[idx.name]!.ops) : '—'}</td>
              <td>
                <button
                  className="btn btn--ghost btn--xs btn--danger"
                  onClick={() => void drop(idx.name)}
                  disabled={idx.name === '_id_'}
                >
                  Drop
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {creating && (
        <CreateIndexDialog
          onCancel={() => setCreating(false)}
          onCreate={async (input) => {
            await window.mex.schema.createIndex(connectionId, db, collection, input);
            setCreating(false);
            await load();
          }}
        />
      )}
    </div>
  );
}

function CreateIndexDialog({
  onCancel,
  onCreate,
}: {
  onCancel: () => void;
  onCreate: (input: CreateIndexInput) => Promise<void>;
}) {
  const [keys, setKeys] = useState<IndexKey[]>([{ field: '', direction: 1 }]);
  const [name, setName] = useState('');
  const [unique, setUnique] = useState(false);
  const [sparse, setSparse] = useState(false);
  const [background, setBackground] = useState(true);
  const [ttl, setTtl] = useState('');
  const [partial, setPartial] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function update(i: number, patch: Partial<IndexKey>) {
    setKeys((prev) => prev.map((k, idx) => (idx === i ? { ...k, ...patch } : k)));
  }

  async function submit() {
    setBusy(true);
    setError(null);
    try {
      await onCreate({
        name: name.trim() || undefined,
        keys: keys.filter((k) => k.field.trim() !== ''),
        unique,
        sparse,
        background,
        ttlSeconds: ttl ? Number(ttl) : undefined,
        partialFilter: partial.trim() || undefined,
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal-backdrop" onClick={onCancel}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal__header">
          <h2>Create index</h2>
          <button className="btn btn--ghost" onClick={onCancel}>
            ✕
          </button>
        </header>
        <div className="modal__body">
          <div className="field">
            <span>Keys</span>
            {keys.map((k, i) => (
              <div key={i} className="idx-key-row">
                <input
                  placeholder="field"
                  value={k.field}
                  onChange={(e) => update(i, { field: e.target.value })}
                />
                <select
                  value={String(k.direction)}
                  onChange={(e) =>
                    update(i, { direction: e.target.value as IndexKey['direction'] })
                  }
                >
                  <option value="1">1 (asc)</option>
                  <option value="-1">-1 (desc)</option>
                  <option value="2dsphere">2dsphere</option>
                  <option value="2d">2d</option>
                  <option value="text">text</option>
                  <option value="hashed">hashed</option>
                </select>
                <button
                  className="btn btn--ghost btn--xs"
                  onClick={() => setKeys((p) => p.filter((_, idx) => idx !== i))}
                >
                  ✕
                </button>
              </div>
            ))}
            <button
              className="btn btn--ghost btn--xs"
              onClick={() => setKeys((p) => [...p, { field: '', direction: 1 }])}
            >
              + Add key
            </button>
          </div>

          <label className="field">
            <span>Name (optional)</span>
            <input value={name} onChange={(e) => setName(e.target.value)} />
          </label>

          <div className="idx-flags">
            <label className="field--row">
              <input type="checkbox" checked={unique} onChange={(e) => setUnique(e.target.checked)} />
              <span>Unique</span>
            </label>
            <label className="field--row">
              <input type="checkbox" checked={sparse} onChange={(e) => setSparse(e.target.checked)} />
              <span>Sparse</span>
            </label>
            <label className="field--row">
              <input
                type="checkbox"
                checked={background}
                onChange={(e) => setBackground(e.target.checked)}
              />
              <span>Background</span>
            </label>
          </div>

          <label className="field">
            <span>TTL seconds (optional)</span>
            <input
              type="number"
              min={0}
              value={ttl}
              onChange={(e) => setTtl(e.target.value)}
              placeholder="e.g. 3600"
            />
          </label>

          <label className="field">
            <span>Partial filter (JSON, optional)</span>
            <textarea
              value={partial}
              onChange={(e) => setPartial(e.target.value)}
              rows={2}
              placeholder='{ "status": "active" }'
            />
          </label>

          {error && <div className="test-result test-result--fail">{error}</div>}
        </div>
        <footer className="modal__footer">
          <div className="spacer" />
          <button className="btn btn--ghost" onClick={onCancel}>
            Cancel
          </button>
          <button
            className="btn btn--primary"
            onClick={() => void submit()}
            disabled={busy || keys.every((k) => !k.field.trim())}
          >
            {busy ? 'Creating…' : 'Create'}
          </button>
        </footer>
      </div>
    </div>
  );
}
