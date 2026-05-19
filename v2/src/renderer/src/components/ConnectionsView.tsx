import { useEffect, useState } from 'react';
import { useConnectionsStore } from '../store/connections';
import { ConnectionForm } from './ConnectionForm';
import { StatePill } from './StatePill';
import { formatUriPreview } from '../utils/uri';
import type { ConnectionRecord } from '../../../shared/connection.js';

export function ConnectionsView() {
  const list = useConnectionsStore((s) => s.list);
  const states = useConnectionsStore((s) => s.states);
  const load = useConnectionsStore((s) => s.load);
  const create = useConnectionsStore((s) => s.create);
  const update = useConnectionsStore((s) => s.update);
  const remove = useConnectionsStore((s) => s.remove);
  const duplicate = useConnectionsStore((s) => s.duplicate);
  const open = useConnectionsStore((s) => s.open);
  const close = useConnectionsStore((s) => s.close);

  const [editing, setEditing] = useState<ConnectionRecord | null | undefined>(
    undefined,
  );
  const [previews, setPreviews] = useState<Record<string, string>>({});

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    let cancelled = false;
    async function fetchPreviews() {
      const next: Record<string, string> = {};
      for (const c of list) {
        const record = await window.mex.connections.get(c.id);
        if (record) next[c.id] = formatUriPreview(record.uri);
      }
      if (!cancelled) setPreviews(next);
    }
    void fetchPreviews();
    return () => {
      cancelled = true;
    };
  }, [list]);

  async function openForCreate() {
    setEditing(null);
  }

  async function openForEdit(id: string) {
    const record = await window.mex.connections.get(id);
    setEditing(record);
  }

  async function handleSave(input: Parameters<typeof create>[0]) {
    if (editing) {
      await update(editing.id, input);
    } else {
      await create(input);
    }
    setEditing(undefined);
  }

  async function handleDelete(id: string, name: string) {
    if (!confirm(`Delete connection "${name}"? This cannot be undone.`)) return;
    await remove(id);
  }

  return (
    <div className="view">
      <header className="view__header">
        <h1>Connections</h1>
        <button className="btn btn--primary" onClick={openForCreate}>
          + New connection
        </button>
      </header>

      {list.length === 0 ? (
        <div className="empty">
          <h2>No connections yet</h2>
          <p>
            Add your first MongoDB connection to get started. URIs and credentials
            are encrypted with your OS keychain.
          </p>
          <button className="btn btn--primary" onClick={openForCreate}>
            + Add connection
          </button>
        </div>
      ) : (
        <div className="card-grid">
          {list.map((c) => {
            const state = states[c.id] ?? { kind: 'disconnected' as const };
            const busy = state.kind === 'connecting';
            const connected = state.kind === 'connected';
            return (
              <article key={c.id} className="card">
                <header className="card__header">
                  <div>
                    <h3>{c.name}</h3>
                    <p className="muted">
                      {previews[c.id] ?? '…'}
                      {c.hasSsh && <span className="badge">SSH</span>}
                    </p>
                  </div>
                  <StatePill state={state} />
                </header>

                {state.kind === 'error' && (
                  <p className="card__error">{state.message}</p>
                )}

                <footer className="card__actions">
                  {connected ? (
                    <button
                      className="btn btn--ghost"
                      onClick={() => close(c.id)}
                    >
                      Disconnect
                    </button>
                  ) : (
                    <button
                      className="btn btn--primary"
                      onClick={() => open(c.id)}
                      disabled={busy}
                    >
                      {busy ? 'Connecting…' : 'Connect'}
                    </button>
                  )}
                  <div className="spacer" />
                  <button className="btn btn--ghost" onClick={() => openForEdit(c.id)}>
                    Edit
                  </button>
                  <button className="btn btn--ghost" onClick={() => duplicate(c.id)}>
                    Duplicate
                  </button>
                  <button
                    className="btn btn--ghost btn--danger"
                    onClick={() => handleDelete(c.id, c.name)}
                  >
                    Delete
                  </button>
                </footer>
              </article>
            );
          })}
        </div>
      )}

      {editing !== undefined && (
        <ConnectionForm
          initial={editing}
          onCancel={() => setEditing(undefined)}
          onSave={handleSave}
        />
      )}
    </div>
  );
}
