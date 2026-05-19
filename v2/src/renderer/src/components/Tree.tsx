import { useMemo, useState } from 'react';
import { useConnectionsStore } from '../store/connections';
import { useNamespacesStore } from '../store/namespaces';
import { useSelectionStore, type Selection } from '../store/selection';
import { ContextMenu, type MenuItem } from './ContextMenu';

interface MenuPos {
  x: number;
  y: number;
  items: MenuItem[];
}

export function Tree() {
  const list = useConnectionsStore((s) => s.list);
  const states = useConnectionsStore((s) => s.states);
  const nsByConn = useNamespacesStore((s) => s.byConnection);
  const loadDatabases = useNamespacesStore((s) => s.loadDatabases);
  const refresh = useNamespacesStore((s) => s.refresh);
  const toggleExpanded = useNamespacesStore((s) => s.toggleExpanded);
  const select = useSelectionStore((s) => s.select);
  const selection = useSelectionStore((s) => s.current);

  const [filter, setFilter] = useState('');
  const [menu, setMenu] = useState<MenuPos | null>(null);

  const filterLower = filter.trim().toLowerCase();
  function matches(...labels: string[]) {
    if (!filterLower) return true;
    return labels.some((l) => l.toLowerCase().includes(filterLower));
  }

  const connected = useMemo(
    () => list.filter((c) => states[c.id]?.kind === 'connected'),
    [list, states],
  );

  function isSelected(s: Selection): boolean {
    if (s.kind !== selection.kind) return false;
    if (s.kind === 'welcome') return true;
    if (s.kind === 'database' && selection.kind === 'database') {
      return s.connectionId === selection.connectionId && s.db === selection.db;
    }
    if (s.kind === 'collection' && selection.kind === 'collection') {
      return (
        s.connectionId === selection.connectionId &&
        s.db === selection.db &&
        s.collection === selection.collection
      );
    }
    return false;
  }

  function onConnContextMenu(e: React.MouseEvent, connectionId: string) {
    e.preventDefault();
    setMenu({
      x: e.clientX,
      y: e.clientY,
      items: [
        {
          label: 'Refresh',
          onClick: () => void refresh(connectionId),
        },
        {
          label: 'Create database…',
          onClick: () => void createDbFlow(connectionId),
        },
      ],
    });
  }

  function onDbContextMenu(e: React.MouseEvent, connectionId: string, db: string) {
    e.preventDefault();
    setMenu({
      x: e.clientX,
      y: e.clientY,
      items: [
        {
          label: 'Refresh collections',
          onClick: () => void useNamespacesStore.getState().loadCollections(connectionId, db),
        },
        {
          label: 'Create collection…',
          onClick: () => void createCollFlow(connectionId, db),
        },
        {
          label: 'Drop database…',
          danger: true,
          onClick: () => void dropDbFlow(connectionId, db),
        },
      ],
    });
  }

  function onCollContextMenu(
    e: React.MouseEvent,
    connectionId: string,
    db: string,
    collection: string,
  ) {
    e.preventDefault();
    setMenu({
      x: e.clientX,
      y: e.clientY,
      items: [
        {
          label: 'Rename collection…',
          onClick: () => void renameCollFlow(connectionId, db, collection),
        },
        {
          label: 'Drop collection…',
          danger: true,
          onClick: () => void dropCollFlow(connectionId, db, collection),
        },
      ],
    });
  }

  async function createDbFlow(connectionId: string) {
    const name = prompt('New database name:');
    if (!name) return;
    await window.mex.ns.createDatabase(connectionId, name);
    await loadDatabases(connectionId);
  }

  async function dropDbFlow(connectionId: string, db: string) {
    if (!confirm(`Drop database "${db}"? This is permanent.`)) return;
    await window.mex.ns.dropDatabase(connectionId, db);
    await loadDatabases(connectionId);
  }

  async function createCollFlow(connectionId: string, db: string) {
    const name = prompt(`New collection in "${db}":`);
    if (!name) return;
    await window.mex.ns.createCollection(connectionId, db, { name });
    await useNamespacesStore.getState().loadCollections(connectionId, db);
  }

  async function dropCollFlow(connectionId: string, db: string, name: string) {
    if (!confirm(`Drop collection "${db}.${name}"?`)) return;
    await window.mex.ns.dropCollection(connectionId, db, name);
    await useNamespacesStore.getState().loadCollections(connectionId, db);
  }

  async function renameCollFlow(connectionId: string, db: string, from: string) {
    const to = prompt(`Rename "${db}.${from}" to:`, from);
    if (!to || to === from) return;
    await window.mex.ns.renameCollection(connectionId, db, from, to);
    await useNamespacesStore.getState().loadCollections(connectionId, db);
  }

  return (
    <div className="tree">
      <div className="tree__filter">
        <input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter namespaces…"
          spellCheck={false}
        />
      </div>

      <div
        className={`tree__row tree__row--welcome${
          selection.kind === 'welcome' ? ' tree__row--active' : ''
        }`}
        onClick={() => select({ kind: 'welcome' })}
      >
        <span>Connections</span>
      </div>
      <div
        className={`tree__row tree__row--welcome${
          selection.kind === 'migrations' ? ' tree__row--active' : ''
        }`}
        onClick={() => select({ kind: 'migrations' })}
      >
        <span>Migrations</span>
      </div>
      <div
        className={`tree__row tree__row--welcome${
          selection.kind === 'settings' ? ' tree__row--active' : ''
        }`}
        onClick={() => select({ kind: 'settings' })}
      >
        <span>Settings</span>
      </div>

      <div className="tree__list">
        {connected.length === 0 && (
          <p className="tree__empty muted">
            Open a connection to browse databases.
          </p>
        )}

        {connected.map((conn) => {
          const cache = nsByConn[conn.id];
          if (!cache && !filter) {
            void loadDatabases(conn.id);
          }
          const dbs = cache?.databases ?? [];

          const isConnSel =
            selection.kind === 'connection' && selection.connectionId === conn.id;
          return (
            <div key={conn.id} className="tree__conn">
              <div
                className={`tree__row tree__row--conn${
                  isConnSel ? ' tree__row--active' : ''
                }`}
                onClick={() => select({ kind: 'connection', connectionId: conn.id })}
                onContextMenu={(e) => onConnContextMenu(e, conn.id)}
              >
                <span className="tree__caret">●</span>
                <span className="tree__label">{conn.name}</span>
              </div>

              {dbs.filter((d) => matches(d.name)).map((db) => {
                const expanded = cache?.expanded.has(db.name) ?? false;
                const colls = cache?.collections[db.name] ?? [];
                const collsLoading = cache?.collectionsLoading[db.name] ?? false;
                const isDbSel = isSelected({
                  kind: 'database',
                  connectionId: conn.id,
                  db: db.name,
                });

                return (
                  <div key={db.name} className="tree__db">
                    <div
                      className={`tree__row tree__row--db${
                        isDbSel ? ' tree__row--active' : ''
                      }`}
                      onClick={() => {
                        select({ kind: 'database', connectionId: conn.id, db: db.name });
                      }}
                      onDoubleClick={() => void toggleExpanded(conn.id, db.name)}
                      onContextMenu={(e) => onDbContextMenu(e, conn.id, db.name)}
                    >
                      <button
                        className="tree__caret-btn"
                        onClick={(e) => {
                          e.stopPropagation();
                          void toggleExpanded(conn.id, db.name);
                        }}
                      >
                        {expanded ? '▾' : '▸'}
                      </button>
                      <span className="tree__label">{db.name}</span>
                    </div>

                    {expanded && (
                      <div className="tree__colls">
                        {collsLoading && (
                          <div className="tree__row tree__row--ph muted">loading…</div>
                        )}
                        {!collsLoading && colls.length === 0 && (
                          <div className="tree__row tree__row--ph muted">(empty)</div>
                        )}
                        {colls
                          .filter((c) => matches(c.name, db.name))
                          .map((c) => {
                            const isSel = isSelected({
                              kind: 'collection',
                              connectionId: conn.id,
                              db: db.name,
                              collection: c.name,
                            });
                            return (
                              <div
                                key={c.name}
                                className={`tree__row tree__row--coll${
                                  isSel ? ' tree__row--active' : ''
                                }`}
                                onClick={() =>
                                  select({
                                    kind: 'collection',
                                    connectionId: conn.id,
                                    db: db.name,
                                    collection: c.name,
                                  })
                                }
                                onContextMenu={(e) =>
                                  onCollContextMenu(e, conn.id, db.name, c.name)
                                }
                              >
                                <span className="tree__coll-icon">▤</span>
                                <span className="tree__label">{c.name}</span>
                                {c.type !== 'collection' && (
                                  <span className="tree__type-tag">{c.type}</span>
                                )}
                              </div>
                            );
                          })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>

      {menu && (
        <ContextMenu
          x={menu.x}
          y={menu.y}
          items={menu.items}
          onClose={() => setMenu(null)}
        />
      )}
    </div>
  );
}
