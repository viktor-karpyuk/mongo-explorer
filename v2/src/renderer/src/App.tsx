import { useEffect, useState } from 'react';
import { ConnectionsView } from './components/ConnectionsView';
import { Tree } from './components/Tree';
import { DbStatsPanel } from './components/DbStatsPanel';
import { CollectionStatsPanel } from './components/CollectionStatsPanel';
import { subscribeConnectionState, useConnectionsStore } from './store/connections';
import { useSelectionStore } from './store/selection';
import { useNamespacesStore } from './store/namespaces';

export function App() {
  const [version, setVersion] = useState<string>('—');
  const selection = useSelectionStore((s) => s.current);

  useEffect(() => {
    void window.mex.appVersion().then(setVersion);
    const unsub = subscribeConnectionState();

    const unsubStates = useConnectionsStore.subscribe((state, prev) => {
      for (const id of Object.keys(state.states)) {
        const cur = state.states[id];
        const before = prev.states[id];
        if (cur?.kind === 'connected' && before?.kind !== 'connected') {
          void useNamespacesStore.getState().loadDatabases(id);
        }
        if (cur?.kind !== 'connected' && before?.kind === 'connected') {
          useNamespacesStore.getState().reset(id);
        }
      }
    });

    return () => {
      unsub();
      unsubStates();
    };
  }, []);

  return (
    <div className="shell">
      <header className="shell-titlebar">
        <span className="shell-titlebar__brand">Mongo Explorer</span>
        <span className="shell-titlebar__version">v{version}</span>
      </header>
      <div className="shell-body">
        <aside className="shell-sidebar">
          <Tree />
        </aside>
        <main className="shell-main">
          {selection.kind === 'welcome' && <ConnectionsView />}
          {selection.kind === 'database' && (
            <DbStatsPanel connectionId={selection.connectionId} db={selection.db} />
          )}
          {selection.kind === 'collection' && (
            <CollectionStatsPanel
              connectionId={selection.connectionId}
              db={selection.db}
              collection={selection.collection}
            />
          )}
        </main>
      </div>
    </div>
  );
}
