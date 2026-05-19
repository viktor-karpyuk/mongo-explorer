import { useEffect, useState } from 'react';
import { ConnectionsView } from './components/ConnectionsView';
import { subscribeConnectionState } from './store/connections';

export function App() {
  const [version, setVersion] = useState<string>('—');

  useEffect(() => {
    void window.mex.appVersion().then(setVersion);
    const unsub = subscribeConnectionState();
    return unsub;
  }, []);

  return (
    <div className="shell">
      <header className="shell-titlebar">
        <span className="shell-titlebar__brand">Mongo Explorer</span>
        <span className="shell-titlebar__version">v{version}</span>
      </header>
      <main className="shell-main">
        <ConnectionsView />
      </main>
    </div>
  );
}
