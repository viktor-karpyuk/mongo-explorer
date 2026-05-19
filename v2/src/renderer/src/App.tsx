import { useEffect, useState } from 'react';

export function App() {
  const [version, setVersion] = useState<string>('—');
  const [pong, setPong] = useState<string>('—');

  useEffect(() => {
    void window.mex.appVersion().then(setVersion);
    void window.mex.ping().then(setPong);
  }, []);

  return (
    <div className="shell">
      <header className="shell-titlebar">
        <span className="shell-titlebar__brand">Mongo Explorer</span>
        <span className="shell-titlebar__version">v{version}</span>
      </header>
      <main className="shell-main">
        <div className="shell-card">
          <h1>Welcome</h1>
          <p>v2 scaffold is up. IPC roundtrip: {pong}.</p>
          <p className="muted">
            Phase A complete — Electron + React + TypeScript shell with a typed
            contextBridge. Next: persistence + crypto, then connections.
          </p>
        </div>
      </main>
    </div>
  );
}
