import type { ConnectionState } from '../../../shared/connectionState.js';

export function StatePill({ state }: { state: ConnectionState }) {
  const cls = `pill pill--${state.kind}`;
  switch (state.kind) {
    case 'disconnected':
      return <span className={cls}>disconnected</span>;
    case 'connecting':
      return <span className={cls}>connecting…</span>;
    case 'connected':
      return (
        <span className={cls} title={`MongoDB ${state.serverVersion} · ${state.topology}`}>
          connected · {state.pingMs} ms
        </span>
      );
    case 'error':
      return (
        <span className={cls} title={state.message}>
          error
        </span>
      );
  }
}
