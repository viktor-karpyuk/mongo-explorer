export type ConnectionState =
  | { kind: 'disconnected' }
  | { kind: 'connecting' }
  | {
      kind: 'connected';
      pingMs: number;
      serverVersion: string;
      topology: string;
      connectedAt: number;
    }
  | { kind: 'error'; message: string };

export interface ConnectionStateChange {
  id: string;
  state: ConnectionState;
}

export interface TestResult {
  ok: boolean;
  pingMs?: number;
  serverVersion?: string;
  topology?: string;
  error?: string;
}
