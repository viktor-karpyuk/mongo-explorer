import type {
  ConnectionInput,
  ConnectionRecord,
  ConnectionSummary,
} from './connection.js';
import type {
  ConnectionState,
  ConnectionStateChange,
  TestResult,
} from './connectionState.js';

export const IpcChannels = {
  AppVersion: 'app:version',
  AppPing: 'app:ping',

  ConnList: 'connections:list',
  ConnGet: 'connections:get',
  ConnCreate: 'connections:create',
  ConnUpdate: 'connections:update',
  ConnDelete: 'connections:delete',
  ConnDuplicate: 'connections:duplicate',
  ConnTest: 'connections:test',
  ConnOpen: 'connections:open',
  ConnClose: 'connections:close',
  ConnState: 'connections:state',
  ConnStatesAll: 'connections:states',
  ConnStateChanged: 'connections:state-changed',
} as const;

export type IpcChannel = (typeof IpcChannels)[keyof typeof IpcChannels];

export interface ConnectionsApi {
  list(): Promise<ConnectionSummary[]>;
  get(id: string): Promise<ConnectionRecord | null>;
  create(input: ConnectionInput): Promise<ConnectionSummary>;
  update(id: string, input: ConnectionInput): Promise<ConnectionSummary | null>;
  delete(id: string): Promise<boolean>;
  duplicate(id: string): Promise<ConnectionSummary | null>;
  test(input: ConnectionInput): Promise<TestResult>;
  open(id: string): Promise<ConnectionState>;
  close(id: string): Promise<void>;
  state(id: string): Promise<ConnectionState>;
  states(): Promise<Array<{ id: string; state: ConnectionState }>>;
  onStateChange(cb: (change: ConnectionStateChange) => void): () => void;
}

export interface BridgeApi {
  appVersion(): Promise<string>;
  ping(): Promise<'pong'>;
  connections: ConnectionsApi;
}

declare global {
  interface Window {
    mex: BridgeApi;
  }
}
