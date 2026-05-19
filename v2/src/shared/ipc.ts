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
import type {
  CollStats,
  CollectionInfo,
  CreateCollectionInput,
  DatabaseInfo,
  DbStats,
} from './namespace.js';

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

  DbList: 'db:list',
  DbCreate: 'db:create',
  DbDrop: 'db:drop',
  DbStats: 'db:stats',
  CollList: 'coll:list',
  CollCreate: 'coll:create',
  CollDrop: 'coll:drop',
  CollRename: 'coll:rename',
  CollStats: 'coll:stats',
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

export interface NamespacesApi {
  listDatabases(connectionId: string): Promise<DatabaseInfo[]>;
  createDatabase(connectionId: string, name: string): Promise<void>;
  dropDatabase(connectionId: string, name: string): Promise<void>;
  dbStats(connectionId: string, name: string): Promise<DbStats>;

  listCollections(connectionId: string, db: string): Promise<CollectionInfo[]>;
  createCollection(
    connectionId: string,
    db: string,
    input: CreateCollectionInput,
  ): Promise<void>;
  dropCollection(connectionId: string, db: string, name: string): Promise<void>;
  renameCollection(
    connectionId: string,
    db: string,
    from: string,
    to: string,
  ): Promise<void>;
  collStats(connectionId: string, db: string, name: string): Promise<CollStats>;
}

export interface BridgeApi {
  appVersion(): Promise<string>;
  ping(): Promise<'pong'>;
  connections: ConnectionsApi;
  ns: NamespacesApi;
}

declare global {
  interface Window {
    mex: BridgeApi;
  }
}
