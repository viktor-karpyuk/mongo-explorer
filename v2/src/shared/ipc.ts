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
import type { FindRequest, FindResponse } from './query.js';
import type { AggregateRequest, AggregateResponse } from './aggregation.js';
import type {
  CreateIndexInput,
  ExplainRequest,
  ExplainResponse,
  IndexInfo,
  IndexStatRow,
  SchemaReport,
  ValidatorPayload,
} from './schema.js';

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

  QueryFind: 'query:find',
  QueryAggregate: 'query:aggregate',
  QueryExplain: 'query:explain',
  QueryHistory: 'query:history',

  SchemaAnalyze: 'schema:analyze',
  IdxList: 'idx:list',
  IdxCreate: 'idx:create',
  IdxDrop: 'idx:drop',
  IdxStats: 'idx:stats',
  ValidatorGet: 'validator:get',
  ValidatorSet: 'validator:set',
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

export interface QueryHistoryRow {
  id: number;
  connectionId: string;
  database: string;
  collection: string;
  kind: 'find' | 'aggregate' | 'command';
  body: string;
  durationMs: number | null;
  rowCount: number | null;
  error: string | null;
  ranAt: number;
}

export interface QueriesApi {
  find(req: FindRequest): Promise<FindResponse>;
  aggregate(req: AggregateRequest): Promise<AggregateResponse>;
  explain(req: ExplainRequest): Promise<ExplainResponse>;
  history(connectionId: string, limit?: number): Promise<QueryHistoryRow[]>;
}

export interface SchemaApi {
  analyze(connectionId: string, db: string, coll: string, sampleSize?: number): Promise<SchemaReport>;

  listIndexes(connectionId: string, db: string, coll: string): Promise<IndexInfo[]>;
  createIndex(connectionId: string, db: string, coll: string, input: CreateIndexInput): Promise<string>;
  dropIndex(connectionId: string, db: string, coll: string, name: string): Promise<void>;
  indexStats(connectionId: string, db: string, coll: string): Promise<IndexStatRow[]>;

  getValidator(connectionId: string, db: string, coll: string): Promise<ValidatorPayload>;
  setValidator(connectionId: string, db: string, coll: string, payload: ValidatorPayload): Promise<void>;
}

export interface BridgeApi {
  appVersion(): Promise<string>;
  ping(): Promise<'pong'>;
  connections: ConnectionsApi;
  ns: NamespacesApi;
  query: QueriesApi;
  schema: SchemaApi;
}

declare global {
  interface Window {
    mex: BridgeApi;
  }
}
