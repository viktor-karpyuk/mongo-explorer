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
import type {
  BulkWriteInput,
  DeleteInput,
  InsertManyInput,
  InsertOneInput,
  MutateResponse,
  ReplaceInput,
  UpdateInput,
} from './mutation.js';
import type { ClusterSnapshot } from './cluster.js';
import type { MonitorTick, SlowOp } from './monitor.js';
import type {
  DumpRestoreEvent,
  DumpRestoreRequest,
  ExportRequest,
  ImportRequest,
  IoResult,
} from './io.js';
import type { ShellEvent } from './shell.js';
import type {
  MigrationJob,
  MigrationProgress,
  MigrationSpec,
  PreflightResult,
} from './migration.js';

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

  MutateInsertOne: 'mutate:insertOne',
  MutateInsertMany: 'mutate:insertMany',
  MutateUpdate: 'mutate:update',
  MutateReplace: 'mutate:replace',
  MutateDelete: 'mutate:delete',
  MutateBulk: 'mutate:bulk',

  ClusterSnapshot: 'cluster:snapshot',

  MonitorTick: 'monitor:tick',
  MonitorProfileGet: 'monitor:profile-get',
  MonitorProfileSet: 'monitor:profile-set',
  MonitorSlowOps: 'monitor:slow-ops',

  IoExport: 'io:export',
  IoImport: 'io:import',
  IoPickSave: 'io:pick-save',
  IoPickOpen: 'io:pick-open',
  IoPickDir: 'io:pick-dir',
  IoDumpRestoreStart: 'io:dump-restore-start',
  IoDumpRestoreEvent: 'io:dump-restore-event',

  ShellStart: 'shell:start',
  ShellSend: 'shell:send',
  ShellStop: 'shell:stop',
  ShellEvent: 'shell:event',

  MigrateList: 'migrate:list',
  MigrateGet: 'migrate:get',
  MigrateCreate: 'migrate:create',
  MigrateStart: 'migrate:start',
  MigratePause: 'migrate:pause',
  MigrateCancel: 'migrate:cancel',
  MigrateDelete: 'migrate:delete',
  MigratePreflight: 'migrate:preflight',
  MigrateProgress: 'migrate:progress',
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

export interface MutateApi {
  insertOne(input: InsertOneInput): Promise<MutateResponse>;
  insertMany(input: InsertManyInput): Promise<MutateResponse>;
  update(input: UpdateInput): Promise<MutateResponse>;
  replace(input: ReplaceInput): Promise<MutateResponse>;
  delete(input: DeleteInput): Promise<MutateResponse>;
  bulk(input: BulkWriteInput): Promise<MutateResponse>;
}

export interface ClusterApi {
  snapshot(connectionId: string): Promise<ClusterSnapshot>;
}

export interface ProfilerInput {
  level: 0 | 1 | 2;
  slowMs: number;
}

export interface MonitorApi {
  tick(connectionId: string): Promise<MonitorTick>;
  profileGet(connectionId: string, db: string): Promise<ProfilerInput>;
  profileSet(connectionId: string, db: string, input: ProfilerInput): Promise<void>;
  slowOps(connectionId: string, db: string, limit?: number): Promise<SlowOp[]>;
}

export interface IoApi {
  export(req: ExportRequest): Promise<IoResult>;
  import(req: ImportRequest): Promise<IoResult>;
  pickSave(defaultName: string): Promise<string | null>;
  pickOpen(): Promise<string | null>;
  pickDir(): Promise<string | null>;
  startDumpRestore(req: DumpRestoreRequest): Promise<string>;
  onDumpRestoreEvent(cb: (event: DumpRestoreEvent) => void): () => void;
}

export interface ShellApi {
  start(connectionId: string): Promise<string>;
  send(sessionId: string, text: string): Promise<boolean>;
  stop(sessionId: string): Promise<void>;
  onEvent(cb: (event: ShellEvent) => void): () => void;
}

export interface MigrationApi {
  list(): Promise<MigrationJob[]>;
  get(jobId: string): Promise<MigrationJob | null>;
  preflight(spec: MigrationSpec): Promise<PreflightResult>;
  create(spec: MigrationSpec): Promise<MigrationJob>;
  start(jobId: string): Promise<void>;
  pause(jobId: string): Promise<void>;
  cancel(jobId: string): Promise<void>;
  delete(jobId: string): Promise<void>;
  onProgress(cb: (event: MigrationProgress) => void): () => void;
}

export interface BridgeApi {
  appVersion(): Promise<string>;
  ping(): Promise<'pong'>;
  connections: ConnectionsApi;
  ns: NamespacesApi;
  query: QueriesApi;
  schema: SchemaApi;
  mutate: MutateApi;
  cluster: ClusterApi;
  monitor: MonitorApi;
  io: IoApi;
  shell: ShellApi;
  migrate: MigrationApi;
}

declare global {
  interface Window {
    mex: BridgeApi;
  }
}
