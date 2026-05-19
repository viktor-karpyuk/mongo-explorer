import { contextBridge, ipcRenderer } from 'electron';
import {
  IpcChannels,
  type BridgeApi,
  type ClusterApi,
  type ConnectionsApi,
  type IoApi,
  type MonitorApi,
  type MutateApi,
  type NamespacesApi,
  type ProfilerInput,
  type QueriesApi,
  type QueryHistoryRow,
  type MigrationApi,
  type SchemaApi,
  type ShellApi,
} from '../shared/ipc.js';
import type { ShellEvent } from '../shared/shell.js';
import type {
  MigrationJob,
  MigrationProgress,
  MigrationSpec,
  PreflightResult,
} from '../shared/migration.js';
import type { ClusterSnapshot } from '../shared/cluster.js';
import type { MonitorTick, SlowOp } from '../shared/monitor.js';
import type {
  DumpRestoreEvent,
  DumpRestoreRequest,
  ExportRequest,
  ImportRequest,
  IoResult,
} from '../shared/io.js';
import type {
  BulkWriteInput,
  DeleteInput,
  InsertManyInput,
  InsertOneInput,
  MutateResponse,
  ReplaceInput,
  UpdateInput,
} from '../shared/mutation.js';
import type { FindRequest, FindResponse } from '../shared/query.js';
import type { AggregateRequest, AggregateResponse } from '../shared/aggregation.js';
import type {
  CreateIndexInput,
  ExplainRequest,
  ExplainResponse,
  IndexInfo,
  IndexStatRow,
  SchemaReport,
  ValidatorPayload,
} from '../shared/schema.js';
import type {
  ConnectionInput,
  ConnectionRecord,
  ConnectionSummary,
} from '../shared/connection.js';
import type {
  ConnectionState,
  ConnectionStateChange,
  TestResult,
} from '../shared/connectionState.js';
import type {
  CollStats,
  CollectionInfo,
  CreateCollectionInput,
  DatabaseInfo,
  DbStats,
} from '../shared/namespace.js';

const connections: ConnectionsApi = {
  list: () => ipcRenderer.invoke(IpcChannels.ConnList) as Promise<ConnectionSummary[]>,
  get: (id) =>
    ipcRenderer.invoke(IpcChannels.ConnGet, id) as Promise<ConnectionRecord | null>,
  create: (input: ConnectionInput) =>
    ipcRenderer.invoke(IpcChannels.ConnCreate, input) as Promise<ConnectionSummary>,
  update: (id, input) =>
    ipcRenderer.invoke(IpcChannels.ConnUpdate, id, input) as Promise<
      ConnectionSummary | null
    >,
  delete: (id) => ipcRenderer.invoke(IpcChannels.ConnDelete, id) as Promise<boolean>,
  duplicate: (id) =>
    ipcRenderer.invoke(IpcChannels.ConnDuplicate, id) as Promise<
      ConnectionSummary | null
    >,
  test: (input) => ipcRenderer.invoke(IpcChannels.ConnTest, input) as Promise<TestResult>,
  open: (id) => ipcRenderer.invoke(IpcChannels.ConnOpen, id) as Promise<ConnectionState>,
  close: (id) => ipcRenderer.invoke(IpcChannels.ConnClose, id) as Promise<void>,
  state: (id) => ipcRenderer.invoke(IpcChannels.ConnState, id) as Promise<ConnectionState>,
  states: () =>
    ipcRenderer.invoke(IpcChannels.ConnStatesAll) as Promise<
      Array<{ id: string; state: ConnectionState }>
    >,
  onStateChange: (cb) => {
    const handler = (_e: unknown, change: ConnectionStateChange): void => cb(change);
    ipcRenderer.on(IpcChannels.ConnStateChanged, handler);
    return () => {
      ipcRenderer.off(IpcChannels.ConnStateChanged, handler);
    };
  },
};

const ns: NamespacesApi = {
  listDatabases: (id) =>
    ipcRenderer.invoke(IpcChannels.DbList, id) as Promise<DatabaseInfo[]>,
  createDatabase: (id, name) =>
    ipcRenderer.invoke(IpcChannels.DbCreate, id, name) as Promise<void>,
  dropDatabase: (id, name) =>
    ipcRenderer.invoke(IpcChannels.DbDrop, id, name) as Promise<void>,
  dbStats: (id, name) =>
    ipcRenderer.invoke(IpcChannels.DbStats, id, name) as Promise<DbStats>,

  listCollections: (id, db) =>
    ipcRenderer.invoke(IpcChannels.CollList, id, db) as Promise<CollectionInfo[]>,
  createCollection: (id, db, input: CreateCollectionInput) =>
    ipcRenderer.invoke(IpcChannels.CollCreate, id, db, input) as Promise<void>,
  dropCollection: (id, db, name) =>
    ipcRenderer.invoke(IpcChannels.CollDrop, id, db, name) as Promise<void>,
  renameCollection: (id, db, from, to) =>
    ipcRenderer.invoke(IpcChannels.CollRename, id, db, from, to) as Promise<void>,
  collStats: (id, db, name) =>
    ipcRenderer.invoke(IpcChannels.CollStats, id, db, name) as Promise<CollStats>,
};

const query: QueriesApi = {
  find: (req: FindRequest) =>
    ipcRenderer.invoke(IpcChannels.QueryFind, req) as Promise<FindResponse>,
  aggregate: (req: AggregateRequest) =>
    ipcRenderer.invoke(IpcChannels.QueryAggregate, req) as Promise<AggregateResponse>,
  explain: (req: ExplainRequest) =>
    ipcRenderer.invoke(IpcChannels.QueryExplain, req) as Promise<ExplainResponse>,
  history: (connectionId, limit) =>
    ipcRenderer.invoke(IpcChannels.QueryHistory, connectionId, limit) as Promise<
      QueryHistoryRow[]
    >,
};

const schema: SchemaApi = {
  analyze: (id, db, coll, sampleSize) =>
    ipcRenderer.invoke(IpcChannels.SchemaAnalyze, id, db, coll, sampleSize) as Promise<SchemaReport>,
  listIndexes: (id, db, coll) =>
    ipcRenderer.invoke(IpcChannels.IdxList, id, db, coll) as Promise<IndexInfo[]>,
  createIndex: (id, db, coll, input: CreateIndexInput) =>
    ipcRenderer.invoke(IpcChannels.IdxCreate, id, db, coll, input) as Promise<string>,
  dropIndex: (id, db, coll, name) =>
    ipcRenderer.invoke(IpcChannels.IdxDrop, id, db, coll, name) as Promise<void>,
  indexStats: (id, db, coll) =>
    ipcRenderer.invoke(IpcChannels.IdxStats, id, db, coll) as Promise<IndexStatRow[]>,
  getValidator: (id, db, coll) =>
    ipcRenderer.invoke(IpcChannels.ValidatorGet, id, db, coll) as Promise<ValidatorPayload>,
  setValidator: (id, db, coll, payload) =>
    ipcRenderer.invoke(IpcChannels.ValidatorSet, id, db, coll, payload) as Promise<void>,
};

const mutate: MutateApi = {
  insertOne: (input: InsertOneInput) =>
    ipcRenderer.invoke(IpcChannels.MutateInsertOne, input) as Promise<MutateResponse>,
  insertMany: (input: InsertManyInput) =>
    ipcRenderer.invoke(IpcChannels.MutateInsertMany, input) as Promise<MutateResponse>,
  update: (input: UpdateInput) =>
    ipcRenderer.invoke(IpcChannels.MutateUpdate, input) as Promise<MutateResponse>,
  replace: (input: ReplaceInput) =>
    ipcRenderer.invoke(IpcChannels.MutateReplace, input) as Promise<MutateResponse>,
  delete: (input: DeleteInput) =>
    ipcRenderer.invoke(IpcChannels.MutateDelete, input) as Promise<MutateResponse>,
  bulk: (input: BulkWriteInput) =>
    ipcRenderer.invoke(IpcChannels.MutateBulk, input) as Promise<MutateResponse>,
};

const cluster: ClusterApi = {
  snapshot: (id) =>
    ipcRenderer.invoke(IpcChannels.ClusterSnapshot, id) as Promise<ClusterSnapshot>,
};

const io: IoApi = {
  export: (req: ExportRequest) =>
    ipcRenderer.invoke(IpcChannels.IoExport, req) as Promise<IoResult>,
  import: (req: ImportRequest) =>
    ipcRenderer.invoke(IpcChannels.IoImport, req) as Promise<IoResult>,
  pickSave: (defaultName) =>
    ipcRenderer.invoke(IpcChannels.IoPickSave, defaultName) as Promise<string | null>,
  pickOpen: () => ipcRenderer.invoke(IpcChannels.IoPickOpen) as Promise<string | null>,
  pickDir: () => ipcRenderer.invoke(IpcChannels.IoPickDir) as Promise<string | null>,
  startDumpRestore: (req: DumpRestoreRequest) =>
    ipcRenderer.invoke(IpcChannels.IoDumpRestoreStart, req) as Promise<string>,
  onDumpRestoreEvent: (cb) => {
    const handler = (_e: unknown, event: DumpRestoreEvent) => cb(event);
    ipcRenderer.on(IpcChannels.IoDumpRestoreEvent, handler);
    return () => {
      ipcRenderer.off(IpcChannels.IoDumpRestoreEvent, handler);
    };
  },
};

const monitor: MonitorApi = {
  tick: (id) => ipcRenderer.invoke(IpcChannels.MonitorTick, id) as Promise<MonitorTick>,
  profileGet: (id, db) =>
    ipcRenderer.invoke(IpcChannels.MonitorProfileGet, id, db) as Promise<ProfilerInput>,
  profileSet: (id, db, input) =>
    ipcRenderer.invoke(IpcChannels.MonitorProfileSet, id, db, input) as Promise<void>,
  slowOps: (id, db, limit) =>
    ipcRenderer.invoke(IpcChannels.MonitorSlowOps, id, db, limit) as Promise<SlowOp[]>,
};

const shell: ShellApi = {
  start: (connectionId) =>
    ipcRenderer.invoke(IpcChannels.ShellStart, connectionId) as Promise<string>,
  send: (sessionId, text) =>
    ipcRenderer.invoke(IpcChannels.ShellSend, sessionId, text) as Promise<boolean>,
  stop: (sessionId) =>
    ipcRenderer.invoke(IpcChannels.ShellStop, sessionId) as Promise<void>,
  onEvent: (cb) => {
    const handler = (_e: unknown, event: ShellEvent) => cb(event);
    ipcRenderer.on(IpcChannels.ShellEvent, handler);
    return () => {
      ipcRenderer.off(IpcChannels.ShellEvent, handler);
    };
  },
};

const migrate: MigrationApi = {
  list: () => ipcRenderer.invoke(IpcChannels.MigrateList) as Promise<MigrationJob[]>,
  get: (jobId) =>
    ipcRenderer.invoke(IpcChannels.MigrateGet, jobId) as Promise<MigrationJob | null>,
  preflight: (spec: MigrationSpec) =>
    ipcRenderer.invoke(IpcChannels.MigratePreflight, spec) as Promise<PreflightResult>,
  create: (spec: MigrationSpec) =>
    ipcRenderer.invoke(IpcChannels.MigrateCreate, spec) as Promise<MigrationJob>,
  start: (jobId) => ipcRenderer.invoke(IpcChannels.MigrateStart, jobId) as Promise<void>,
  pause: (jobId) => ipcRenderer.invoke(IpcChannels.MigratePause, jobId) as Promise<void>,
  cancel: (jobId) =>
    ipcRenderer.invoke(IpcChannels.MigrateCancel, jobId) as Promise<void>,
  delete: (jobId) =>
    ipcRenderer.invoke(IpcChannels.MigrateDelete, jobId) as Promise<void>,
  onProgress: (cb) => {
    const handler = (_e: unknown, event: MigrationProgress) => cb(event);
    ipcRenderer.on(IpcChannels.MigrateProgress, handler);
    return () => {
      ipcRenderer.off(IpcChannels.MigrateProgress, handler);
    };
  },
};

const api: BridgeApi = {
  appVersion: () => ipcRenderer.invoke(IpcChannels.AppVersion) as Promise<string>,
  ping: () => ipcRenderer.invoke(IpcChannels.AppPing) as Promise<'pong'>,
  connections,
  ns,
  query,
  schema,
  mutate,
  cluster,
  monitor,
  io,
  shell,
  migrate,
};

contextBridge.exposeInMainWorld('mex', api);
