import { contextBridge, ipcRenderer } from 'electron';
import {
  IpcChannels,
  type BridgeApi,
  type ConnectionsApi,
  type NamespacesApi,
} from '../shared/ipc.js';
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

const api: BridgeApi = {
  appVersion: () => ipcRenderer.invoke(IpcChannels.AppVersion) as Promise<string>,
  ping: () => ipcRenderer.invoke(IpcChannels.AppPing) as Promise<'pong'>,
  connections,
  ns,
};

contextBridge.exposeInMainWorld('mex', api);
