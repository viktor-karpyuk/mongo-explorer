import { contextBridge, ipcRenderer } from 'electron';
import {
  IpcChannels,
  type BridgeApi,
  type ConnectionsApi,
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

const api: BridgeApi = {
  appVersion: () => ipcRenderer.invoke(IpcChannels.AppVersion) as Promise<string>,
  ping: () => ipcRenderer.invoke(IpcChannels.AppPing) as Promise<'pong'>,
  connections,
};

contextBridge.exposeInMainWorld('mex', api);
