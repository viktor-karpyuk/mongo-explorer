import { contextBridge, ipcRenderer } from 'electron';
import { IpcChannels, type BridgeApi } from '../shared/ipc.js';

const api: BridgeApi = {
  appVersion: () => ipcRenderer.invoke(IpcChannels.AppVersion),
  ping: () => ipcRenderer.invoke(IpcChannels.AppPing),
};

contextBridge.exposeInMainWorld('mex', api);
