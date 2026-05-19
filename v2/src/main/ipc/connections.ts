import { BrowserWindow, ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  ConnectionInput,
  ConnectionRecord,
  ConnectionSummary,
} from '../../shared/connection.js';
import type {
  ConnectionState,
  ConnectionStateChange,
  TestResult,
} from '../../shared/connectionState.js';
import type { AppContext } from '../context.js';
import { MongoRegistry } from '../mongo/registry.js';

export function registerConnectionsIpc(ctx: AppContext): MongoRegistry {
  const registry = new MongoRegistry();

  registry.onStateChange((id, state) => {
    const change: ConnectionStateChange = { id, state };
    for (const win of BrowserWindow.getAllWindows()) {
      win.webContents.send(IpcChannels.ConnStateChanged, change);
    }
  });

  ipcMain.handle(IpcChannels.ConnList, (): ConnectionSummary[] =>
    ctx.connections.list(),
  );

  ipcMain.handle(IpcChannels.ConnGet, (_e, id: string): ConnectionRecord | null =>
    ctx.connections.get(id),
  );

  ipcMain.handle(
    IpcChannels.ConnCreate,
    (_e, input: ConnectionInput): ConnectionSummary => ctx.connections.create(input),
  );

  ipcMain.handle(
    IpcChannels.ConnUpdate,
    (_e, id: string, input: ConnectionInput): ConnectionSummary | null =>
      ctx.connections.update(id, input),
  );

  ipcMain.handle(IpcChannels.ConnDelete, async (_e, id: string): Promise<boolean> => {
    await registry.disconnect(id);
    return ctx.connections.delete(id);
  });

  ipcMain.handle(
    IpcChannels.ConnDuplicate,
    (_e, id: string): ConnectionSummary | null => {
      const record = ctx.connections.get(id);
      if (!record) return null;
      return ctx.connections.create({
        name: `${record.name} (copy)`,
        uri: record.uri,
        ssh: record.ssh,
        notes: record.notes,
      });
    },
  );

  ipcMain.handle(
    IpcChannels.ConnTest,
    async (_e, input: ConnectionInput): Promise<TestResult> => {
      const record: ConnectionRecord = {
        id: '__test__',
        name: input.name,
        uri: input.uri,
        ssh: input.ssh,
        notes: input.notes,
        createdAt: 0,
        updatedAt: 0,
      };
      return registry.test(record);
    },
  );

  ipcMain.handle(
    IpcChannels.ConnOpen,
    async (_e, id: string): Promise<ConnectionState> => {
      const record = ctx.connections.get(id);
      if (!record) return { kind: 'error', message: 'Connection not found' };
      const state = await registry.connect(record);
      if (state.kind === 'connected') ctx.connections.touchLastUsed(id);
      return state;
    },
  );

  ipcMain.handle(IpcChannels.ConnClose, async (_e, id: string): Promise<void> => {
    await registry.disconnect(id);
  });

  ipcMain.handle(
    IpcChannels.ConnState,
    (_e, id: string): ConnectionState => registry.getState(id),
  );

  ipcMain.handle(
    IpcChannels.ConnStatesAll,
    (): Array<{ id: string; state: ConnectionState }> =>
      [...registry.snapshot().entries()].map(([id, state]) => ({ id, state })),
  );

  return registry;
}
