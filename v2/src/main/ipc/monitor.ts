import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type { MonitorTick, SlowOp } from '../../shared/monitor.js';
import type { MongoRegistry } from '../mongo/registry.js';
import {
  getProfilerLevel,
  listSlowOps,
  setProfilerLevel,
  tick,
  type ProfilerLevelInput,
} from '../mongo/monitor.js';

function requireClient(registry: MongoRegistry, id: string) {
  const c = registry.getClient(id);
  if (!c) throw new Error(`Connection ${id} is not open.`);
  return c;
}

export function registerMonitorIpc(registry: MongoRegistry): void {
  ipcMain.handle(
    IpcChannels.MonitorTick,
    (_e, connectionId: string): Promise<MonitorTick> =>
      tick(requireClient(registry, connectionId)),
  );

  ipcMain.handle(
    IpcChannels.MonitorProfileGet,
    (_e, connectionId: string, db: string): Promise<ProfilerLevelInput> =>
      getProfilerLevel(requireClient(registry, connectionId), db),
  );

  ipcMain.handle(
    IpcChannels.MonitorProfileSet,
    (_e, connectionId: string, db: string, input: ProfilerLevelInput): Promise<void> =>
      setProfilerLevel(requireClient(registry, connectionId), db, input),
  );

  ipcMain.handle(
    IpcChannels.MonitorSlowOps,
    (_e, connectionId: string, db: string, limit?: number): Promise<SlowOp[]> =>
      listSlowOps(requireClient(registry, connectionId), db, limit),
  );
}
