import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type { ClusterSnapshot } from '../../shared/cluster.js';
import type { MongoRegistry } from '../mongo/registry.js';
import { clusterSnapshot } from '../mongo/cluster.js';

function requireClient(registry: MongoRegistry, id: string) {
  const c = registry.getClient(id);
  if (!c) throw new Error(`Connection ${id} is not open.`);
  return c;
}

export function registerClusterIpc(registry: MongoRegistry): void {
  ipcMain.handle(
    IpcChannels.ClusterSnapshot,
    (_e, connectionId: string): Promise<ClusterSnapshot> =>
      clusterSnapshot(requireClient(registry, connectionId)),
  );
}
