import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  MigrationJob,
  MigrationSpec,
  PreflightResult,
} from '../../shared/migration.js';
import type { AppContext } from '../context.js';
import type { MongoRegistry } from '../mongo/registry.js';
import { preflight } from '../migration/preflight.js';
import { cancelJob, pauseJob, startJob } from '../migration/runner.js';

function requireClient(registry: MongoRegistry, id: string) {
  const c = registry.getClient(id);
  if (!c) throw new Error(`Connection ${id} is not open.`);
  return c;
}

export function registerMigrationIpc(ctx: AppContext, registry: MongoRegistry): void {
  ipcMain.handle(IpcChannels.MigrateList, (): MigrationJob[] => ctx.migrations.list());
  ipcMain.handle(IpcChannels.MigrateGet, (_e, id: string): MigrationJob | null =>
    ctx.migrations.get(id),
  );

  ipcMain.handle(
    IpcChannels.MigratePreflight,
    async (_e, spec: MigrationSpec): Promise<PreflightResult> => {
      const source = requireClient(registry, spec.sourceConnectionId);
      const target = requireClient(registry, spec.targetConnectionId);
      return preflight(source, target, spec);
    },
  );

  ipcMain.handle(
    IpcChannels.MigrateCreate,
    (_e, spec: MigrationSpec): MigrationJob => ctx.migrations.create(spec),
  );

  ipcMain.handle(IpcChannels.MigrateStart, async (_e, jobId: string): Promise<void> => {
    await startJob(ctx, registry, jobId);
  });

  ipcMain.handle(IpcChannels.MigratePause, (_e, jobId: string): void => {
    pauseJob(jobId);
  });

  ipcMain.handle(IpcChannels.MigrateCancel, (_e, jobId: string): void => {
    cancelJob(jobId);
  });

  ipcMain.handle(IpcChannels.MigrateDelete, (_e, jobId: string): void => {
    ctx.migrations.delete(jobId);
  });
}
