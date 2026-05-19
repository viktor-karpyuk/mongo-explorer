import { dialog, ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  DumpRestoreRequest,
  ExportRequest,
  ImportRequest,
  IoResult,
} from '../../shared/io.js';
import type { MongoRegistry } from '../mongo/registry.js';
import type { AppContext } from '../context.js';
import { exportFind } from '../io/export.js';
import { runImport } from '../io/import.js';
import { startDumpRestore } from '../io/dumpRestore.js';

function requireClient(registry: MongoRegistry, id: string) {
  const c = registry.getClient(id);
  if (!c) throw new Error(`Connection ${id} is not open.`);
  return c;
}

export function registerIoIpc(ctx: AppContext, registry: MongoRegistry): void {
  ipcMain.handle(
    IpcChannels.IoExport,
    (_e, req: ExportRequest): Promise<IoResult> =>
      exportFind(requireClient(registry, req.connectionId), req),
  );

  ipcMain.handle(
    IpcChannels.IoImport,
    (_e, req: ImportRequest): Promise<IoResult> =>
      runImport(requireClient(registry, req.connectionId), req),
  );

  ipcMain.handle(IpcChannels.IoPickSave, async (_e, defaultName: string) => {
    const result = await dialog.showSaveDialog({ defaultPath: defaultName });
    return result.canceled ? null : result.filePath ?? null;
  });

  ipcMain.handle(IpcChannels.IoPickOpen, async () => {
    const result = await dialog.showOpenDialog({ properties: ['openFile'] });
    return result.canceled || result.filePaths.length === 0
      ? null
      : result.filePaths[0]!;
  });

  ipcMain.handle(IpcChannels.IoPickDir, async () => {
    const result = await dialog.showOpenDialog({ properties: ['openDirectory'] });
    return result.canceled || result.filePaths.length === 0
      ? null
      : result.filePaths[0]!;
  });

  ipcMain.handle(
    IpcChannels.IoDumpRestoreStart,
    (_e, req: DumpRestoreRequest): string => startDumpRestore(ctx, req),
  );
}
