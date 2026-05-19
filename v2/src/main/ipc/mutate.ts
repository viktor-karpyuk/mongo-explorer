import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  BulkWriteInput,
  DeleteInput,
  InsertManyInput,
  InsertOneInput,
  MutateResponse,
  ReplaceInput,
  UpdateInput,
} from '../../shared/mutation.js';
import type { MongoRegistry } from '../mongo/registry.js';
import {
  bulkWrite,
  deleteDocs,
  insertMany,
  insertOne,
  replaceOne,
  updateDocs,
} from '../mongo/mutate.js';

function requireClient(registry: MongoRegistry, id: string) {
  const c = registry.getClient(id);
  if (!c) throw new Error(`Connection ${id} is not open.`);
  return c;
}

export function registerMutateIpc(registry: MongoRegistry): void {
  ipcMain.handle(
    IpcChannels.MutateInsertOne,
    (_e, input: InsertOneInput): Promise<MutateResponse> =>
      insertOne(requireClient(registry, input.connectionId), input),
  );
  ipcMain.handle(
    IpcChannels.MutateInsertMany,
    (_e, input: InsertManyInput): Promise<MutateResponse> =>
      insertMany(requireClient(registry, input.connectionId), input),
  );
  ipcMain.handle(
    IpcChannels.MutateUpdate,
    (_e, input: UpdateInput): Promise<MutateResponse> =>
      updateDocs(requireClient(registry, input.connectionId), input),
  );
  ipcMain.handle(
    IpcChannels.MutateReplace,
    (_e, input: ReplaceInput): Promise<MutateResponse> =>
      replaceOne(requireClient(registry, input.connectionId), input),
  );
  ipcMain.handle(
    IpcChannels.MutateDelete,
    (_e, input: DeleteInput): Promise<MutateResponse> =>
      deleteDocs(requireClient(registry, input.connectionId), input),
  );
  ipcMain.handle(
    IpcChannels.MutateBulk,
    (_e, input: BulkWriteInput): Promise<MutateResponse> =>
      bulkWrite(requireClient(registry, input.connectionId), input),
  );
}
