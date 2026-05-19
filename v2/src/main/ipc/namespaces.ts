import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  CollStats,
  CollectionInfo,
  CreateCollectionInput,
  DatabaseInfo,
  DbStats,
} from '../../shared/namespace.js';
import type { MongoRegistry } from '../mongo/registry.js';
import {
  collStats,
  createCollection,
  createDatabase,
  dbStats,
  dropCollection,
  dropDatabase,
  listCollections,
  listDatabases,
  renameCollection,
} from '../mongo/namespaces.js';

function requireClient(registry: MongoRegistry, connectionId: string) {
  const client = registry.getClient(connectionId);
  if (!client) throw new Error(`Connection ${connectionId} is not open.`);
  return client;
}

export function registerNamespaceIpc(registry: MongoRegistry): void {
  ipcMain.handle(
    IpcChannels.DbList,
    (_e, connectionId: string): Promise<DatabaseInfo[]> =>
      listDatabases(requireClient(registry, connectionId)),
  );

  ipcMain.handle(
    IpcChannels.DbCreate,
    (_e, connectionId: string, name: string): Promise<void> =>
      createDatabase(requireClient(registry, connectionId), name),
  );

  ipcMain.handle(
    IpcChannels.DbDrop,
    (_e, connectionId: string, name: string): Promise<void> =>
      dropDatabase(requireClient(registry, connectionId), name),
  );

  ipcMain.handle(
    IpcChannels.DbStats,
    (_e, connectionId: string, name: string): Promise<DbStats> =>
      dbStats(requireClient(registry, connectionId), name),
  );

  ipcMain.handle(
    IpcChannels.CollList,
    (_e, connectionId: string, db: string): Promise<CollectionInfo[]> =>
      listCollections(requireClient(registry, connectionId), db),
  );

  ipcMain.handle(
    IpcChannels.CollCreate,
    (
      _e,
      connectionId: string,
      db: string,
      input: CreateCollectionInput,
    ): Promise<void> =>
      createCollection(requireClient(registry, connectionId), db, input),
  );

  ipcMain.handle(
    IpcChannels.CollDrop,
    (_e, connectionId: string, db: string, name: string): Promise<void> =>
      dropCollection(requireClient(registry, connectionId), db, name),
  );

  ipcMain.handle(
    IpcChannels.CollRename,
    (
      _e,
      connectionId: string,
      db: string,
      from: string,
      to: string,
    ): Promise<void> =>
      renameCollection(requireClient(registry, connectionId), db, from, to),
  );

  ipcMain.handle(
    IpcChannels.CollStats,
    (_e, connectionId: string, db: string, name: string): Promise<CollStats> =>
      collStats(requireClient(registry, connectionId), db, name),
  );
}
