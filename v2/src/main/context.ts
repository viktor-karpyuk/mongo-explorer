import { app } from 'electron';
import { join } from 'node:path';
import { openStore, type Db } from './db/store.js';
import { ConnectionsRepo } from './db/connections.js';
import { PrefsRepo } from './db/prefs.js';
import { QueryHistoryRepo } from './db/queryHistory.js';
import type { MongoRegistry } from './mongo/registry.js';

export interface AppContext {
  db: Db;
  connections: ConnectionsRepo;
  prefs: PrefsRepo;
  queryHistory: QueryHistoryRepo;
  registry?: MongoRegistry;
}

let context: AppContext | undefined;

export function initContext(): AppContext {
  if (context) return context;

  const dbPath = join(app.getPath('userData'), 'mex-v2.db');
  const db = openStore(dbPath);
  context = {
    db,
    connections: new ConnectionsRepo(db),
    prefs: new PrefsRepo(db),
    queryHistory: new QueryHistoryRepo(db),
  };
  return context;
}

export function getContext(): AppContext {
  if (!context) throw new Error('AppContext not initialized — call initContext() first.');
  return context;
}

export async function closeContext(): Promise<void> {
  if (context) {
    await context.registry?.disconnectAll();
    context.db.close();
    context = undefined;
  }
}
