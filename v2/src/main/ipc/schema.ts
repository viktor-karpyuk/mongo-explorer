import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  CreateIndexInput,
  ExplainRequest,
  ExplainResponse,
  IndexInfo,
  IndexStatRow,
  SchemaReport,
  ValidatorPayload,
} from '../../shared/schema.js';
import type { MongoRegistry } from '../mongo/registry.js';
import { analyzeSchema } from '../mongo/schema.js';
import { createIndex, dropIndex, indexStats, listIndexes } from '../mongo/indexes.js';
import { getValidator, setValidator } from '../mongo/validator.js';
import { explainFind } from '../mongo/explain.js';

function requireClient(registry: MongoRegistry, id: string) {
  const c = registry.getClient(id);
  if (!c) throw new Error(`Connection ${id} is not open.`);
  return c;
}

export function registerSchemaIpc(registry: MongoRegistry): void {
  ipcMain.handle(
    IpcChannels.SchemaAnalyze,
    (
      _e,
      connectionId: string,
      db: string,
      collection: string,
      sampleSize?: number,
    ): Promise<SchemaReport> =>
      analyzeSchema(requireClient(registry, connectionId), db, collection, sampleSize),
  );

  ipcMain.handle(
    IpcChannels.IdxList,
    (_e, id: string, db: string, coll: string): Promise<IndexInfo[]> =>
      listIndexes(requireClient(registry, id), db, coll),
  );

  ipcMain.handle(
    IpcChannels.IdxCreate,
    (_e, id: string, db: string, coll: string, input: CreateIndexInput): Promise<string> =>
      createIndex(requireClient(registry, id), db, coll, input),
  );

  ipcMain.handle(
    IpcChannels.IdxDrop,
    (_e, id: string, db: string, coll: string, name: string): Promise<void> =>
      dropIndex(requireClient(registry, id), db, coll, name),
  );

  ipcMain.handle(
    IpcChannels.IdxStats,
    (_e, id: string, db: string, coll: string): Promise<IndexStatRow[]> =>
      indexStats(requireClient(registry, id), db, coll),
  );

  ipcMain.handle(
    IpcChannels.ValidatorGet,
    (_e, id: string, db: string, coll: string): Promise<ValidatorPayload> =>
      getValidator(requireClient(registry, id), db, coll),
  );

  ipcMain.handle(
    IpcChannels.ValidatorSet,
    (
      _e,
      id: string,
      db: string,
      coll: string,
      payload: ValidatorPayload,
    ): Promise<void> =>
      setValidator(requireClient(registry, id), db, coll, payload),
  );

  ipcMain.handle(
    IpcChannels.QueryExplain,
    async (_e, req: ExplainRequest): Promise<ExplainResponse> => {
      try {
        const explain = await explainFind(requireClient(registry, req.connectionId), req);
        return { ok: true, explain };
      } catch (err) {
        return { ok: false, error: err instanceof Error ? err.message : String(err) };
      }
    },
  );
}
