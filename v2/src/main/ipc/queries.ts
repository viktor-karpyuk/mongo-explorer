import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type { FindRequest, FindResponse } from '../../shared/query.js';
import { executeFind } from '../mongo/find.js';
import type { MongoRegistry } from '../mongo/registry.js';
import type { AppContext } from '../context.js';

export function registerQueryIpc(ctx: AppContext, registry: MongoRegistry): void {
  ipcMain.handle(
    IpcChannels.QueryFind,
    async (_e, req: FindRequest): Promise<FindResponse> => {
      const t0 = performance.now();
      const client = registry.getClient(req.connectionId);
      if (!client) {
        return {
          ok: false,
          error: 'Connection is not open.',
          durationMs: Math.round(performance.now() - t0),
        };
      }
      try {
        const result = await executeFind(client, req);
        ctx.queryHistory.record({
          connectionId: req.connectionId,
          database: req.db,
          collection: req.collection,
          kind: 'find',
          body: JSON.stringify({
            filter: req.filter ?? '',
            projection: req.projection ?? '',
            sort: req.sort ?? '',
            skip: req.skip ?? 0,
            limit: req.limit ?? 50,
          }),
          durationMs: result.durationMs,
          rowCount: result.rows.length,
        });
        return result;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        ctx.queryHistory.record({
          connectionId: req.connectionId,
          database: req.db,
          collection: req.collection,
          kind: 'find',
          body: JSON.stringify({
            filter: req.filter ?? '',
            projection: req.projection ?? '',
            sort: req.sort ?? '',
            skip: req.skip ?? 0,
            limit: req.limit ?? 50,
          }),
          error: message,
        });
        return {
          ok: false,
          error: message,
          durationMs: Math.round(performance.now() - t0),
        };
      }
    },
  );

  ipcMain.handle(
    IpcChannels.QueryHistory,
    (_e, connectionId: string, limit?: number) =>
      ctx.queryHistory.list(connectionId, limit),
  );
}
