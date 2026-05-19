import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  AggregateRequest,
  AggregateResponse,
} from '../../shared/aggregation.js';
import type { MongoRegistry } from '../mongo/registry.js';
import type { AppContext } from '../context.js';
import { executeAggregate } from '../mongo/aggregate.js';

export function registerAggregateIpc(
  ctx: AppContext,
  registry: MongoRegistry,
): void {
  ipcMain.handle(
    IpcChannels.QueryAggregate,
    async (_e, req: AggregateRequest): Promise<AggregateResponse> => {
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
        const result = await executeAggregate(client, req);
        ctx.queryHistory.record({
          connectionId: req.connectionId,
          database: req.db,
          collection: req.collection,
          kind: 'aggregate',
          body: JSON.stringify(req.pipeline),
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
          kind: 'aggregate',
          body: JSON.stringify(req.pipeline),
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
}
