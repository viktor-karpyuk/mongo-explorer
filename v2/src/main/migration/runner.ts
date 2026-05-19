import { EJSON } from 'bson';
import { BrowserWindow } from 'electron';
import type { MongoClient } from 'mongodb';
import { IpcChannels } from '../../shared/ipc.js';
import type {
  MigrationJob,
  MigrationProgress,
} from '../../shared/migration.js';
import type { AppContext } from '../context.js';
import type { MongoRegistry } from '../mongo/registry.js';

const BATCH_SIZE = 1000;
const HEARTBEAT_EVERY = 200;
const PUBLISH_EVERY_MS = 200;

interface ActiveRun {
  job: MigrationJob;
  pauseRequested: boolean;
  cancelRequested: boolean;
}

const active = new Map<string, ActiveRun>();

export function isRunning(jobId: string): boolean {
  return active.has(jobId);
}

export function pauseJob(jobId: string): void {
  const run = active.get(jobId);
  if (run) run.pauseRequested = true;
}

export function cancelJob(jobId: string): void {
  const run = active.get(jobId);
  if (run) run.cancelRequested = true;
}

export async function startJob(
  ctx: AppContext,
  registry: MongoRegistry,
  jobId: string,
): Promise<void> {
  const job = ctx.migrations!.get(jobId);
  if (!job) throw new Error(`Job ${jobId} not found.`);
  if (active.has(jobId)) return;

  const source = registry.getClient(job.spec.sourceConnectionId);
  const target = registry.getClient(job.spec.targetConnectionId);
  if (!source) throw new Error('Source connection not open.');
  if (!target) throw new Error('Target connection not open.');

  ctx.migrations!.setStatus(jobId, 'running');
  const run: ActiveRun = { job, pauseRequested: false, cancelRequested: false };
  active.set(jobId, run);

  void runLoop(ctx, source, target, run).finally(() => active.delete(jobId));
}

async function runLoop(
  ctx: AppContext,
  source: MongoClient,
  target: MongoClient,
  run: ActiveRun,
): Promise<void> {
  const { job } = run;
  const namespaces = job.spec.namespaces;
  let cp = job.checkpoint ?? {
    namespaceIndex: 0,
    lastId: null,
    copiedInNamespace: 0,
  };

  let lastPublish = 0;

  try {
    for (; cp.namespaceIndex < namespaces.length; cp.namespaceIndex++) {
      const ns = namespaces[cp.namespaceIndex]!;
      const srcColl = source.db(ns.db).collection(ns.collection);
      const tgtColl = target.db(ns.db).collection(ns.collection);

      if (job.spec.ensureCollections !== false) {
        try {
          await target.db(ns.db).createCollection(ns.collection);
        } catch {
          /* already exists */
        }
      }

      const estimated = await srcColl.estimatedDocumentCount();

      const filter = cp.lastId
        ? { _id: { $gt: EJSON.parse(cp.lastId) } }
        : {};
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const cursor = srcColl.find(filter as any).sort({ _id: 1 });

      let batch: object[] = [];
      let lastIdInBatch: object | null = null;

      for await (const doc of cursor) {
        if (run.cancelRequested) {
          throw new Error('Cancelled by user.');
        }
        if (run.pauseRequested) {
          if (batch.length > 0) {
            await tgtColl.insertMany(batch, { ordered: false });
            cp.copiedInNamespace += batch.length;
            cp.lastId = lastIdInBatch ? EJSON.stringify(lastIdInBatch) : cp.lastId;
            ctx.migrations!.setCheckpoint(job.id, cp);
          }
          ctx.migrations!.setStatus(job.id, 'paused');
          publish({
            jobId: job.id,
            status: 'paused',
            currentNamespace: ns,
            copied: cp.copiedInNamespace,
            estimatedTotal: estimated,
          });
          return;
        }

        batch.push(doc as object);
        lastIdInBatch = (doc as { _id: object })._id;

        if (batch.length >= BATCH_SIZE) {
          await tgtColl.insertMany(batch, { ordered: false });
          cp.copiedInNamespace += batch.length;
          cp.lastId = EJSON.stringify(lastIdInBatch);
          batch = [];

          if (cp.copiedInNamespace % HEARTBEAT_EVERY === 0) {
            ctx.migrations!.setCheckpoint(job.id, cp);
          }
          const now = Date.now();
          if (now - lastPublish > PUBLISH_EVERY_MS) {
            lastPublish = now;
            publish({
              jobId: job.id,
              status: 'running',
              currentNamespace: ns,
              copied: cp.copiedInNamespace,
              estimatedTotal: estimated,
            });
          }
        }
      }

      if (batch.length > 0) {
        await tgtColl.insertMany(batch, { ordered: false });
        cp.copiedInNamespace += batch.length;
        cp.lastId = lastIdInBatch ? EJSON.stringify(lastIdInBatch) : cp.lastId;
      }

      // Namespace done — reset checkpoint for next ns.
      cp = { namespaceIndex: cp.namespaceIndex + 1, lastId: null, copiedInNamespace: 0 };
      ctx.migrations!.setCheckpoint(job.id, cp);
    }

    ctx.migrations!.setStatus(job.id, 'completed');
    publish({ jobId: job.id, status: 'completed', copied: 0, estimatedTotal: null });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    ctx.migrations!.setStatus(job.id, 'failed', message);
    publish({
      jobId: job.id,
      status: 'failed',
      copied: cp.copiedInNamespace,
      estimatedTotal: null,
      error: message,
    });
  }
}

function publish(progress: MigrationProgress): void {
  for (const win of BrowserWindow.getAllWindows()) {
    win.webContents.send(IpcChannels.MigrateProgress, progress);
  }
}
