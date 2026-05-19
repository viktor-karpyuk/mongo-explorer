import type { AnyBulkWriteOperation, MongoClient } from 'mongodb';
import type {
  BulkWriteInput,
  DeleteInput,
  InsertManyInput,
  InsertOneInput,
  MutateResponse,
  ReplaceInput,
  UpdateInput,
} from '../../shared/mutation.js';
import { parseFilter } from './queryParser.js';

function parseDoc(input: string): Record<string, unknown> {
  return parseFilter(input);
}

function parseDocs(input: string): Record<string, unknown>[] {
  const trimmed = input.trim();
  if (!trimmed) return [];
  if (trimmed.startsWith('[')) {
    const arr = parseFilter(trimmed) as unknown;
    if (!Array.isArray(arr)) throw new Error('Top-level must be an array.');
    return arr as Record<string, unknown>[];
  }
  // NDJSON
  return trimmed
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => parseDoc(l));
}

async function timed<T>(fn: () => Promise<T>): Promise<[T, number]> {
  const t0 = performance.now();
  const result = await fn();
  return [result, Math.round(performance.now() - t0)];
}

function err(e: unknown, t0: number): MutateResponse {
  return {
    ok: false,
    error: e instanceof Error ? e.message : String(e),
    durationMs: Math.round(performance.now() - t0),
  };
}

export async function insertOne(
  client: MongoClient,
  input: InsertOneInput,
): Promise<MutateResponse> {
  const t0 = performance.now();
  try {
    const doc = parseDoc(input.document);
    const [r, durationMs] = await timed(() =>
      client.db(input.db).collection(input.collection).insertOne(doc),
    );
    return {
      ok: true,
      inserted: r.acknowledged ? 1 : 0,
      upsertedIds: [String(r.insertedId)],
      durationMs,
    };
  } catch (e) {
    return err(e, t0);
  }
}

export async function insertMany(
  client: MongoClient,
  input: InsertManyInput,
): Promise<MutateResponse> {
  const t0 = performance.now();
  try {
    const docs = parseDocs(input.documents);
    if (docs.length === 0) throw new Error('No documents to insert.');
    const [r, durationMs] = await timed(() =>
      client
        .db(input.db)
        .collection(input.collection)
        .insertMany(docs, { ordered: input.ordered ?? true }),
    );
    return {
      ok: true,
      inserted: r.insertedCount,
      upsertedIds: Object.values(r.insertedIds).map(String),
      durationMs,
    };
  } catch (e) {
    return err(e, t0);
  }
}

export async function updateDocs(
  client: MongoClient,
  input: UpdateInput,
): Promise<MutateResponse> {
  const t0 = performance.now();
  try {
    const filter = parseDoc(input.filter);
    const update = parseDoc(input.update);
    const coll = client.db(input.db).collection(input.collection);
    const opts = { upsert: !!input.upsert };
    const r = input.many
      ? await coll.updateMany(filter, update, opts)
      : await coll.updateOne(filter, update, opts);
    return {
      ok: true,
      matched: r.matchedCount,
      modified: r.modifiedCount,
      upsertedIds: r.upsertedId ? [String(r.upsertedId)] : [],
      durationMs: Math.round(performance.now() - t0),
    };
  } catch (e) {
    return err(e, t0);
  }
}

export async function replaceOne(
  client: MongoClient,
  input: ReplaceInput,
): Promise<MutateResponse> {
  const t0 = performance.now();
  try {
    const filter = parseDoc(input.filter);
    const replacement = parseDoc(input.replacement);
    const r = await client
      .db(input.db)
      .collection(input.collection)
      .replaceOne(filter, replacement, { upsert: !!input.upsert });
    return {
      ok: true,
      matched: r.matchedCount,
      modified: r.modifiedCount,
      upsertedIds: r.upsertedId ? [String(r.upsertedId)] : [],
      durationMs: Math.round(performance.now() - t0),
    };
  } catch (e) {
    return err(e, t0);
  }
}

export async function deleteDocs(
  client: MongoClient,
  input: DeleteInput,
): Promise<MutateResponse> {
  const t0 = performance.now();
  try {
    const filter = parseDoc(input.filter);
    const coll = client.db(input.db).collection(input.collection);
    const r = input.many ? await coll.deleteMany(filter) : await coll.deleteOne(filter);
    return {
      ok: true,
      deleted: r.deletedCount,
      durationMs: Math.round(performance.now() - t0),
    };
  } catch (e) {
    return err(e, t0);
  }
}

export async function bulkWrite(
  client: MongoClient,
  input: BulkWriteInput,
): Promise<MutateResponse> {
  const t0 = performance.now();
  try {
    const ops: AnyBulkWriteOperation[] = input.ops.map((op) => {
      const body = parseDoc(op.body);
      switch (op.kind) {
        case 'insertOne':
          return { insertOne: { document: body as Record<string, unknown> } };
        case 'updateOne':
          return {
            updateOne: {
              filter: (body as Record<string, unknown>)['filter'] as Record<string, unknown>,
              update: (body as Record<string, unknown>)['update'] as Record<string, unknown>,
              upsert: !!(body as Record<string, unknown>)['upsert'],
            },
          };
        case 'updateMany':
          return {
            updateMany: {
              filter: (body as Record<string, unknown>)['filter'] as Record<string, unknown>,
              update: (body as Record<string, unknown>)['update'] as Record<string, unknown>,
              upsert: !!(body as Record<string, unknown>)['upsert'],
            },
          };
        case 'replaceOne':
          return {
            replaceOne: {
              filter: (body as Record<string, unknown>)['filter'] as Record<string, unknown>,
              replacement: (body as Record<string, unknown>)['replacement'] as Record<string, unknown>,
              upsert: !!(body as Record<string, unknown>)['upsert'],
            },
          };
        case 'deleteOne':
          return { deleteOne: { filter: body as Record<string, unknown> } };
        case 'deleteMany':
          return { deleteMany: { filter: body as Record<string, unknown> } };
      }
    });
    const r = await client
      .db(input.db)
      .collection(input.collection)
      .bulkWrite(ops, { ordered: input.ordered ?? true });
    return {
      ok: true,
      matched: r.matchedCount,
      modified: r.modifiedCount,
      inserted: r.insertedCount,
      deleted: r.deletedCount,
      durationMs: Math.round(performance.now() - t0),
    };
  } catch (e) {
    return err(e, t0);
  }
}
