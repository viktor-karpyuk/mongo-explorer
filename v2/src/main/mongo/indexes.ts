import type { MongoClient, Document, IndexSpecification } from 'mongodb';
import type {
  CreateIndexInput,
  IndexInfo,
  IndexKey,
  IndexStatRow,
} from '../../shared/schema.js';

export async function listIndexes(
  client: MongoClient,
  db: string,
  collection: string,
): Promise<IndexInfo[]> {
  const indexes = (await client.db(db).collection(collection).indexes()) as Document[];
  return indexes.map((idx) => {
    const keys: IndexKey[] = Object.entries(idx['key'] as Record<string, unknown>).map(
      ([field, dir]) => ({
        field,
        direction: dir as IndexKey['direction'],
      }),
    );
    return {
      name: String(idx['name']),
      keys,
      unique: Boolean(idx['unique']),
      sparse: Boolean(idx['sparse']),
      background: Boolean(idx['background']),
      ttlSeconds:
        typeof idx['expireAfterSeconds'] === 'number'
          ? (idx['expireAfterSeconds'] as number)
          : null,
      partialFilter: idx['partialFilterExpression']
        ? JSON.stringify(idx['partialFilterExpression'])
        : undefined,
    };
  });
}

export async function createIndex(
  client: MongoClient,
  db: string,
  collection: string,
  input: CreateIndexInput,
): Promise<string> {
  const spec: Record<string, IndexKey['direction']> = {};
  for (const k of input.keys) spec[k.field] = k.direction;
  const opts: Record<string, unknown> = {};
  if (input.name) opts['name'] = input.name;
  if (input.unique) opts['unique'] = true;
  if (input.sparse) opts['sparse'] = true;
  if (input.background) opts['background'] = true;
  if (input.ttlSeconds != null) opts['expireAfterSeconds'] = input.ttlSeconds;
  if (input.partialFilter) {
    try {
      opts['partialFilterExpression'] = JSON.parse(input.partialFilter);
    } catch (e) {
      throw new Error(
        `partialFilter is not valid JSON: ${(e as Error).message}`,
      );
    }
  }
  return client
    .db(db)
    .collection(collection)
    .createIndex(spec as IndexSpecification, opts);
}

export async function dropIndex(
  client: MongoClient,
  db: string,
  collection: string,
  name: string,
): Promise<void> {
  await client.db(db).collection(collection).dropIndex(name);
}

export async function indexStats(
  client: MongoClient,
  db: string,
  collection: string,
): Promise<IndexStatRow[]> {
  const rows = (await client
    .db(db)
    .collection(collection)
    .aggregate([{ $indexStats: {} }])
    .toArray()) as Document[];
  return rows.map((r) => ({
    name: String(r['name']),
    ops: Number((r['accesses'] as Document | undefined)?.['ops'] ?? 0),
    since:
      (r['accesses'] as Document | undefined)?.['since']
        ? String((r['accesses'] as Document)['since'])
        : null,
  }));
}
