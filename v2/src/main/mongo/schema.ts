import type { MongoClient } from 'mongodb';
import type { SchemaField, SchemaReport } from '../../shared/schema.js';

const MAX_DISTINCT_TRACKED = 64;

interface Stat {
  presenceCount: number;
  types: Map<string, number>;
  values: Set<string>;
}

export async function analyzeSchema(
  client: MongoClient,
  db: string,
  collection: string,
  sampleSize = 1000,
): Promise<SchemaReport> {
  const coll = client.db(db).collection(collection);
  const totalDocs = await coll.estimatedDocumentCount();
  const docs = await coll
    .aggregate([{ $sample: { size: Math.min(sampleSize, Math.max(totalDocs, 1)) } }])
    .toArray();

  const stats = new Map<string, Stat>();
  for (const doc of docs) walk(doc as Record<string, unknown>, '', stats);

  const fields: SchemaField[] = [...stats.entries()]
    .map(([path, stat]) => {
      const totalTypeHits = [...stat.types.values()].reduce((s, n) => s + n, 0) || 1;
      const types: Record<string, number> = {};
      for (const [t, n] of stat.types) types[t] = n / totalTypeHits;
      return {
        path,
        presence: docs.length === 0 ? 0 : stat.presenceCount / docs.length,
        types,
        approxDistinct: stat.values.size,
      };
    })
    .sort((a, b) => b.presence - a.presence);

  return { sampleSize: docs.length, totalDocs, fields };
}

function walk(value: unknown, prefix: string, stats: Map<string, Stat>): void {
  if (value === undefined) return;
  if (value === null) {
    bump(stats, prefix, 'null', value);
    return;
  }
  if (Array.isArray(value)) {
    bump(stats, prefix, 'array', value);
    if (value.length > 0) walk(value[0], `${prefix}[]`, stats);
    return;
  }
  if (typeof value === 'object') {
    if (isBsonWrapper(value)) {
      bump(stats, prefix, bsonWrapperType(value as Record<string, unknown>), value);
      return;
    }
    bump(stats, prefix, 'object', value);
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      walk(v, prefix ? `${prefix}.${k}` : k, stats);
    }
    return;
  }
  if (typeof value === 'string') bump(stats, prefix, 'string', value);
  else if (typeof value === 'number') bump(stats, prefix, 'number', value);
  else if (typeof value === 'boolean') bump(stats, prefix, 'boolean', value);
}

function bump(
  stats: Map<string, Stat>,
  path: string,
  type: string,
  value: unknown,
): void {
  if (!path) return;
  let stat = stats.get(path);
  if (!stat) {
    stat = { presenceCount: 0, types: new Map(), values: new Set() };
    stats.set(path, stat);
  }
  stat.presenceCount++;
  stat.types.set(type, (stat.types.get(type) ?? 0) + 1);
  if (stat.values.size < MAX_DISTINCT_TRACKED) {
    stat.values.add(typeof value === 'object' ? JSON.stringify(value) : String(value));
  }
}

function isBsonWrapper(v: unknown): boolean {
  if (typeof v !== 'object' || v === null) return false;
  const cls = v.constructor?.name ?? '';
  return ['ObjectId', 'Long', 'Decimal128', 'Binary', 'Timestamp', 'BSONRegExp', 'MinKey', 'MaxKey', 'Date'].includes(cls);
}

function bsonWrapperType(v: Record<string, unknown>): string {
  const cls = (v as { constructor: { name: string } }).constructor.name;
  switch (cls) {
    case 'ObjectId': return 'objectid';
    case 'Long': return 'long';
    case 'Decimal128': return 'decimal';
    case 'Binary': return 'binary';
    case 'Timestamp': return 'timestamp';
    case 'BSONRegExp': return 'regex';
    case 'MinKey': return 'minkey';
    case 'MaxKey': return 'maxkey';
    case 'Date': return 'date';
    default: return cls.toLowerCase();
  }
}
