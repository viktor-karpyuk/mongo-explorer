import type { Document, MongoClient } from 'mongodb';
import type {
  CollStats,
  CollectionInfo,
  CreateCollectionInput,
  DatabaseInfo,
  DbStats,
} from '../../shared/namespace.js';

export async function listDatabases(client: MongoClient): Promise<DatabaseInfo[]> {
  const result = (await client.db('admin').command({ listDatabases: 1 })) as Document;
  const dbs = (result['databases'] as Document[] | undefined) ?? [];
  return dbs.map((d) => ({
    name: String(d['name']),
    sizeOnDisk: typeof d['sizeOnDisk'] === 'number' ? d['sizeOnDisk'] : undefined,
    empty: typeof d['empty'] === 'boolean' ? d['empty'] : undefined,
  }));
}

export async function listCollections(
  client: MongoClient,
  database: string,
): Promise<CollectionInfo[]> {
  const raw = await client
    .db(database)
    .listCollections({}, { nameOnly: false })
    .toArray();
  return raw.map((c) => {
    const opts = ((c as Document)['options'] ?? {}) as Record<string, unknown>;
    const type: CollectionInfo['type'] =
      c.type === 'view'
        ? 'view'
        : opts['timeseries']
          ? 'timeseries'
          : 'collection';
    return { name: c.name, type, options: opts };
  });
}

export async function dbStats(client: MongoClient, database: string): Promise<DbStats> {
  const stats = (await client.db(database).command({ dbStats: 1, scale: 1 })) as Document;
  return {
    db: database,
    collections: Number(stats['collections'] ?? 0),
    views: Number(stats['views'] ?? 0),
    objects: Number(stats['objects'] ?? 0),
    avgObjSize: Number(stats['avgObjSize'] ?? 0),
    dataSize: Number(stats['dataSize'] ?? 0),
    storageSize: Number(stats['storageSize'] ?? 0),
    indexes: Number(stats['indexes'] ?? 0),
    indexSize: Number(stats['indexSize'] ?? 0),
    totalSize: Number(stats['totalSize'] ?? 0),
  };
}

export async function collStats(
  client: MongoClient,
  database: string,
  collection: string,
): Promise<CollStats> {
  const stats = (await client
    .db(database)
    .command({ collStats: collection, scale: 1 })) as Document;
  return {
    ns: `${database}.${collection}`,
    count: Number(stats['count'] ?? 0),
    size: Number(stats['size'] ?? 0),
    avgObjSize: Number(stats['avgObjSize'] ?? 0),
    storageSize: Number(stats['storageSize'] ?? 0),
    totalIndexSize: Number(stats['totalIndexSize'] ?? 0),
    nindexes: Number(stats['nindexes'] ?? 0),
    capped: Boolean(stats['capped']),
  };
}

export async function createDatabase(
  client: MongoClient,
  database: string,
): Promise<void> {
  // Mongo creates the db lazily on first write. We create a placeholder
  // collection and immediately drop it so the database becomes visible.
  await client.db(database).createCollection('_mex_init');
  await client.db(database).collection('_mex_init').drop();
}

export async function dropDatabase(client: MongoClient, database: string): Promise<void> {
  await client.db(database).dropDatabase();
}

export async function createCollection(
  client: MongoClient,
  database: string,
  input: CreateCollectionInput,
): Promise<void> {
  const options: Record<string, unknown> = {};
  if (input.capped) {
    options['capped'] = true;
    options['size'] = input.capped.sizeBytes;
    if (input.capped.maxDocs) options['max'] = input.capped.maxDocs;
  }
  if (input.timeseries) {
    options['timeseries'] = input.timeseries;
  }
  if (input.validator) {
    options['validator'] = input.validator;
    if (input.validationLevel) options['validationLevel'] = input.validationLevel;
    if (input.validationAction) options['validationAction'] = input.validationAction;
  }
  await client.db(database).createCollection(input.name, options);
}

export async function dropCollection(
  client: MongoClient,
  database: string,
  collection: string,
): Promise<void> {
  await client.db(database).collection(collection).drop();
}

export async function renameCollection(
  client: MongoClient,
  database: string,
  from: string,
  to: string,
): Promise<void> {
  await client.db(database).renameCollection(from, to);
}
