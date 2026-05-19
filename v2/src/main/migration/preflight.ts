import type { MongoClient } from 'mongodb';
import type { MigrationSpec, PreflightResult } from '../../shared/migration.js';

export async function preflight(
  source: MongoClient,
  target: MongoClient,
  spec: MigrationSpec,
): Promise<PreflightResult> {
  const checks = [] as PreflightResult['checks'];

  checks.push(await ping('Source reachable', source));
  checks.push(await ping('Target reachable', target));

  for (const ns of spec.namespaces) {
    try {
      const exists = await source
        .db(ns.db)
        .listCollections({ name: ns.collection })
        .toArray();
      checks.push({
        name: `Source has ${ns.db}.${ns.collection}`,
        ok: exists.length > 0,
        detail: exists.length === 0 ? 'collection not found' : undefined,
      });
    } catch (err) {
      checks.push({
        name: `Source has ${ns.db}.${ns.collection}`,
        ok: false,
        detail: (err as Error).message,
      });
    }
  }

  return { ok: checks.every((c) => c.ok), checks };
}

async function ping(name: string, client: MongoClient) {
  try {
    await client.db('admin').command({ ping: 1 });
    return { name, ok: true };
  } catch (err) {
    return { name, ok: false, detail: (err as Error).message };
  }
}
