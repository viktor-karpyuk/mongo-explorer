import { EJSON } from 'bson';
import type { MongoClient } from 'mongodb';
import type { FindRequest, FindResult } from '../../shared/query.js';
import { parseFilter, parseProjection, parseSort } from './queryParser.js';

const PAGE_FETCH_EXTRA = 1;

export async function executeFind(
  client: MongoClient,
  req: FindRequest,
): Promise<FindResult> {
  const t0 = performance.now();

  const filter = parseFilter(req.filter);
  const projection = parseProjection(req.projection);
  const sort = parseSort(req.sort);
  const skip = clampNonNegative(req.skip);
  const limit = clampPositive(req.limit, 50);

  let cursor = client.db(req.db).collection(req.collection).find(filter);

  if (Object.keys(projection).length > 0) cursor = cursor.project(projection);
  if (Object.keys(sort).length > 0) cursor = cursor.sort(sort);
  if (skip > 0) cursor = cursor.skip(skip);

  // Fetch one extra row to determine hasMore without exhausting cursor.
  cursor = cursor.limit(limit + PAGE_FETCH_EXTRA);

  if (req.maxTimeMs && req.maxTimeMs > 0) {
    cursor = cursor.maxTimeMS(req.maxTimeMs);
  }

  const fetched = await cursor.toArray();
  const hasMore = fetched.length > limit;
  const rows = (hasMore ? fetched.slice(0, limit) : fetched).map((d) =>
    EJSON.stringify(d, { relaxed: false }),
  );

  return {
    ok: true,
    rows,
    hasMore,
    durationMs: Math.round(performance.now() - t0),
  };
}

function clampNonNegative(n: number | undefined): number {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.floor(n!));
}

function clampPositive(n: number | undefined, fallback: number): number {
  if (!Number.isFinite(n) || (n ?? 0) <= 0) return fallback;
  return Math.max(1, Math.floor(n!));
}
