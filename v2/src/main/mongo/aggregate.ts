import { EJSON } from 'bson';
import type { MongoClient } from 'mongodb';
import type {
  AggregateRequest,
  AggregateResult,
} from '../../shared/aggregation.js';
import { parseFilter } from './queryParser.js';

const PAGE_FETCH_EXTRA = 1;

export async function executeAggregate(
  client: MongoClient,
  req: AggregateRequest,
): Promise<AggregateResult> {
  const t0 = performance.now();
  const limit = req.limit > 0 ? Math.floor(req.limit) : 50;

  const pipeline = req.pipeline.map(({ operator, body }) => {
    if (!operator.startsWith('$')) {
      throw new Error(`Stage operator must start with $: ${operator}`);
    }
    const parsedBody = parseFilter(body);
    return { [operator]: parsedBody } as Record<string, unknown>;
  });

  // Append a final limit so we don't pull more than we can show.
  pipeline.push({ $limit: limit + PAGE_FETCH_EXTRA });

  let cursor = client
    .db(req.db)
    .collection(req.collection)
    .aggregate(pipeline, { allowDiskUse: true });
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
