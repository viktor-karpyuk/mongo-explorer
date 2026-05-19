import { EJSON } from 'bson';
import type { MongoClient } from 'mongodb';
import type { ExplainRequest } from '../../shared/schema.js';
import { parseFilter, parseProjection, parseSort } from './queryParser.js';

export async function explainFind(
  client: MongoClient,
  req: ExplainRequest,
): Promise<string> {
  const filter = parseFilter(req.filter);
  const projection = parseProjection(req.projection);
  const sort = parseSort(req.sort);

  let cursor = client.db(req.db).collection(req.collection).find(filter);
  if (Object.keys(projection).length > 0) cursor = cursor.project(projection);
  if (Object.keys(sort).length > 0) cursor = cursor.sort(sort);
  if ((req.skip ?? 0) > 0) cursor = cursor.skip(req.skip!);
  if ((req.limit ?? 0) > 0) cursor = cursor.limit(req.limit!);

  const explain = await cursor.explain(req.verbosity ?? 'queryPlanner');
  return EJSON.stringify(explain, { relaxed: false });
}
