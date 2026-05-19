import { EJSON } from 'bson';
import { createWriteStream } from 'node:fs';
import type { MongoClient } from 'mongodb';
import type { ExportRequest, IoResult } from '../../shared/io.js';
import { parseFilter, parseProjection, parseSort } from '../mongo/queryParser.js';

export async function exportFind(
  client: MongoClient,
  req: ExportRequest,
): Promise<IoResult> {
  const t0 = performance.now();
  try {
    const filter = parseFilter(req.filter);
    const projection = parseProjection(req.projection);
    const sort = parseSort(req.sort);

    let cursor = client.db(req.db).collection(req.collection).find(filter);
    if (Object.keys(projection).length > 0) cursor = cursor.project(projection);
    if (Object.keys(sort).length > 0) cursor = cursor.sort(sort);
    if ((req.limit ?? 0) > 0) cursor = cursor.limit(req.limit!);

    const out = createWriteStream(req.filePath);
    let written = 0;

    if (req.format === 'json') {
      out.write('[\n');
    }

    let csvHeaders: string[] | null = null;

    for await (const doc of cursor) {
      const ejson = EJSON.serialize(doc as object, { relaxed: false });
      if (req.format === 'ndjson') {
        out.write(JSON.stringify(ejson));
        out.write('\n');
      } else if (req.format === 'json') {
        if (written > 0) out.write(',\n');
        out.write(JSON.stringify(ejson));
      } else if (req.format === 'csv') {
        if (csvHeaders === null) {
          csvHeaders = Object.keys(ejson as Record<string, unknown>);
          out.write(csvHeaders.map(csvQuote).join(','));
          out.write('\n');
        }
        const row = csvHeaders
          .map((h) => csvQuote(stringifyCell((ejson as Record<string, unknown>)[h])))
          .join(',');
        out.write(row);
        out.write('\n');
      }
      written++;
    }

    if (req.format === 'json') {
      out.write('\n]\n');
    }

    await new Promise<void>((resolve, reject) => {
      out.end((err: unknown) => (err ? reject(err as Error) : resolve()));
    });

    return {
      ok: true,
      written,
      durationMs: Math.round(performance.now() - t0),
    };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
      durationMs: Math.round(performance.now() - t0),
    };
  }
}

function csvQuote(s: string): string {
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function stringifyCell(v: unknown): string {
  if (v === undefined || v === null) return '';
  if (typeof v === 'string') return v;
  return JSON.stringify(v);
}
