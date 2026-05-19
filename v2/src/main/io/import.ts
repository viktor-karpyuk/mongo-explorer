import { EJSON } from 'bson';
import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import type { MongoClient } from 'mongodb';
import type { ImportRequest, IoResult } from '../../shared/io.js';

const BATCH_SIZE = 1000;

export async function runImport(
  client: MongoClient,
  req: ImportRequest,
): Promise<IoResult> {
  const t0 = performance.now();
  try {
    const docs = await readDocuments(req);
    let inserted = 0;
    if (!req.dryRun && docs.length > 0) {
      const coll = client.db(req.db).collection(req.collection);
      for (let i = 0; i < docs.length; i += BATCH_SIZE) {
        const batch = docs.slice(i, i + BATCH_SIZE);
        const r = await coll.insertMany(batch, { ordered: req.ordered ?? true });
        inserted += r.insertedCount;
      }
    }
    return {
      ok: true,
      read: docs.length,
      inserted: req.dryRun ? 0 : inserted,
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

async function readDocuments(req: ImportRequest): Promise<Record<string, unknown>[]> {
  if (req.format === 'ndjson') {
    return readNdjson(req.filePath);
  }
  if (req.format === 'json') {
    return readJsonArray(req.filePath);
  }
  return readCsv(req.filePath);
}

async function readNdjson(path: string): Promise<Record<string, unknown>[]> {
  const docs: Record<string, unknown>[] = [];
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.trim() === '') continue;
    docs.push(EJSON.parse(line) as Record<string, unknown>);
  }
  return docs;
}

async function readJsonArray(path: string): Promise<Record<string, unknown>[]> {
  const chunks: Buffer[] = [];
  for await (const chunk of createReadStream(path)) {
    chunks.push(chunk as Buffer);
  }
  const text = Buffer.concat(chunks).toString('utf8');
  const parsed = EJSON.parse(text) as unknown;
  if (!Array.isArray(parsed)) {
    throw new Error('Top-level JSON must be an array.');
  }
  return parsed as Record<string, unknown>[];
}

async function readCsv(path: string): Promise<Record<string, unknown>[]> {
  const chunks: Buffer[] = [];
  for await (const chunk of createReadStream(path)) {
    chunks.push(chunk as Buffer);
  }
  const text = Buffer.concat(chunks).toString('utf8');
  const lines = text.split(/\r?\n/).filter((l) => l !== '');
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]!);
  return lines.slice(1).map((line) => {
    const cells = parseCsvLine(line);
    const obj: Record<string, unknown> = {};
    headers.forEach((h, i) => {
      obj[h] = cells[i] ?? '';
    });
    return obj;
  });
}

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let buf = '';
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"' && line[i + 1] === '"') {
        buf += '"';
        i++;
      } else if (ch === '"') {
        inQuote = false;
      } else {
        buf += ch;
      }
    } else {
      if (ch === ',') {
        out.push(buf);
        buf = '';
      } else if (ch === '"') {
        inQuote = true;
      } else {
        buf += ch;
      }
    }
  }
  out.push(buf);
  return out;
}
