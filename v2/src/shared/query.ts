export interface FindRequest {
  connectionId: string;
  db: string;
  collection: string;
  filter?: string;       // Mongo shell or EJSON
  projection?: string;
  sort?: string;
  skip?: number;
  limit?: number;
  maxTimeMs?: number;
}

export interface FindResult {
  ok: true;
  /** Documents serialized as canonical EJSON strings, one per row. */
  rows: string[];
  hasMore: boolean;
  durationMs: number;
}

export interface QueryError {
  ok: false;
  error: string;
  /** Optional source location for parse errors. */
  location?: { kind: 'filter' | 'projection' | 'sort'; message: string };
  durationMs: number;
}

export type FindResponse = FindResult | QueryError;
