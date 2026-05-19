export interface SchemaField {
  path: string;
  /** Fraction of sampled docs containing the path (0–1). */
  presence: number;
  /** Distribution of BSON types observed for this field. Sums to 1. */
  types: Record<string, number>;
  /** Distinct value count, capped during sampling. */
  approxDistinct: number;
}

export interface SchemaReport {
  sampleSize: number;
  totalDocs: number;
  fields: SchemaField[];
}

export interface IndexKey {
  field: string;
  direction: 1 | -1 | '2d' | '2dsphere' | 'text' | 'hashed';
}

export interface IndexInfo {
  name: string;
  keys: IndexKey[];
  unique: boolean;
  sparse: boolean;
  background: boolean;
  ttlSeconds: number | null;
  partialFilter?: string;
}

export interface CreateIndexInput {
  name?: string;
  keys: IndexKey[];
  unique?: boolean;
  sparse?: boolean;
  background?: boolean;
  ttlSeconds?: number;
  partialFilter?: string;
}

export interface IndexStatRow {
  name: string;
  ops: number;
  since: string | null;
}

export interface ValidatorPayload {
  validator: string | null;     // JSON string, null if none
  validationLevel?: 'off' | 'moderate' | 'strict';
  validationAction?: 'warn' | 'error';
}

export interface ExplainRequest {
  connectionId: string;
  db: string;
  collection: string;
  filter?: string;
  projection?: string;
  sort?: string;
  skip?: number;
  limit?: number;
  verbosity?: 'queryPlanner' | 'executionStats' | 'allPlansExecution';
}

export type ExplainResponse =
  | { ok: true; explain: string }
  | { ok: false; error: string };
