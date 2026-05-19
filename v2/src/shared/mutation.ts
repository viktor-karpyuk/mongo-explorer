export interface MutateBase {
  connectionId: string;
  db: string;
  collection: string;
}

export interface InsertOneInput extends MutateBase {
  document: string; // EJSON / shell-flavored JSON
}

export interface InsertManyInput extends MutateBase {
  /** Either a JSON array, or NDJSON (one document per line). */
  documents: string;
  ordered?: boolean;
}

export interface UpdateInput extends MutateBase {
  filter: string;
  update: string;
  upsert?: boolean;
  many?: boolean;
}

export interface ReplaceInput extends MutateBase {
  filter: string;
  replacement: string;
  upsert?: boolean;
}

export interface DeleteInput extends MutateBase {
  filter: string;
  many?: boolean;
}

export type MutateResponse =
  | {
      ok: true;
      matched?: number;
      modified?: number;
      inserted?: number;
      deleted?: number;
      upsertedIds?: string[];
      durationMs: number;
    }
  | { ok: false; error: string; durationMs: number };

export interface BulkOp {
  kind: 'insertOne' | 'updateOne' | 'updateMany' | 'replaceOne' | 'deleteOne' | 'deleteMany';
  /** JSON body per op kind. */
  body: string;
}

export interface BulkWriteInput extends MutateBase {
  ops: BulkOp[];
  ordered?: boolean;
}
