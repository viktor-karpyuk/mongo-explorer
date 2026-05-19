import type { Db } from './store.js';

export type QueryKind = 'find' | 'aggregate' | 'command';

export interface QueryHistoryRow {
  id: number;
  connectionId: string;
  database: string;
  collection: string;
  kind: QueryKind;
  body: string;
  durationMs: number | null;
  rowCount: number | null;
  error: string | null;
  ranAt: number;
}

export interface QueryHistoryInput {
  connectionId: string;
  database: string;
  collection: string;
  kind: QueryKind;
  body: string;
  durationMs?: number;
  rowCount?: number;
  error?: string;
}

interface Row {
  id: number;
  connection_id: string;
  database_name: string;
  collection: string;
  kind: QueryKind;
  body: string;
  duration_ms: number | null;
  row_count: number | null;
  error: string | null;
  ran_at: number;
}

function rowToRecord(row: Row): QueryHistoryRow {
  return {
    id: row.id,
    connectionId: row.connection_id,
    database: row.database_name,
    collection: row.collection,
    kind: row.kind,
    body: row.body,
    durationMs: row.duration_ms,
    rowCount: row.row_count,
    error: row.error,
    ranAt: row.ran_at,
  };
}

const MAX_PER_CONNECTION = 100;

export class QueryHistoryRepo {
  constructor(private readonly db: Db) {}

  record(input: QueryHistoryInput): QueryHistoryRow {
    const ranAt = Date.now();
    const info = this.db
      .prepare(
        `INSERT INTO query_history
         (connection_id, database_name, collection, kind, body,
          duration_ms, row_count, error, ran_at)
         VALUES (@cid, @db, @col, @kind, @body, @dur, @rc, @err, @ran)`,
      )
      .run({
        cid: input.connectionId,
        db: input.database,
        col: input.collection,
        kind: input.kind,
        body: input.body,
        dur: input.durationMs ?? null,
        rc: input.rowCount ?? null,
        err: input.error ?? null,
        ran: ranAt,
      });

    this.trim(input.connectionId);

    const row = this.db
      .prepare('SELECT * FROM query_history WHERE id = ?')
      .get(info.lastInsertRowid) as Row;
    return rowToRecord(row);
  }

  list(connectionId: string, limit = 100): QueryHistoryRow[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM query_history
         WHERE connection_id = ?
         ORDER BY ran_at DESC
         LIMIT ?`,
      )
      .all(connectionId, limit) as Row[];
    return rows.map(rowToRecord);
  }

  private trim(connectionId: string): void {
    this.db
      .prepare(
        `DELETE FROM query_history
         WHERE connection_id = ?
           AND id NOT IN (
             SELECT id FROM query_history
             WHERE connection_id = ?
             ORDER BY ran_at DESC
             LIMIT ?
           )`,
      )
      .run(connectionId, connectionId, MAX_PER_CONNECTION);
  }
}
