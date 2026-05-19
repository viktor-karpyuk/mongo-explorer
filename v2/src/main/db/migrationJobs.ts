import { ulid } from 'ulid';
import type { Db } from './store.js';
import type {
  MigrationCheckpoint,
  MigrationJob,
  MigrationSpec,
  MigrationStatus,
} from '../../shared/migration.js';

interface JobRow {
  id: string;
  source_conn_id: string;
  target_conn_id: string;
  spec: string;
  status: MigrationStatus;
  pid: number | null;
  heartbeat_at: number | null;
  started_at: number | null;
  finished_at: number | null;
  checkpoint: string | null;
  error: string | null;
}

function rowToJob(row: JobRow): MigrationJob {
  return {
    id: row.id,
    spec: JSON.parse(row.spec) as MigrationSpec,
    status: row.status,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    error: row.error,
    checkpoint: row.checkpoint ? (JSON.parse(row.checkpoint) as MigrationCheckpoint) : null,
  };
}

export class MigrationJobsRepo {
  constructor(private readonly db: Db) {}

  /** On boot: any job that was running is now an orphan; mark FAILED. */
  reconcileOrphans(): void {
    this.db
      .prepare(
        `UPDATE migration_jobs SET status = 'failed', error = 'orphaned at restart'
         WHERE status IN ('running','paused') AND heartbeat_at IS NOT NULL
           AND heartbeat_at < ?`,
      )
      .run(Date.now() - 60_000);
  }

  list(): MigrationJob[] {
    const rows = this.db.prepare('SELECT * FROM migration_jobs ORDER BY id DESC').all() as JobRow[];
    return rows.map(rowToJob);
  }

  get(id: string): MigrationJob | null {
    const row = this.db.prepare('SELECT * FROM migration_jobs WHERE id = ?').get(id) as
      | JobRow
      | undefined;
    return row ? rowToJob(row) : null;
  }

  create(spec: MigrationSpec): MigrationJob {
    const id = ulid();
    this.db
      .prepare(
        `INSERT INTO migration_jobs (id, source_conn_id, target_conn_id, spec, status)
         VALUES (?, ?, ?, ?, 'pending')`,
      )
      .run(id, spec.sourceConnectionId, spec.targetConnectionId, JSON.stringify(spec));
    return this.get(id)!;
  }

  setStatus(id: string, status: MigrationStatus, error?: string): void {
    this.db
      .prepare(
        `UPDATE migration_jobs SET status = ?,
           started_at = COALESCE(started_at, CASE WHEN ? = 'running' THEN ? ELSE started_at END),
           finished_at = CASE WHEN ? IN ('completed','failed') THEN ? ELSE finished_at END,
           error = COALESCE(?, error),
           heartbeat_at = ?
         WHERE id = ?`,
      )
      .run(status, status, Date.now(), status, Date.now(), error ?? null, Date.now(), id);
  }

  setCheckpoint(id: string, checkpoint: MigrationCheckpoint): void {
    this.db
      .prepare(
        'UPDATE migration_jobs SET checkpoint = ?, heartbeat_at = ? WHERE id = ?',
      )
      .run(JSON.stringify(checkpoint), Date.now(), id);
  }

  delete(id: string): void {
    this.db.prepare('DELETE FROM migration_jobs WHERE id = ?').run(id);
  }
}
