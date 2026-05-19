import Database from 'better-sqlite3';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { migrations } from './migrations.js';

export type Db = Database.Database;

export function openStore(filePath: string): Db {
  mkdirSync(dirname(filePath), { recursive: true });

  const db = new Database(filePath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.pragma('synchronous = NORMAL');

  db.exec(`
    CREATE TABLE IF NOT EXISTS schema_version (
      version    INTEGER PRIMARY KEY,
      applied_at INTEGER NOT NULL
    );
  `);

  applyMigrations(db);
  return db;
}

function applyMigrations(db: Db): void {
  const row = db.prepare('SELECT MAX(version) AS v FROM schema_version').get() as
    | { v: number | null }
    | undefined;
  const current = row?.v ?? 0;

  const pending = migrations.filter((m) => m.version > current);
  if (pending.length === 0) return;

  const insertVersion = db.prepare(
    'INSERT INTO schema_version (version, applied_at) VALUES (?, ?)',
  );

  const apply = db.transaction(() => {
    for (const m of pending) {
      db.exec(m.sql);
      insertVersion.run(m.version, Date.now());
    }
  });
  apply();
}
