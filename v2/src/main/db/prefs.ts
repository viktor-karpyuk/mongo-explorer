import type { Db } from './store.js';

export class PrefsRepo {
  constructor(private readonly db: Db) {}

  get(key: string): string | null {
    const row = this.db.prepare('SELECT value FROM prefs WHERE key = ?').get(
      key,
    ) as { value: string } | undefined;
    return row?.value ?? null;
  }

  set(key: string, value: string): void {
    this.db
      .prepare(
        `INSERT INTO prefs (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      )
      .run(key, value);
  }

  delete(key: string): void {
    this.db.prepare('DELETE FROM prefs WHERE key = ?').run(key);
  }
}
