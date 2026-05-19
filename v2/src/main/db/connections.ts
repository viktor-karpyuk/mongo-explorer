import { ulid } from 'ulid';
import type { Db } from './store.js';
import { decryptString, encryptString } from '../secrets.js';
import type {
  ConnectionInput,
  ConnectionRecord,
  ConnectionSummary,
} from '../../shared/connection.js';

interface ConnRow {
  id: string;
  name: string;
  uri_cipher: Buffer;
  ssh_host: string | null;
  ssh_port: number | null;
  ssh_user: string | null;
  ssh_key_path: string | null;
  notes: string | null;
  created_at: number;
  updated_at: number;
  last_used_at: number | null;
}

function rowToRecord(row: ConnRow): ConnectionRecord {
  return {
    id: row.id,
    name: row.name,
    uri: decryptString(row.uri_cipher),
    ssh:
      row.ssh_host && row.ssh_port && row.ssh_user && row.ssh_key_path
        ? {
            host: row.ssh_host,
            port: row.ssh_port,
            user: row.ssh_user,
            keyPath: row.ssh_key_path,
          }
        : undefined,
    notes: row.notes ?? undefined,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastUsedAt: row.last_used_at ?? undefined,
  };
}

function rowToSummary(row: ConnRow): ConnectionSummary {
  return {
    id: row.id,
    name: row.name,
    hasSsh: !!row.ssh_host,
    notes: row.notes ?? undefined,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastUsedAt: row.last_used_at ?? undefined,
  };
}

export class ConnectionsRepo {
  constructor(private readonly db: Db) {}

  list(): ConnectionSummary[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM connections
         ORDER BY COALESCE(last_used_at, updated_at) DESC`,
      )
      .all() as ConnRow[];
    return rows.map(rowToSummary);
  }

  get(id: string): ConnectionRecord | null {
    const row = this.db
      .prepare('SELECT * FROM connections WHERE id = ?')
      .get(id) as ConnRow | undefined;
    return row ? rowToRecord(row) : null;
  }

  create(input: ConnectionInput): ConnectionSummary {
    const now = Date.now();
    const id = ulid();
    const uriCipher = encryptString(input.uri);
    this.db
      .prepare(
        `INSERT INTO connections
         (id, name, uri_cipher, ssh_host, ssh_port, ssh_user, ssh_key_path,
          notes, created_at, updated_at)
         VALUES (@id, @name, @uri_cipher, @ssh_host, @ssh_port, @ssh_user,
                 @ssh_key_path, @notes, @now, @now)`,
      )
      .run({
        id,
        name: input.name,
        uri_cipher: uriCipher,
        ssh_host: input.ssh?.host ?? null,
        ssh_port: input.ssh?.port ?? null,
        ssh_user: input.ssh?.user ?? null,
        ssh_key_path: input.ssh?.keyPath ?? null,
        notes: input.notes ?? null,
        now,
      });
    return rowToSummary({
      id,
      name: input.name,
      uri_cipher: uriCipher,
      ssh_host: input.ssh?.host ?? null,
      ssh_port: input.ssh?.port ?? null,
      ssh_user: input.ssh?.user ?? null,
      ssh_key_path: input.ssh?.keyPath ?? null,
      notes: input.notes ?? null,
      created_at: now,
      updated_at: now,
      last_used_at: null,
    });
  }

  update(id: string, input: ConnectionInput): ConnectionSummary | null {
    const existing = this.db
      .prepare('SELECT id FROM connections WHERE id = ?')
      .get(id);
    if (!existing) return null;

    const now = Date.now();
    const uriCipher = encryptString(input.uri);
    this.db
      .prepare(
        `UPDATE connections SET
           name = @name,
           uri_cipher = @uri_cipher,
           ssh_host = @ssh_host,
           ssh_port = @ssh_port,
           ssh_user = @ssh_user,
           ssh_key_path = @ssh_key_path,
           notes = @notes,
           updated_at = @now
         WHERE id = @id`,
      )
      .run({
        id,
        name: input.name,
        uri_cipher: uriCipher,
        ssh_host: input.ssh?.host ?? null,
        ssh_port: input.ssh?.port ?? null,
        ssh_user: input.ssh?.user ?? null,
        ssh_key_path: input.ssh?.keyPath ?? null,
        notes: input.notes ?? null,
        now,
      });

    const row = this.db
      .prepare('SELECT * FROM connections WHERE id = ?')
      .get(id) as ConnRow;
    return rowToSummary(row);
  }

  delete(id: string): boolean {
    const info = this.db
      .prepare('DELETE FROM connections WHERE id = ?')
      .run(id);
    return info.changes > 0;
  }

  touchLastUsed(id: string): void {
    this.db
      .prepare('UPDATE connections SET last_used_at = ? WHERE id = ?')
      .run(Date.now(), id);
  }
}
