export interface MigrationNamespace {
  db: string;
  collection: string;
}

export interface MigrationSpec {
  sourceConnectionId: string;
  targetConnectionId: string;
  namespaces: MigrationNamespace[];
  migrateUsers?: boolean;
  /** Re-create collection on target if missing. */
  ensureCollections?: boolean;
}

export type MigrationStatus =
  | 'pending'
  | 'running'
  | 'paused'
  | 'completed'
  | 'failed';

export interface MigrationCheckpoint {
  namespaceIndex: number;
  lastId: string | null; // EJSON string
  copiedInNamespace: number;
}

export interface MigrationJob {
  id: string;
  spec: MigrationSpec;
  status: MigrationStatus;
  startedAt: number | null;
  finishedAt: number | null;
  error: string | null;
  checkpoint: MigrationCheckpoint | null;
}

export interface MigrationProgress {
  jobId: string;
  status: MigrationStatus;
  currentNamespace?: MigrationNamespace;
  copied: number;
  estimatedTotal: number | null;
  error?: string;
}

export interface PreflightResult {
  ok: boolean;
  checks: PreflightCheck[];
}

export interface PreflightCheck {
  name: string;
  ok: boolean;
  detail?: string;
}
