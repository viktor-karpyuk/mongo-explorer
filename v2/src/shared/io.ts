export type ExportFormat = 'json' | 'ndjson' | 'csv';
export type ImportFormat = 'json' | 'ndjson' | 'csv';

export interface ExportRequest {
  connectionId: string;
  db: string;
  collection: string;
  format: ExportFormat;
  filter?: string;
  projection?: string;
  sort?: string;
  limit?: number;
  /** Absolute target file path. */
  filePath: string;
}

export interface ImportRequest {
  connectionId: string;
  db: string;
  collection: string;
  format: ImportFormat;
  filePath: string;
  dryRun: boolean;
  ordered?: boolean;
}

export interface IoResult {
  ok: boolean;
  written?: number;
  read?: number;
  inserted?: number;
  durationMs: number;
  error?: string;
}

export interface DumpRestoreRequest {
  kind: 'dump' | 'restore';
  /** Saved connection ID used to source the URI for --uri */
  connectionId: string;
  db?: string;
  collection?: string;
  /** Source dir for restore or target dir for dump. */
  path: string;
  /** Extra binary arguments. */
  extraArgs?: string[];
}

export interface DumpRestoreEvent {
  jobId: string;
  channel: 'stdout' | 'stderr' | 'exit';
  data: string;
  exitCode?: number | null;
}
