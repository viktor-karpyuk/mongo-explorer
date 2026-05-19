export interface ShellEvent {
  sessionId: string;
  channel: 'stdout' | 'stderr' | 'exit';
  data: string;
  exitCode?: number | null;
}
