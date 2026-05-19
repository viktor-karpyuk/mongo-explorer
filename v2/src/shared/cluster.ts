export type MemberState =
  | 'PRIMARY'
  | 'SECONDARY'
  | 'RECOVERING'
  | 'STARTUP'
  | 'STARTUP2'
  | 'ARBITER'
  | 'DOWN'
  | 'ROLLBACK'
  | 'REMOVED'
  | 'UNKNOWN';

export interface MemberInfo {
  name: string;
  state: MemberState;
  stateCode: number;
  health: 0 | 1;
  uptime: number;
  pingMs: number | null;
  lagSeconds: number | null;
  priority: number;
  votes: number;
}

export type TopologyType = 'standalone' | 'replicaset' | 'sharded' | 'unknown';

export interface TopologyInfo {
  type: TopologyType;
  setName?: string;
  primary?: string;
  members: MemberInfo[];
}

export interface HealthReport {
  score: number;            // 0–100
  reasons: string[];        // Each subtracted contributor
}

export interface ClusterSnapshot {
  topology: TopologyInfo;
  health: HealthReport;
  /** rs.conf() as canonical EJSON string when on a replica set. */
  rsConfig: string | null;
}
