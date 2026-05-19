export interface MonitorTick {
  ts: number;
  uptimeSeconds: number;

  opsInsert: number;
  opsQuery: number;
  opsUpdate: number;
  opsDelete: number;
  opsGetMore: number;
  opsCommand: number;

  networkInBytes: number;
  networkOutBytes: number;

  currentConnections: number;
  availableConnections: number;

  residentMb: number;
  virtualMb: number;
  mappedMb: number;

  wtCacheBytesUsed: number;
  wtCacheBytesMax: number;
  wtCachePercent: number;

  currentOps: number;
  latencyReadsAvgUs: number | null;
  latencyWritesAvgUs: number | null;
  latencyCommandsAvgUs: number | null;
}

export interface SlowOp {
  op: string;
  ns: string;
  millis: number;
  ts: number;
  client?: string;
  user?: string;
  command?: string;
}
