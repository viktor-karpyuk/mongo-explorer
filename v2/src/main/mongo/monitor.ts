import type { Document, MongoClient } from 'mongodb';
import type { MonitorTick, SlowOp } from '../../shared/monitor.js';

function n(v: unknown): number {
  return typeof v === 'number' ? v : Number(v ?? 0);
}

export async function tick(client: MongoClient): Promise<MonitorTick> {
  const admin = client.db('admin');
  const s = (await admin.command({ serverStatus: 1 })) as Document;

  const ops = (s['opcounters'] as Document) ?? {};
  const network = (s['network'] as Document) ?? {};
  const conns = (s['connections'] as Document) ?? {};
  const mem = (s['mem'] as Document) ?? {};
  const wt = (s['wiredTiger'] as Document | undefined)?.['cache'] as Document | undefined;

  const wtUsed = n(wt?.['bytes currently in the cache']);
  const wtMax = n(wt?.['maximum bytes configured']);

  const opLatencies = ((s['opLatencies'] as Document) ?? {}) as Document;
  const readsLat = ((opLatencies['reads'] as Document) ?? {}) as Document;
  const writesLat = ((opLatencies['writes'] as Document) ?? {}) as Document;
  const cmdsLat = ((opLatencies['commands'] as Document) ?? {}) as Document;

  const globalLock = (s['globalLock'] as Document) ?? {};

  return {
    ts: Date.now(),
    uptimeSeconds: n(s['uptime']),

    opsInsert: n(ops['insert']),
    opsQuery: n(ops['query']),
    opsUpdate: n(ops['update']),
    opsDelete: n(ops['delete']),
    opsGetMore: n(ops['getmore']),
    opsCommand: n(ops['command']),

    networkInBytes: n(network['bytesIn']),
    networkOutBytes: n(network['bytesOut']),

    currentConnections: n(conns['current']),
    availableConnections: n(conns['available']),

    residentMb: n(mem['resident']),
    virtualMb: n(mem['virtual']),
    mappedMb: n(mem['mapped']),

    wtCacheBytesUsed: wtUsed,
    wtCacheBytesMax: wtMax,
    wtCachePercent: wtMax > 0 ? (wtUsed / wtMax) * 100 : 0,

    currentOps: n(((globalLock['currentQueue'] as Document) ?? {})['total']),

    latencyReadsAvgUs:
      n(readsLat['ops']) > 0 ? n(readsLat['latency']) / n(readsLat['ops']) : null,
    latencyWritesAvgUs:
      n(writesLat['ops']) > 0 ? n(writesLat['latency']) / n(writesLat['ops']) : null,
    latencyCommandsAvgUs:
      n(cmdsLat['ops']) > 0 ? n(cmdsLat['latency']) / n(cmdsLat['ops']) : null,
  };
}

export interface ProfilerLevelInput {
  level: 0 | 1 | 2;
  slowMs: number;
}

export async function setProfilerLevel(
  client: MongoClient,
  db: string,
  input: ProfilerLevelInput,
): Promise<void> {
  await client
    .db(db)
    .command({ profile: input.level, slowms: input.slowMs });
}

export async function getProfilerLevel(
  client: MongoClient,
  db: string,
): Promise<ProfilerLevelInput> {
  const r = (await client.db(db).command({ profile: -1 })) as Document;
  return {
    level: ((r['was'] as 0 | 1 | 2) ?? 0) as 0 | 1 | 2,
    slowMs: n(r['slowms']) || 100,
  };
}

export async function listSlowOps(
  client: MongoClient,
  db: string,
  limit = 50,
): Promise<SlowOp[]> {
  try {
    const rows = (await client
      .db(db)
      .collection('system.profile')
      .find({})
      .sort({ ts: -1 })
      .limit(limit)
      .toArray()) as Document[];
    return rows.map((r) => ({
      op: String(r['op'] ?? ''),
      ns: String(r['ns'] ?? ''),
      millis: n(r['millis']),
      ts: r['ts'] ? new Date(String(r['ts'])).getTime() : 0,
      client: r['client'] ? String(r['client']) : undefined,
      user: r['user'] ? String(r['user']) : undefined,
      command: r['command'] ? JSON.stringify(r['command']) : undefined,
    }));
  } catch {
    return [];
  }
}
