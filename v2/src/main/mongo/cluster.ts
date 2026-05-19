import { EJSON } from 'bson';
import type { Document, MongoClient } from 'mongodb';
import type {
  ClusterSnapshot,
  HealthReport,
  MemberInfo,
  MemberState,
  TopologyInfo,
  TopologyType,
} from '../../shared/cluster.js';

const STATE_BY_CODE: Record<number, MemberState> = {
  0: 'STARTUP',
  1: 'PRIMARY',
  2: 'SECONDARY',
  3: 'RECOVERING',
  5: 'STARTUP2',
  6: 'UNKNOWN',
  7: 'ARBITER',
  8: 'DOWN',
  9: 'ROLLBACK',
  10: 'REMOVED',
};

export async function clusterSnapshot(client: MongoClient): Promise<ClusterSnapshot> {
  const admin = client.db('admin');
  const hello = (await admin.command({ hello: 1 })) as Document;

  let type: TopologyType = 'unknown';
  let setName: string | undefined;
  let primary: string | undefined;
  const members: MemberInfo[] = [];
  let rsConfigStr: string | null = null;

  if (hello['msg'] === 'isdbgrid') {
    type = 'sharded';
  } else if (typeof hello['setName'] === 'string') {
    type = 'replicaset';
    setName = String(hello['setName']);
    primary = (hello['primary'] as string | undefined) ?? undefined;
    try {
      const status = (await admin.command({ replSetGetStatus: 1 })) as Document;
      const priorities = await collectPriorities(admin);
      for (const m of (status['members'] as Document[]) ?? []) {
        members.push(toMember(m, priorities));
      }
    } catch {
      /* not authorized or not on a replica set */
    }
    try {
      const cfg = (await admin.command({ replSetGetConfig: 1 })) as Document;
      rsConfigStr = EJSON.stringify(cfg['config'] ?? cfg, { relaxed: false });
    } catch {
      /* swallow */
    }
  } else {
    type = 'standalone';
  }

  const topology: TopologyInfo = { type, setName, primary, members };
  const health = scoreHealth(topology);
  return { topology, health, rsConfig: rsConfigStr };
}

async function collectPriorities(adminDb: { command(cmd: Document): Promise<Document> }) {
  try {
    const cfg = await adminDb.command({ replSetGetConfig: 1 });
    const map = new Map<string, { priority: number; votes: number }>();
    for (const m of ((cfg['config'] as Document | undefined)?.['members'] as Document[]) ?? []) {
      map.set(String(m['host']), {
        priority: Number(m['priority'] ?? 1),
        votes: Number(m['votes'] ?? 1),
      });
    }
    return map;
  } catch {
    return new Map<string, { priority: number; votes: number }>();
  }
}

function toMember(m: Document, prio: Map<string, { priority: number; votes: number }>): MemberInfo {
  const stateCode = Number(m['state'] ?? 6);
  return {
    name: String(m['name']),
    state: STATE_BY_CODE[stateCode] ?? 'UNKNOWN',
    stateCode,
    health: (m['health'] === 1 ? 1 : 0) as 0 | 1,
    uptime: Number(m['uptime'] ?? 0),
    pingMs: m['pingMs'] != null ? Number(m['pingMs']) : null,
    lagSeconds: computeLag(m),
    priority: prio.get(String(m['name']))?.priority ?? 1,
    votes: prio.get(String(m['name']))?.votes ?? 1,
  };
}

function computeLag(m: Document): number | null {
  const opTime = (m['optimeDate'] as Date | undefined) ?? null;
  if (!opTime) return null;
  return Math.max(0, Math.round((Date.now() - new Date(opTime).getTime()) / 1000));
}

export function scoreHealth(topology: TopologyInfo): HealthReport {
  const reasons: string[] = [];
  let score = 100;

  if (topology.type === 'standalone') {
    return { score, reasons: ['Standalone instance — no replication metrics.'] };
  }

  if (topology.type === 'sharded') {
    return { score, reasons: ['Sharded cluster — open the cluster tab on individual shards for details.'] };
  }

  if (topology.members.length === 0) {
    return { score: 50, reasons: ['Could not read replica set status (auth or unsupported).'] };
  }

  const hasPrimary = topology.members.some((m) => m.state === 'PRIMARY');
  if (!hasPrimary) {
    score -= 50;
    reasons.push('No primary elected');
  }

  for (const m of topology.members) {
    if (m.health !== 1 || m.state === 'DOWN') {
      score -= 30;
      reasons.push(`${m.name} is ${m.state} / unhealthy`);
    }
    if (m.lagSeconds != null && m.lagSeconds > 10 && m.state === 'SECONDARY') {
      score -= 15;
      reasons.push(`${m.name} lag ${m.lagSeconds}s`);
    }
  }

  score = Math.max(0, Math.min(100, score));
  return { score, reasons };
}
