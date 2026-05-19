import { useEffect, useRef, useState } from 'react';
import type { MonitorTick, SlowOp } from '../../../shared/monitor.js';
import type { ProfilerInput } from '../../../shared/ipc.js';
import { useConnectionsStore } from '../store/connections';
import { Sparkline } from './Sparkline';
import { formatBytes, formatCount } from '../utils/format';

interface Props {
  connectionId: string;
}

const HISTORY_LEN = 60;

interface Series {
  ops: number[];
  netIn: number[];
  netOut: number[];
  conns: number[];
  cachePct: number[];
  latReads: number[];
  latWrites: number[];
}

function emptySeries(): Series {
  return { ops: [], netIn: [], netOut: [], conns: [], cachePct: [], latReads: [], latWrites: [] };
}

function push(arr: number[], v: number): number[] {
  const next = arr.length >= HISTORY_LEN ? arr.slice(1) : arr.slice();
  next.push(v);
  return next;
}

export function MonitoringPanel({ connectionId }: Props) {
  const state = useConnectionsStore((s) => s.states[connectionId]);
  const [series, setSeries] = useState<Series>(emptySeries());
  const [latest, setLatest] = useState<MonitorTick | null>(null);
  const [error, setError] = useState<string | null>(null);
  const prevTick = useRef<MonitorTick | null>(null);

  useEffect(() => {
    if (state?.kind !== 'connected') return;
    let cancelled = false;

    async function poll() {
      try {
        const tick = await window.mex.monitor.tick(connectionId);
        if (cancelled) return;
        setError(null);

        const prev = prevTick.current;
        prevTick.current = tick;
        setLatest(tick);

        if (prev) {
          const dtSec = Math.max(1, (tick.ts - prev.ts) / 1000);
          const opsTotal =
            tick.opsInsert + tick.opsQuery + tick.opsUpdate +
            tick.opsDelete + tick.opsGetMore + tick.opsCommand;
          const prevOps =
            prev.opsInsert + prev.opsQuery + prev.opsUpdate +
            prev.opsDelete + prev.opsGetMore + prev.opsCommand;
          const opsRate = Math.max(0, (opsTotal - prevOps) / dtSec);
          const netInRate = Math.max(0, (tick.networkInBytes - prev.networkInBytes) / dtSec);
          const netOutRate = Math.max(0, (tick.networkOutBytes - prev.networkOutBytes) / dtSec);

          setSeries((s) => ({
            ops: push(s.ops, opsRate),
            netIn: push(s.netIn, netInRate),
            netOut: push(s.netOut, netOutRate),
            conns: push(s.conns, tick.currentConnections),
            cachePct: push(s.cachePct, tick.wtCachePercent),
            latReads: push(s.latReads, tick.latencyReadsAvgUs ?? 0),
            latWrites: push(s.latWrites, tick.latencyWritesAvgUs ?? 0),
          }));
        }
      } catch (e: unknown) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    }

    void poll();
    const id = window.setInterval(() => void poll(), 2000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [connectionId, state?.kind]);

  if (state?.kind !== 'connected') {
    return (
      <div className="view">
        <div className="alert alert--error">
          Connection must be open to monitor.
        </div>
      </div>
    );
  }

  const opsRate = series.ops.at(-1) ?? 0;
  const netInRate = series.netIn.at(-1) ?? 0;
  const netOutRate = series.netOut.at(-1) ?? 0;

  return (
    <div className="view">
      <header className="view__header">
        <h1>Monitoring</h1>
        <span className="muted">2 s refresh · last 2 min</span>
      </header>

      {error && <div className="alert alert--error">{error}</div>}

      <div className="metric-grid">
        <Metric
          label="Operations / sec"
          value={formatCount(Math.round(opsRate))}
          spark={series.ops}
          color="#4ea1ff"
        />
        <Metric
          label="Network in / sec"
          value={`${formatBytes(netInRate)}`}
          spark={series.netIn}
          color="#34d399"
        />
        <Metric
          label="Network out / sec"
          value={`${formatBytes(netOutRate)}`}
          spark={series.netOut}
          color="#a78bfa"
        />
        <Metric
          label="Active connections"
          value={`${latest?.currentConnections ?? 0} / ${(latest?.currentConnections ?? 0) + (latest?.availableConnections ?? 0)}`}
          spark={series.conns}
          color="#fbbf24"
        />
        <Metric
          label="WT cache used"
          value={`${(latest?.wtCachePercent ?? 0).toFixed(1)}%`}
          spark={series.cachePct}
          color="#f472b6"
        />
        <Metric
          label="Avg read latency"
          value={
            latest?.latencyReadsAvgUs != null
              ? `${(latest.latencyReadsAvgUs / 1000).toFixed(2)} ms`
              : '—'
          }
          spark={series.latReads}
          color="#60a5fa"
        />
        <Metric
          label="Avg write latency"
          value={
            latest?.latencyWritesAvgUs != null
              ? `${(latest.latencyWritesAvgUs / 1000).toFixed(2)} ms`
              : '—'
          }
          spark={series.latWrites}
          color="#f87171"
        />
        <Metric
          label="Resident memory"
          value={`${latest?.residentMb ?? 0} MB`}
          spark={[]}
          color="#94a3b8"
        />
      </div>

      <ProfilerSection connectionId={connectionId} />
    </div>
  );
}

function Metric({
  label,
  value,
  spark,
  color,
}: {
  label: string;
  value: string;
  spark: number[];
  color: string;
}) {
  return (
    <article className="metric">
      <div className="metric__label">{label}</div>
      <div className="metric__value">{value}</div>
      <Sparkline values={spark} color={color} width={160} height={32} />
    </article>
  );
}

function ProfilerSection({ connectionId }: { connectionId: string }) {
  const [db, setDb] = useState('admin');
  const [profile, setProfile] = useState<ProfilerInput>({ level: 0, slowMs: 100 });
  const [slow, setSlow] = useState<SlowOp[]>([]);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    setError(null);
    try {
      const [p, ops] = await Promise.all([
        window.mex.monitor.profileGet(connectionId, db),
        window.mex.monitor.slowOps(connectionId, db, 50),
      ]);
      setProfile(p);
      setSlow(ops);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function apply() {
    try {
      await window.mex.monitor.profileSet(connectionId, db, profile);
      await load();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <section className="panel">
      <div className="panel__head">
        <h2>Profiler / slow ops</h2>
      </div>

      <div className="profiler-controls">
        <label className="inline-field">
          <span>Database</span>
          <input value={db} onChange={(e) => setDb(e.target.value)} />
        </label>
        <label className="inline-field">
          <span>Level</span>
          <select
            value={profile.level}
            onChange={(e) =>
              setProfile({ ...profile, level: Number(e.target.value) as 0 | 1 | 2 })
            }
          >
            <option value={0}>0 — off</option>
            <option value={1}>1 — slow only</option>
            <option value={2}>2 — all</option>
          </select>
        </label>
        <label className="inline-field">
          <span>slowMs</span>
          <input
            type="number"
            min={0}
            value={profile.slowMs}
            onChange={(e) =>
              setProfile({ ...profile, slowMs: Number(e.target.value) || 100 })
            }
          />
        </label>
        <button className="btn btn--ghost" onClick={() => void load()}>
          Load
        </button>
        <button className="btn btn--primary" onClick={() => void apply()}>
          Apply
        </button>
      </div>

      {error && <div className="alert alert--error">{error}</div>}

      {slow.length > 0 && (
        <table className="slow-ops">
          <thead>
            <tr>
              <th>Op</th>
              <th>Namespace</th>
              <th>ms</th>
              <th>When</th>
              <th>Command</th>
            </tr>
          </thead>
          <tbody>
            {slow.map((op, i) => (
              <tr key={i}>
                <td>{op.op}</td>
                <td className="slow-ops__ns">{op.ns}</td>
                <td className="slow-ops__ms">{op.millis}</td>
                <td className="muted">{new Date(op.ts).toLocaleTimeString()}</td>
                <td className="slow-ops__cmd">{op.command}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
