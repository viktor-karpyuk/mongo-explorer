import { useEffect, useState } from 'react';
import type { ClusterSnapshot, MemberInfo } from '../../../shared/cluster.js';
import { useConnectionsStore } from '../store/connections';

interface Props {
  connectionId: string;
}

export function ClusterPanel({ connectionId }: Props) {
  const conn = useConnectionsStore((s) => s.list.find((c) => c.id === connectionId));
  const state = useConnectionsStore((s) => s.states[connectionId]);

  const [snap, setSnap] = useState<ClusterSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function load() {
    if (state?.kind !== 'connected') return;
    setLoading(true);
    setError(null);
    try {
      const s = await window.mex.cluster.snapshot(connectionId);
      setSnap(s);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setSnap(null);
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectionId, state?.kind]);

  return (
    <div className="view">
      <header className="view__header">
        <h1>{conn?.name ?? 'Cluster'}</h1>
        <button className="btn btn--ghost" onClick={() => void load()} disabled={loading}>
          {loading ? 'Refreshing…' : 'Refresh'}
        </button>
      </header>

      {state?.kind !== 'connected' && (
        <div className="alert alert--error">
          Open this connection to view cluster details.
        </div>
      )}
      {error && <div className="alert alert--error">{error}</div>}

      {snap && (
        <>
          <section className="panel">
            <div className="health-row">
              <div className="health-pill" data-score={scoreBand(snap.health.score)}>
                <span className="health-pill__value">{snap.health.score}</span>
                <span className="health-pill__label">health</span>
              </div>
              <div className="topology-summary">
                <div>
                  <span className="muted">Type</span>
                  <strong>{snap.topology.type}</strong>
                </div>
                {snap.topology.setName && (
                  <div>
                    <span className="muted">Replica set</span>
                    <strong>{snap.topology.setName}</strong>
                  </div>
                )}
                {snap.topology.primary && (
                  <div>
                    <span className="muted">Primary</span>
                    <strong>{snap.topology.primary}</strong>
                  </div>
                )}
              </div>
            </div>
            {snap.health.reasons.length > 0 && (
              <ul className="health-reasons">
                {snap.health.reasons.map((r, i) => (
                  <li key={i}>{r}</li>
                ))}
              </ul>
            )}
          </section>

          {snap.topology.members.length > 0 && (
            <section className="panel">
              <h2>Members</h2>
              <div className="member-grid">
                {snap.topology.members.map((m) => (
                  <MemberCard key={m.name} member={m} />
                ))}
              </div>
            </section>
          )}

          {snap.rsConfig && (
            <section className="panel">
              <div className="panel__head">
                <h2>rs.conf()</h2>
                <button
                  className="btn btn--ghost btn--xs"
                  onClick={() => {
                    void navigator.clipboard.writeText(prettyJson(snap.rsConfig!));
                  }}
                >
                  Copy
                </button>
              </div>
              <pre className="rs-config">{prettyJson(snap.rsConfig)}</pre>
            </section>
          )}
        </>
      )}
    </div>
  );
}

function MemberCard({ member }: { member: MemberInfo }) {
  return (
    <article className={`member member--${member.state.toLowerCase()}`}>
      <header>
        <span className="member__state">{member.state}</span>
        {member.health === 1 ? (
          <span className="dot dot--ok" />
        ) : (
          <span className="dot dot--bad" />
        )}
      </header>
      <h3>{member.name}</h3>
      <dl>
        {member.pingMs != null && (
          <>
            <dt>Ping</dt>
            <dd>{member.pingMs} ms</dd>
          </>
        )}
        {member.lagSeconds != null && (
          <>
            <dt>Lag</dt>
            <dd>{member.lagSeconds}s</dd>
          </>
        )}
        <dt>Uptime</dt>
        <dd>{formatUptime(member.uptime)}</dd>
        <dt>Priority</dt>
        <dd>{member.priority}</dd>
        <dt>Votes</dt>
        <dd>{member.votes}</dd>
      </dl>
    </article>
  );
}

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}

function scoreBand(score: number): 'good' | 'warn' | 'bad' {
  if (score >= 80) return 'good';
  if (score >= 50) return 'warn';
  return 'bad';
}

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}
