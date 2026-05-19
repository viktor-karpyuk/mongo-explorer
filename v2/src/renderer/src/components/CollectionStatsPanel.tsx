import { useEffect, useState } from 'react';
import type { CollStats } from '../../../shared/namespace.js';
import { formatBytes, formatCount } from '../utils/format';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
}

export function CollectionStatsPanel({ connectionId, db, collection }: Props) {
  const [stats, setStats] = useState<CollStats | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setStats(null);
    setError(null);
    window.mex.ns
      .collStats(connectionId, db, collection)
      .then(setStats)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [connectionId, db, collection]);

  return (
    <div className="view">
      <header className="view__header">
        <h1>
          <span className="muted">{db} ›</span> {collection}
        </h1>
      </header>

      {error && <div className="alert alert--error">{error}</div>}
      {!stats && !error && <div className="muted">Loading…</div>}

      {stats && (
        <div className="stats-grid">
          <Stat label="Documents" value={formatCount(stats.count)} />
          <Stat label="Avg doc size" value={formatBytes(stats.avgObjSize)} />
          <Stat label="Data size" value={formatBytes(stats.size)} />
          <Stat label="Storage size" value={formatBytes(stats.storageSize)} />
          <Stat label="Indexes" value={formatCount(stats.nindexes)} />
          <Stat label="Total index size" value={formatBytes(stats.totalIndexSize)} />
          <Stat label="Capped" value={stats.capped ? 'yes' : 'no'} />
          <Stat label="Namespace" value={stats.ns} />
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat">
      <span className="stat__label">{label}</span>
      <span className="stat__value">{value}</span>
    </div>
  );
}
