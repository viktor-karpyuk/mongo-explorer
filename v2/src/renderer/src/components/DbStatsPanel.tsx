import { useEffect, useState } from 'react';
import type { DbStats } from '../../../shared/namespace.js';
import { formatBytes, formatCount } from '../utils/format';

interface Props {
  connectionId: string;
  db: string;
}

export function DbStatsPanel({ connectionId, db }: Props) {
  const [stats, setStats] = useState<DbStats | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setStats(null);
    setError(null);
    window.mex.ns
      .dbStats(connectionId, db)
      .then(setStats)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [connectionId, db]);

  return (
    <div className="view">
      <header className="view__header">
        <h1>{db}</h1>
      </header>

      {error && <div className="alert alert--error">{error}</div>}
      {!stats && !error && <div className="muted">Loading…</div>}

      {stats && (
        <>
          <div className="stats-grid">
            <Stat label="Collections" value={formatCount(stats.collections)} />
            <Stat label="Views" value={formatCount(stats.views)} />
            <Stat label="Documents" value={formatCount(stats.objects)} />
            <Stat label="Avg doc size" value={formatBytes(stats.avgObjSize)} />
            <Stat label="Data size" value={formatBytes(stats.dataSize)} />
            <Stat label="Storage size" value={formatBytes(stats.storageSize)} />
            <Stat label="Indexes" value={formatCount(stats.indexes)} />
            <Stat label="Index size" value={formatBytes(stats.indexSize)} />
          </div>

          <section className="panel">
            <h2>Size breakdown</h2>
            <SizeBars
              items={[
                { label: 'Data', bytes: stats.dataSize, color: '#4ea1ff' },
                { label: 'Indexes', bytes: stats.indexSize, color: '#a78bfa' },
              ]}
              total={stats.totalSize}
            />
          </section>
        </>
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

interface SizeBarItem {
  label: string;
  bytes: number;
  color: string;
}

function SizeBars({ items, total }: { items: SizeBarItem[]; total: number }) {
  const max = Math.max(total, ...items.map((i) => i.bytes), 1);
  return (
    <div className="size-bars">
      {items.map((i) => {
        const pct = (i.bytes / max) * 100;
        return (
          <div key={i.label} className="size-bar">
            <span className="size-bar__label">{i.label}</span>
            <div className="size-bar__track">
              <div
                className="size-bar__fill"
                style={{ width: `${pct}%`, background: i.color }}
              />
            </div>
            <span className="size-bar__value">{formatBytes(i.bytes)}</span>
          </div>
        );
      })}
    </div>
  );
}
