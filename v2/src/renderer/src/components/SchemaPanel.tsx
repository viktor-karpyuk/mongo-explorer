import { useEffect, useState } from 'react';
import type { SchemaReport } from '../../../shared/schema.js';
import { formatCount } from '../utils/format';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
}

const TYPE_COLORS: Record<string, string> = {
  string: '#60a5fa',
  number: '#fbbf24',
  boolean: '#f472b6',
  null: '#6b7280',
  array: '#a78bfa',
  object: '#94a3b8',
  objectid: '#34d399',
  date: '#a78bfa',
  long: '#60a5fa',
  decimal: '#60a5fa',
  binary: '#f472b6',
  regex: '#f472b6',
  timestamp: '#a78bfa',
  minkey: '#6b7280',
  maxkey: '#6b7280',
};

export function SchemaPanel({ connectionId, db, collection }: Props) {
  const [report, setReport] = useState<SchemaReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [sampleSize, setSampleSize] = useState(1000);

  async function run() {
    setLoading(true);
    setError(null);
    try {
      const r = await window.mex.schema.analyze(connectionId, db, collection, sampleSize);
      setReport(r);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setReport(null);
    void run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectionId, db, collection]);

  return (
    <div className="view">
      <header className="view__header">
        <h1>Schema · {db}.{collection}</h1>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <label className="inline-field">
            <span>Sample size</span>
            <input
              type="number"
              min={10}
              max={100000}
              value={sampleSize}
              onChange={(e) => setSampleSize(Number(e.target.value) || 1000)}
            />
          </label>
          <button className="btn btn--primary" onClick={() => void run()} disabled={loading}>
            {loading ? 'Sampling…' : 'Re-sample'}
          </button>
        </div>
      </header>

      {error && <div className="alert alert--error">{error}</div>}

      {report && (
        <>
          <div className="muted" style={{ marginBottom: 12 }}>
            Analyzed {formatCount(report.sampleSize)} documents of ~
            {formatCount(report.totalDocs)} total.
          </div>
          <table className="schema-table">
            <thead>
              <tr>
                <th>Field</th>
                <th>Presence</th>
                <th>Types</th>
                <th>Approx distinct</th>
              </tr>
            </thead>
            <tbody>
              {report.fields.map((f) => (
                <tr key={f.path}>
                  <td className="schema-field">{f.path}</td>
                  <td>
                    <div className="presence-bar">
                      <div
                        className="presence-bar__fill"
                        style={{ width: `${Math.round(f.presence * 100)}%` }}
                      />
                      <span>{Math.round(f.presence * 100)}%</span>
                    </div>
                  </td>
                  <td>
                    <div className="type-bar">
                      {Object.entries(f.types).map(([type, frac]) => (
                        <div
                          key={type}
                          className="type-bar__seg"
                          title={`${type}: ${Math.round(frac * 100)}%`}
                          style={{
                            width: `${Math.round(frac * 100)}%`,
                            background: TYPE_COLORS[type] ?? '#666',
                          }}
                        />
                      ))}
                    </div>
                    <div className="type-bar__legend">
                      {Object.keys(f.types).join(' · ')}
                    </div>
                  </td>
                  <td className="muted">
                    {f.approxDistinct >= 64 ? '64+' : f.approxDistinct}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
