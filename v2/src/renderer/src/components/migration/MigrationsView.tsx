import { useEffect, useState } from 'react';
import { useMigrationsStore } from '../../store/migrations';
import { useConnectionsStore } from '../../store/connections';
import { NewMigrationDialog } from './NewMigrationDialog';
import { formatCount } from '../../utils/format';

export function MigrationsView() {
  const jobs = useMigrationsStore((s) => s.jobs);
  const progress = useMigrationsStore((s) => s.progress);
  const load = useMigrationsStore((s) => s.load);
  const start = useMigrationsStore((s) => s.start);
  const pause = useMigrationsStore((s) => s.pause);
  const cancel = useMigrationsStore((s) => s.cancel);
  const remove = useMigrationsStore((s) => s.remove);
  const createJob = useMigrationsStore((s) => s.create);

  const connections = useConnectionsStore((s) => s.list);
  const connName = (id: string) =>
    connections.find((c) => c.id === id)?.name ?? '(unknown)';

  const [showNew, setShowNew] = useState(false);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div className="view">
      <header className="view__header">
        <h1>Migrations</h1>
        <button className="btn btn--primary" onClick={() => setShowNew(true)}>
          + New migration
        </button>
      </header>

      {jobs.length === 0 ? (
        <div className="empty">
          <h2>No migrations yet</h2>
          <p>Migrate data between two open connections, collection-by-collection.</p>
        </div>
      ) : (
        <table className="migrations-table">
          <thead>
            <tr>
              <th>Status</th>
              <th>Source → Target</th>
              <th>Namespaces</th>
              <th>Progress</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => {
              const prog = progress[job.id];
              return (
                <tr key={job.id}>
                  <td>
                    <span className={`pill pill--${pillFor(job.status)}`}>
                      {job.status}
                    </span>
                  </td>
                  <td className="muted">
                    {connName(job.spec.sourceConnectionId)} →{' '}
                    {connName(job.spec.targetConnectionId)}
                  </td>
                  <td className="muted">{job.spec.namespaces.length}</td>
                  <td>
                    {prog ? (
                      <>
                        {prog.currentNamespace && (
                          <div className="muted" style={{ fontSize: 11 }}>
                            {prog.currentNamespace.db}.{prog.currentNamespace.collection}
                          </div>
                        )}
                        <div>
                          {formatCount(prog.copied)}
                          {prog.estimatedTotal ? ` / ~${formatCount(prog.estimatedTotal)}` : ''}
                        </div>
                      </>
                    ) : (
                      <span className="muted">—</span>
                    )}
                  </td>
                  <td>
                    {job.status === 'pending' || job.status === 'paused' ? (
                      <button
                        className="btn btn--primary btn--xs"
                        onClick={() => void start(job.id)}
                      >
                        Start
                      </button>
                    ) : job.status === 'running' ? (
                      <button
                        className="btn btn--ghost btn--xs"
                        onClick={() => void pause(job.id)}
                      >
                        Pause
                      </button>
                    ) : null}
                    {job.status === 'running' && (
                      <button
                        className="btn btn--ghost btn--xs btn--danger"
                        onClick={() => void cancel(job.id)}
                      >
                        Cancel
                      </button>
                    )}
                    {job.status !== 'running' && (
                      <button
                        className="btn btn--ghost btn--xs btn--danger"
                        onClick={() => void remove(job.id)}
                      >
                        Delete
                      </button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      {showNew && (
        <NewMigrationDialog
          onClose={() => setShowNew(false)}
          onCreate={async (spec) => {
            await createJob(spec);
            setShowNew(false);
          }}
        />
      )}
    </div>
  );
}

function pillFor(status: string): string {
  if (status === 'completed') return 'connected';
  if (status === 'running') return 'connecting';
  if (status === 'failed') return 'error';
  return 'disconnected';
}
