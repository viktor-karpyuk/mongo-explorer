import { useEffect, useState } from 'react';
import { useQueryStore, nsKey } from '../store/query';
import { ResultsPane } from './results/ResultsPane';
import { InsertDialog } from './mutate/InsertDialog';
import { UpdateDialog } from './mutate/UpdateDialog';
import { ReplaceDialog } from './mutate/ReplaceDialog';
import { DeleteDialog } from './mutate/DeleteDialog';
import { BulkWriteDialog } from './mutate/BulkWriteDialog';
import { RowEditDialog } from './mutate/RowEditDialog';
import { ExportDialog } from './io/ExportDialog';
import { ImportDialog } from './io/ImportDialog';
import { DumpRestoreDialog } from './io/DumpRestoreDialog';

type MutateView =
  | null
  | 'insert'
  | 'update'
  | 'replace'
  | 'delete'
  | 'bulk'
  | 'export'
  | 'import'
  | 'dump'
  | { kind: 'editRow'; doc: string };

interface Props {
  connectionId: string;
  db: string;
  collection: string;
}

export function QueryView({ connectionId, db, collection }: Props) {
  const ns = nsKey(connectionId, db, collection);
  const draft = useQueryStore((s) => s.draftByNs[ns]) ?? useQueryStore.getState().getDraft(ns);
  const result = useQueryStore((s) => s.resultByNs[ns]) ?? null;
  const running = useQueryStore((s) => s.runningByNs[ns]) ?? false;
  const setDraft = useQueryStore((s) => s.setDraft);
  const run = useQueryStore((s) => s.run);
  const nextPage = useQueryStore((s) => s.nextPage);
  const prevPage = useQueryStore((s) => s.prevPage);

  const base = { connectionId, db, collection };

  // Cmd/Ctrl+Enter to run
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        void run(ns, base);
      }
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ns, connectionId, db, collection]);

  const [rowsCollapsed, setRowsCollapsed] = useState(false);
  const [mutate, setMutate] = useState<MutateView>(null);

  function rerun() {
    void run(ns, base);
  }

  return (
    <div className="query-view">
      <div className={`query-view__editor${rowsCollapsed ? ' query-view__editor--collapsed' : ''}`}>
        <div className="query-view__editor-head">
          <strong>{db}.{collection}</strong>
          <button
            className="btn btn--ghost btn--xs"
            onClick={() => setRowsCollapsed(!rowsCollapsed)}
          >
            {rowsCollapsed ? 'Expand editor ▾' : 'Collapse editor ▴'}
          </button>
          <div className="spacer" />
          <div className="mutate-menu">
            <button className="btn btn--ghost" onClick={() => setMutate('insert')}>
              Insert
            </button>
            <button className="btn btn--ghost" onClick={() => setMutate('update')}>
              Update
            </button>
            <button className="btn btn--ghost" onClick={() => setMutate('replace')}>
              Replace
            </button>
            <button className="btn btn--ghost btn--danger" onClick={() => setMutate('delete')}>
              Delete
            </button>
            <button className="btn btn--ghost" onClick={() => setMutate('bulk')}>
              Bulk
            </button>
            <button className="btn btn--ghost" onClick={() => setMutate('export')}>
              Export
            </button>
            <button className="btn btn--ghost" onClick={() => setMutate('import')}>
              Import
            </button>
            <button className="btn btn--ghost" onClick={() => setMutate('dump')}>
              Dump/Restore
            </button>
          </div>
          <button
            className="btn btn--primary"
            onClick={() => void run(ns, base)}
            disabled={running}
          >
            {running ? 'Running…' : 'Run ⌘↵'}
          </button>
        </div>

        {!rowsCollapsed && (
          <div className="query-form">
            <label className="query-row">
              <span>Filter</span>
              <input
                value={draft.filter}
                onChange={(e) => setDraft(ns, { filter: e.target.value })}
                spellCheck={false}
                placeholder='{ status: "active" }   or   24-hex _id   or   { _id: ObjectId("...") }'
              />
            </label>
            <label className="query-row">
              <span>Projection</span>
              <input
                value={draft.projection}
                onChange={(e) => setDraft(ns, { projection: e.target.value })}
                spellCheck={false}
                placeholder='{ name: 1, email: 1, _id: 0 }'
              />
            </label>
            <label className="query-row">
              <span>Sort</span>
              <input
                value={draft.sort}
                onChange={(e) => setDraft(ns, { sort: e.target.value })}
                spellCheck={false}
                placeholder='{ createdAt: -1 }'
              />
            </label>
            <div className="query-row query-row--numbers">
              <label>
                <span>Skip</span>
                <input
                  type="number"
                  min={0}
                  value={draft.skip}
                  onChange={(e) => setDraft(ns, { skip: Number(e.target.value) || 0 })}
                />
              </label>
              <label>
                <span>Limit</span>
                <input
                  type="number"
                  min={1}
                  value={draft.limit}
                  onChange={(e) => setDraft(ns, { limit: Number(e.target.value) || 50 })}
                />
              </label>
              <label>
                <span>maxTimeMs</span>
                <input
                  type="number"
                  min={0}
                  value={draft.maxTimeMs}
                  onChange={(e) =>
                    setDraft(ns, { maxTimeMs: Number(e.target.value) || 30_000 })
                  }
                />
              </label>
            </div>
          </div>
        )}
      </div>

      <ResultsPane
        result={result}
        running={running}
        skip={draft.skip}
        limit={draft.limit}
        onPrev={() => void prevPage(ns, base)}
        onNext={() => void nextPage(ns, base)}
        onEditRow={(doc) => setMutate({ kind: 'editRow', doc })}
        onDeleteRow={(doc) => {
          try {
            const parsed = JSON.parse(doc) as Record<string, unknown>;
            const id = parsed['_id'];
            if (id === undefined) {
              alert('Document has no _id; cannot delete inline.');
              return;
            }
            setMutate('delete');
            setDraft(ns, { filter: JSON.stringify({ _id: id }) });
          } catch {
            alert('Could not parse document.');
          }
        }}
      />

      {mutate === 'insert' && (
        <InsertDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          onClose={() => setMutate(null)}
          onDone={rerun}
        />
      )}
      {mutate === 'update' && (
        <UpdateDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          initialFilter={draft.filter}
          onClose={() => setMutate(null)}
          onDone={rerun}
        />
      )}
      {mutate === 'replace' && (
        <ReplaceDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          initialFilter={draft.filter}
          onClose={() => setMutate(null)}
          onDone={rerun}
        />
      )}
      {mutate === 'delete' && (
        <DeleteDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          initialFilter={draft.filter}
          onClose={() => setMutate(null)}
          onDone={rerun}
        />
      )}
      {mutate === 'bulk' && (
        <BulkWriteDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          onClose={() => setMutate(null)}
          onDone={rerun}
        />
      )}
      {mutate && typeof mutate === 'object' && mutate.kind === 'editRow' && (
        <RowEditDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          doc={mutate.doc}
          onClose={() => setMutate(null)}
          onDone={rerun}
        />
      )}
      {mutate === 'export' && (
        <ExportDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          initialFilter={draft.filter}
          initialProjection={draft.projection}
          initialSort={draft.sort}
          onClose={() => setMutate(null)}
        />
      )}
      {mutate === 'import' && (
        <ImportDialog
          connectionId={connectionId}
          db={db}
          collection={collection}
          onClose={() => setMutate(null)}
          onDone={rerun}
        />
      )}
      {mutate === 'dump' && (
        <DumpRestoreDialog
          connectionId={connectionId}
          defaultDb={db}
          defaultCollection={collection}
          onClose={() => setMutate(null)}
        />
      )}
    </div>
  );
}
