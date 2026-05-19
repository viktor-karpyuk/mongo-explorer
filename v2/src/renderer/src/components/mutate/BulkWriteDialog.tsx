import { useState } from 'react';
import { ulid } from 'ulid';
import { MutateDialog } from './MutateDialog';
import type { BulkOp } from '../../../../shared/mutation.js';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  onClose: () => void;
  onDone?: () => void;
}

const KIND_LABEL: Record<BulkOp['kind'], string> = {
  insertOne: 'insertOne',
  updateOne: 'updateOne',
  updateMany: 'updateMany',
  replaceOne: 'replaceOne',
  deleteOne: 'deleteOne',
  deleteMany: 'deleteMany',
};

const SAMPLE: Record<BulkOp['kind'], string> = {
  insertOne: '{ "_id": "x", "name": "y" }',
  updateOne: '{ filter: { _id: "x" }, update: { $set: { y: 1 } } }',
  updateMany: '{ filter: { status: "old" }, update: { $set: { status: "new" } } }',
  replaceOne: '{ filter: { _id: "x" }, replacement: {  } }',
  deleteOne: '{ _id: "x" }',
  deleteMany: '{ status: "stale" }',
};

export function BulkWriteDialog({ connectionId, db, collection, onClose, onDone }: Props) {
  const [ops, setOps] = useState<(BulkOp & { id: string })[]>([
    { id: ulid(), kind: 'insertOne', body: SAMPLE.insertOne },
  ]);
  const [ordered, setOrdered] = useState(true);

  function setOp(i: number, patch: Partial<BulkOp>) {
    setOps((prev) =>
      prev.map((o, idx) =>
        idx === i ? { ...o, ...patch, body: patch.kind ? SAMPLE[patch.kind] : o.body } : o,
      ),
    );
  }

  return (
    <MutateDialog
      title={`Bulk write · ${db}.${collection}`}
      primaryLabel="Run bulk"
      destructive
      canSubmit={ops.length > 0 && ops.every((o) => o.body.trim() !== '')}
      onCancel={onClose}
      onSuccess={() => onDone?.()}
      onSubmit={() =>
        window.mex.mutate.bulk({
          connectionId,
          db,
          collection,
          ops: ops.map(({ kind, body }) => ({ kind, body })),
          ordered,
        })
      }
    >
      {ops.map((op, i) => (
        <div key={op.id} className="bulk-op">
          <div className="bulk-op__head">
            <span className="bulk-op__index">{i + 1}</span>
            <select
              value={op.kind}
              onChange={(e) => setOp(i, { kind: e.target.value as BulkOp['kind'] })}
            >
              {Object.entries(KIND_LABEL).map(([k, v]) => (
                <option key={k} value={k}>
                  {v}
                </option>
              ))}
            </select>
            <div className="spacer" />
            <button
              className="btn btn--ghost btn--xs btn--danger"
              onClick={() => setOps((p) => p.filter((_, idx) => idx !== i))}
            >
              ✕
            </button>
          </div>
          <textarea
            className="json-editor"
            value={op.body}
            onChange={(e) => setOp(i, { body: e.target.value })}
            rows={3}
            spellCheck={false}
          />
        </div>
      ))}

      <button
        className="btn btn--ghost btn--xs"
        onClick={() =>
          setOps((p) => [...p, { id: ulid(), kind: 'insertOne', body: SAMPLE.insertOne }])
        }
      >
        + Add operation
      </button>

      <label className="field--row">
        <input
          type="checkbox"
          checked={ordered}
          onChange={(e) => setOrdered(e.target.checked)}
        />
        <span>Ordered</span>
      </label>
    </MutateDialog>
  );
}
