import { useState } from 'react';
import { MutateDialog } from './MutateDialog';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  initialFilter?: string;
  onClose: () => void;
  onDone?: () => void;
}

export function UpdateDialog({
  connectionId,
  db,
  collection,
  initialFilter,
  onClose,
  onDone,
}: Props) {
  const [filter, setFilter] = useState(initialFilter ?? '{}');
  const [update, setUpdate] = useState('{ $set: {  } }');
  const [many, setMany] = useState(false);
  const [upsert, setUpsert] = useState(false);

  return (
    <MutateDialog
      title={`Update in ${db}.${collection}`}
      primaryLabel={many ? 'Update many' : 'Update one'}
      destructive
      canSubmit={filter.trim().length > 0 && update.trim().length > 0}
      onCancel={onClose}
      onSuccess={() => onDone?.()}
      onSubmit={() =>
        window.mex.mutate.update({
          connectionId,
          db,
          collection,
          filter,
          update,
          many,
          upsert,
        })
      }
    >
      <label className="field">
        <span>Filter</span>
        <textarea
          className="json-editor"
          rows={4}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          spellCheck={false}
        />
      </label>

      <label className="field">
        <span>Update expression ($set / $inc / etc.)</span>
        <textarea
          className="json-editor"
          rows={6}
          value={update}
          onChange={(e) => setUpdate(e.target.value)}
          spellCheck={false}
        />
      </label>

      <div className="idx-flags">
        <label className="field--row">
          <input type="checkbox" checked={many} onChange={(e) => setMany(e.target.checked)} />
          <span>Apply to all matching (updateMany)</span>
        </label>
        <label className="field--row">
          <input
            type="checkbox"
            checked={upsert}
            onChange={(e) => setUpsert(e.target.checked)}
          />
          <span>Upsert</span>
        </label>
      </div>
    </MutateDialog>
  );
}
