import { useState } from 'react';
import { MutateDialog } from './MutateDialog';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  initialFilter?: string;
  initialMany?: boolean;
  onClose: () => void;
  onDone?: () => void;
}

export function DeleteDialog({
  connectionId,
  db,
  collection,
  initialFilter,
  initialMany,
  onClose,
  onDone,
}: Props) {
  const [filter, setFilter] = useState(initialFilter ?? '{}');
  const [many, setMany] = useState(initialMany ?? false);

  return (
    <MutateDialog
      title={`Delete from ${db}.${collection}`}
      primaryLabel={many ? 'Delete many' : 'Delete one'}
      destructive
      canSubmit={filter.trim().length > 0}
      onCancel={onClose}
      onSuccess={() => onDone?.()}
      onSubmit={() =>
        window.mex.mutate.delete({
          connectionId,
          db,
          collection,
          filter,
          many,
        })
      }
    >
      <label className="field">
        <span>Filter</span>
        <textarea
          className="json-editor"
          rows={5}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          spellCheck={false}
        />
      </label>

      <label className="field--row">
        <input type="checkbox" checked={many} onChange={(e) => setMany(e.target.checked)} />
        <span>Apply to all matching (deleteMany)</span>
      </label>
    </MutateDialog>
  );
}
