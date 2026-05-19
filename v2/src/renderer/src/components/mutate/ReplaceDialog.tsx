import { useState } from 'react';
import { MutateDialog } from './MutateDialog';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  initialFilter?: string;
  initialReplacement?: string;
  onClose: () => void;
  onDone?: () => void;
}

export function ReplaceDialog({
  connectionId,
  db,
  collection,
  initialFilter,
  initialReplacement,
  onClose,
  onDone,
}: Props) {
  const [filter, setFilter] = useState(initialFilter ?? '{ _id: ObjectId("...") }');
  const [replacement, setReplacement] = useState(initialReplacement ?? '{}');
  const [upsert, setUpsert] = useState(false);

  return (
    <MutateDialog
      title={`Replace in ${db}.${collection}`}
      primaryLabel="Replace one"
      destructive
      canSubmit={filter.trim().length > 0 && replacement.trim().length > 0}
      onCancel={onClose}
      onSuccess={() => onDone?.()}
      onSubmit={() =>
        window.mex.mutate.replace({
          connectionId,
          db,
          collection,
          filter,
          replacement,
          upsert,
        })
      }
    >
      <label className="field">
        <span>Filter</span>
        <textarea
          className="json-editor"
          rows={3}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          spellCheck={false}
        />
      </label>

      <label className="field">
        <span>Replacement document</span>
        <textarea
          className="json-editor"
          rows={12}
          value={replacement}
          onChange={(e) => setReplacement(e.target.value)}
          spellCheck={false}
        />
      </label>

      <label className="field--row">
        <input
          type="checkbox"
          checked={upsert}
          onChange={(e) => setUpsert(e.target.checked)}
        />
        <span>Upsert</span>
      </label>
    </MutateDialog>
  );
}
