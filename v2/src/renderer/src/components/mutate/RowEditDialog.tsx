import { useState } from 'react';
import { MutateDialog } from './MutateDialog';
import { prettyPrint } from '../../utils/ejson';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  /** Raw EJSON string for the document being edited. */
  doc: string;
  onClose: () => void;
  onDone?: () => void;
}

export function RowEditDialog({
  connectionId,
  db,
  collection,
  doc,
  onClose,
  onDone,
}: Props) {
  const [body, setBody] = useState(prettyPrint(doc));

  function extractIdFilter(): string | null {
    try {
      const parsed = JSON.parse(doc) as Record<string, unknown>;
      const id = parsed['_id'];
      if (id === undefined) return null;
      return JSON.stringify({ _id: id });
    } catch {
      return null;
    }
  }

  return (
    <MutateDialog
      title={`Edit document · ${db}.${collection}`}
      primaryLabel="Save"
      canSubmit={body.trim().length > 0}
      onCancel={onClose}
      onSuccess={() => onDone?.()}
      onSubmit={async () => {
        const filter = extractIdFilter();
        if (!filter) {
          return {
            ok: false,
            error: 'Document has no _id; cannot replace.',
            durationMs: 0,
          };
        }
        return window.mex.mutate.replace({
          connectionId,
          db,
          collection,
          filter,
          replacement: body,
        });
      }}
    >
      <label className="field">
        <span>Document body (replaceOne by _id)</span>
        <textarea
          className="json-editor"
          rows={18}
          value={body}
          onChange={(e) => setBody(e.target.value)}
          spellCheck={false}
        />
      </label>
    </MutateDialog>
  );
}
