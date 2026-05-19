import { useState } from 'react';
import { MutateDialog } from './MutateDialog';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
  onClose: () => void;
  onDone?: () => void;
}

export function InsertDialog({ connectionId, db, collection, onClose, onDone }: Props) {
  const [mode, setMode] = useState<'one' | 'many'>('one');
  const [body, setBody] = useState('{\n  "name": ""\n}');
  const [ordered, setOrdered] = useState(true);

  return (
    <MutateDialog
      title={`Insert into ${db}.${collection}`}
      primaryLabel={mode === 'one' ? 'Insert one' : 'Insert many'}
      canSubmit={body.trim().length > 0}
      onCancel={onClose}
      onSuccess={() => onDone?.()}
      onSubmit={async () => {
        if (mode === 'one') {
          return window.mex.mutate.insertOne({
            connectionId,
            db,
            collection,
            document: body,
          });
        }
        return window.mex.mutate.insertMany({
          connectionId,
          db,
          collection,
          documents: body,
          ordered,
        });
      }}
    >
      <div className="seg-control">
        <button
          className={`seg${mode === 'one' ? ' seg--active' : ''}`}
          onClick={() => setMode('one')}
        >
          Insert one
        </button>
        <button
          className={`seg${mode === 'many' ? ' seg--active' : ''}`}
          onClick={() => setMode('many')}
        >
          Insert many (JSON array or NDJSON)
        </button>
      </div>

      <label className="field">
        <span>Document{mode === 'many' ? 's' : ''}</span>
        <textarea
          className="json-editor"
          value={body}
          onChange={(e) => setBody(e.target.value)}
          rows={14}
          spellCheck={false}
        />
      </label>

      {mode === 'many' && (
        <label className="field--row">
          <input
            type="checkbox"
            checked={ordered}
            onChange={(e) => setOrdered(e.target.checked)}
          />
          <span>Ordered (stop at first error)</span>
        </label>
      )}
    </MutateDialog>
  );
}
