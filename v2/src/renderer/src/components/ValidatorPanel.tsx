import { useEffect, useState } from 'react';
import type { ValidatorPayload } from '../../../shared/schema.js';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
}

export function ValidatorPanel({ connectionId, db, collection }: Props) {
  const [body, setBody] = useState('');
  const [level, setLevel] = useState<NonNullable<ValidatorPayload['validationLevel']>>('moderate');
  const [action, setAction] = useState<NonNullable<ValidatorPayload['validationAction']>>('error');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  async function load() {
    setError(null);
    setSuccess(null);
    try {
      const v = await window.mex.schema.getValidator(connectionId, db, collection);
      setBody(v.validator ?? '');
      if (v.validationLevel) setLevel(v.validationLevel);
      if (v.validationAction) setAction(v.validationAction);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectionId, db, collection]);

  async function apply() {
    setBusy(true);
    setError(null);
    setSuccess(null);
    try {
      await window.mex.schema.setValidator(connectionId, db, collection, {
        validator: body.trim() || null,
        validationLevel: level,
        validationAction: action,
      });
      setSuccess('Validator updated.');
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <header className="view__header">
        <h1>Validator · {db}.{collection}</h1>
      </header>

      {error && <div className="alert alert--error">{error}</div>}
      {success && <div className="alert alert--success">{success}</div>}

      <div className="field">
        <span>Validator JSON ($jsonSchema or expression)</span>
        <textarea
          className="validator-editor"
          value={body}
          onChange={(e) => setBody(e.target.value)}
          spellCheck={false}
          rows={16}
          placeholder='{ "$jsonSchema": { "bsonType": "object", "required": [...], "properties": {...} } }'
        />
      </div>

      <div className="validator-controls">
        <label className="inline-field">
          <span>Level</span>
          <select value={level} onChange={(e) => setLevel(e.target.value as typeof level)}>
            <option value="off">off</option>
            <option value="moderate">moderate</option>
            <option value="strict">strict</option>
          </select>
        </label>
        <label className="inline-field">
          <span>Action</span>
          <select value={action} onChange={(e) => setAction(e.target.value as typeof action)}>
            <option value="warn">warn</option>
            <option value="error">error</option>
          </select>
        </label>
        <div className="spacer" />
        <button className="btn btn--ghost" onClick={() => void load()} disabled={busy}>
          Revert
        </button>
        <button className="btn btn--primary" onClick={() => void apply()} disabled={busy}>
          {busy ? 'Applying…' : 'Apply'}
        </button>
      </div>
    </div>
  );
}
