import { useEffect, useState, type ReactNode } from 'react';
import type { MutateResponse } from '../../../../shared/mutation.js';

interface Props {
  title: string;
  primaryLabel: string;
  destructive?: boolean;
  onCancel: () => void;
  onSubmit: () => Promise<MutateResponse>;
  onSuccess?: (r: Extract<MutateResponse, { ok: true }>) => void;
  children: ReactNode;
  canSubmit: boolean;
}

export function MutateDialog({
  title,
  primaryLabel,
  destructive,
  onCancel,
  onSubmit,
  onSuccess,
  children,
  canSubmit,
}: Props) {
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<MutateResponse | null>(null);

  useEffect(() => {
    function onEsc(e: KeyboardEvent) {
      if (e.key === 'Escape') onCancel();
    }
    window.addEventListener('keydown', onEsc);
    return () => window.removeEventListener('keydown', onEsc);
  }, [onCancel]);

  async function submit() {
    setBusy(true);
    setResult(null);
    try {
      const r = await onSubmit();
      setResult(r);
      if (r.ok && onSuccess) onSuccess(r);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal-backdrop" onClick={onCancel}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal__header">
          <h2>{title}</h2>
          <button className="btn btn--ghost" onClick={onCancel}>
            ✕
          </button>
        </header>
        <div className="modal__body">
          {children}
          {result && result.ok && (
            <div className="alert alert--success">
              Done in {result.durationMs} ms.{' '}
              {result.matched != null && <>matched {result.matched} · </>}
              {result.modified != null && <>modified {result.modified} · </>}
              {result.inserted != null && <>inserted {result.inserted} · </>}
              {result.deleted != null && <>deleted {result.deleted}</>}
            </div>
          )}
          {result && !result.ok && (
            <div className="alert alert--error">{result.error}</div>
          )}
        </div>
        <footer className="modal__footer">
          <div className="spacer" />
          <button className="btn btn--ghost" onClick={onCancel}>
            {result?.ok ? 'Close' : 'Cancel'}
          </button>
          {!result?.ok && (
            <button
              className={`btn ${destructive ? 'btn--danger btn--primary' : 'btn--primary'}`}
              onClick={() => void submit()}
              disabled={!canSubmit || busy}
            >
              {busy ? 'Running…' : primaryLabel}
            </button>
          )}
        </footer>
      </div>
    </div>
  );
}
