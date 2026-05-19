import { useEffect, useState } from 'react';
import type {
  ConnectionInput,
  ConnectionRecord,
} from '../../../shared/connection.js';
import type { TestResult } from '../../../shared/connectionState.js';

interface Props {
  initial?: ConnectionRecord | null;
  onCancel: () => void;
  onSave: (input: ConnectionInput) => Promise<void>;
}

const EMPTY: ConnectionInput = {
  name: '',
  uri: 'mongodb://localhost:27017',
};

export function ConnectionForm({ initial, onCancel, onSave }: Props) {
  const [name, setName] = useState(initial?.name ?? EMPTY.name);
  const [uri, setUri] = useState(initial?.uri ?? EMPTY.uri);
  const [notes, setNotes] = useState(initial?.notes ?? '');

  const [sshEnabled, setSshEnabled] = useState(!!initial?.ssh);
  const [sshHost, setSshHost] = useState(initial?.ssh?.host ?? '');
  const [sshPort, setSshPort] = useState(initial?.ssh?.port?.toString() ?? '22');
  const [sshUser, setSshUser] = useState(initial?.ssh?.user ?? '');
  const [sshKeyPath, setSshKeyPath] = useState(initial?.ssh?.keyPath ?? '');

  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    function onEsc(e: KeyboardEvent) {
      if (e.key === 'Escape') onCancel();
    }
    window.addEventListener('keydown', onEsc);
    return () => window.removeEventListener('keydown', onEsc);
  }, [onCancel]);

  function buildInput(): ConnectionInput {
    return {
      name: name.trim(),
      uri: uri.trim(),
      ssh: sshEnabled
        ? {
            host: sshHost.trim(),
            port: Number(sshPort) || 22,
            user: sshUser.trim(),
            keyPath: sshKeyPath.trim(),
          }
        : undefined,
      notes: notes.trim() || undefined,
    };
  }

  async function handleTest() {
    setTesting(true);
    setTestResult(null);
    try {
      const r = await window.mex.connections.test(buildInput());
      setTestResult(r);
    } finally {
      setTesting(false);
    }
  }

  async function handleSave() {
    if (!name.trim() || !uri.trim()) return;
    setSaving(true);
    try {
      await onSave(buildInput());
    } finally {
      setSaving(false);
    }
  }

  const canSave = name.trim().length > 0 && uri.trim().length > 0 && !saving;

  return (
    <div className="modal-backdrop" onClick={onCancel}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <header className="modal__header">
          <h2>{initial ? 'Edit connection' : 'New connection'}</h2>
          <button className="btn btn--ghost" onClick={onCancel}>
            ✕
          </button>
        </header>

        <div className="modal__body">
          <label className="field">
            <span>Name</span>
            <input
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="My local cluster"
            />
          </label>

          <label className="field">
            <span>Connection URI</span>
            <input
              value={uri}
              onChange={(e) => setUri(e.target.value)}
              placeholder="mongodb://user:pass@host:27017/db"
              spellCheck={false}
              autoCapitalize="off"
            />
          </label>

          <label className="field field--row">
            <input
              type="checkbox"
              checked={sshEnabled}
              onChange={(e) => setSshEnabled(e.target.checked)}
            />
            <span>Tunnel through SSH</span>
          </label>

          {sshEnabled && (
            <div className="ssh-grid">
              <label className="field">
                <span>SSH host</span>
                <input
                  value={sshHost}
                  onChange={(e) => setSshHost(e.target.value)}
                  placeholder="bastion.example.com"
                />
              </label>
              <label className="field">
                <span>Port</span>
                <input
                  value={sshPort}
                  onChange={(e) => setSshPort(e.target.value)}
                  inputMode="numeric"
                />
              </label>
              <label className="field">
                <span>User</span>
                <input
                  value={sshUser}
                  onChange={(e) => setSshUser(e.target.value)}
                  placeholder="ec2-user"
                />
              </label>
              <label className="field">
                <span>Private key path</span>
                <input
                  value={sshKeyPath}
                  onChange={(e) => setSshKeyPath(e.target.value)}
                  placeholder="~/.ssh/id_ed25519"
                />
              </label>
            </div>
          )}

          <label className="field">
            <span>Notes (optional)</span>
            <textarea
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              rows={2}
            />
          </label>

          {testResult && (
            <div
              className={`test-result test-result--${
                testResult.ok ? 'ok' : 'fail'
              }`}
            >
              {testResult.ok ? (
                <>
                  <strong>Reachable.</strong> MongoDB {testResult.serverVersion}{' '}
                  · {testResult.topology} · {testResult.pingMs} ms
                </>
              ) : (
                <>
                  <strong>Failed:</strong> {testResult.error}
                </>
              )}
            </div>
          )}
        </div>

        <footer className="modal__footer">
          <button
            className="btn btn--ghost"
            onClick={handleTest}
            disabled={testing || !uri.trim()}
          >
            {testing ? 'Testing…' : 'Test connection'}
          </button>
          <div className="spacer" />
          <button className="btn btn--ghost" onClick={onCancel}>
            Cancel
          </button>
          <button
            className="btn btn--primary"
            onClick={handleSave}
            disabled={!canSave}
          >
            {saving ? 'Saving…' : 'Save'}
          </button>
        </footer>
      </div>
    </div>
  );
}
