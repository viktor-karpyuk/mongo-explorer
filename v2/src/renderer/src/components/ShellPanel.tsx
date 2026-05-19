import { useEffect, useRef, useState } from 'react';
import { useConnectionsStore } from '../store/connections';
import type { ShellEvent } from '../../../shared/shell.js';

interface Props {
  connectionId: string;
}

export function ShellPanel({ connectionId }: Props) {
  const conn = useConnectionsStore((s) => s.list.find((c) => c.id === connectionId));
  const state = useConnectionsStore((s) => s.states[connectionId]);

  const [sessionId, setSessionId] = useState<string | null>(null);
  const [output, setOutput] = useState('');
  const [input, setInput] = useState('');
  const [history, setHistory] = useState<string[]>([]);
  const [historyCursor, setHistoryCursor] = useState(-1);
  const [running, setRunning] = useState(false);

  const outputRef = useRef<HTMLPreElement>(null);

  useEffect(() => {
    const unsub = window.mex.shell.onEvent((event: ShellEvent) => {
      if (event.sessionId !== sessionId) return;
      if (event.channel === 'exit') {
        setOutput((s) => s + `\n[mongosh exited with code ${event.exitCode ?? '?'}]\n`);
        setRunning(false);
        setSessionId(null);
      } else {
        setOutput((s) => s + event.data);
      }
    });
    return unsub;
  }, [sessionId]);

  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [output]);

  async function start() {
    setOutput('');
    setRunning(true);
    const id = await window.mex.shell.start(connectionId);
    setSessionId(id);
  }

  async function stop() {
    if (!sessionId) return;
    await window.mex.shell.stop(sessionId);
  }

  async function send() {
    if (!sessionId || !input.trim()) return;
    setOutput((s) => s + `> ${input}\n`);
    setHistory((h) => [input, ...h].slice(0, 100));
    setHistoryCursor(-1);
    await window.mex.shell.send(sessionId, input);
    setInput('');
  }

  function onKey(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      void send();
    } else if (e.key === 'ArrowUp' && (e.metaKey || e.ctrlKey || input === '')) {
      e.preventDefault();
      if (history.length === 0) return;
      const next = Math.min(historyCursor + 1, history.length - 1);
      setHistoryCursor(next);
      setInput(history[next] ?? '');
    } else if (e.key === 'ArrowDown' && (e.metaKey || e.ctrlKey || historyCursor >= 0)) {
      e.preventDefault();
      const next = historyCursor - 1;
      if (next < 0) {
        setHistoryCursor(-1);
        setInput('');
      } else {
        setHistoryCursor(next);
        setInput(history[next] ?? '');
      }
    }
  }

  return (
    <div className="view">
      <header className="view__header">
        <h1>Shell · {conn?.name}</h1>
        {sessionId ? (
          <button className="btn btn--ghost btn--danger" onClick={() => void stop()}>
            Stop session
          </button>
        ) : (
          <button
            className="btn btn--primary"
            onClick={() => void start()}
            disabled={state?.kind !== 'connected' || running}
          >
            Start mongosh
          </button>
        )}
      </header>

      <p className="muted">
        Spawns the local <code>mongosh</code> binary against this connection's URI.
        Press Enter to send, Shift+Enter for newline, Cmd/Ctrl+↑/↓ for history.
      </p>

      <pre ref={outputRef} className="shell-output">
        {output || '(no output yet — start a session)'}
      </pre>

      <textarea
        className="shell-input"
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={onKey}
        rows={3}
        spellCheck={false}
        disabled={!sessionId}
        placeholder={sessionId ? 'Type JS expressions, e.g. db.users.find({}).limit(5)' : ''}
      />
    </div>
  );
}
