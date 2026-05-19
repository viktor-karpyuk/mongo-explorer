import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { ulid } from 'ulid';
import { BrowserWindow } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type { ShellEvent } from '../../shared/shell.js';
import type { AppContext } from '../context.js';

interface Session {
  proc: ChildProcessWithoutNullStreams;
}

const sessions = new Map<string, Session>();

export function startShell(ctx: AppContext, connectionId: string): string {
  const sessionId = ulid();
  const record = ctx.connections.get(connectionId);
  if (!record) {
    push({ sessionId, channel: 'stderr', data: 'Connection not found.\n' });
    push({ sessionId, channel: 'exit', data: '', exitCode: 1 });
    return sessionId;
  }

  let proc: ChildProcessWithoutNullStreams;
  try {
    proc = spawn('mongosh', ['--quiet', '--norc', record.uri], {
      env: process.env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
  } catch (err) {
    push({
      sessionId,
      channel: 'stderr',
      data: `Failed to spawn mongosh: ${(err as Error).message}\n` +
        `Is mongosh installed and on your PATH?\n`,
    });
    push({ sessionId, channel: 'exit', data: '', exitCode: 127 });
    return sessionId;
  }

  sessions.set(sessionId, { proc });

  proc.stdout.on('data', (chunk: Buffer) =>
    push({ sessionId, channel: 'stdout', data: chunk.toString('utf8') }),
  );
  proc.stderr.on('data', (chunk: Buffer) =>
    push({ sessionId, channel: 'stderr', data: chunk.toString('utf8') }),
  );
  proc.on('error', (err) =>
    push({ sessionId, channel: 'stderr', data: `${err.message}\n` }),
  );
  proc.on('exit', (code) => {
    sessions.delete(sessionId);
    push({ sessionId, channel: 'exit', data: '', exitCode: code });
  });

  return sessionId;
}

export function sendShell(sessionId: string, text: string): boolean {
  const session = sessions.get(sessionId);
  if (!session) return false;
  session.proc.stdin.write(text.endsWith('\n') ? text : text + '\n');
  return true;
}

export function stopShell(sessionId: string): void {
  const session = sessions.get(sessionId);
  if (!session) return;
  session.proc.kill();
  sessions.delete(sessionId);
}

function push(event: ShellEvent): void {
  for (const win of BrowserWindow.getAllWindows()) {
    win.webContents.send(IpcChannels.ShellEvent, event);
  }
}
