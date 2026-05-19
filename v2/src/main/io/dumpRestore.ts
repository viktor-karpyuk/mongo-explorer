import { spawn } from 'node:child_process';
import { ulid } from 'ulid';
import { BrowserWindow } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type { DumpRestoreEvent, DumpRestoreRequest } from '../../shared/io.js';
import type { AppContext } from '../context.js';

export function startDumpRestore(ctx: AppContext, req: DumpRestoreRequest): string {
  const jobId = ulid();
  const record = ctx.connections.get(req.connectionId);
  if (!record) {
    notifyAll({ jobId, channel: 'stderr', data: 'Connection not found.\n' });
    notifyAll({ jobId, channel: 'exit', data: '', exitCode: 1 });
    return jobId;
  }

  const bin = req.kind === 'dump' ? 'mongodump' : 'mongorestore';
  const args: string[] = ['--uri', record.uri];
  if (req.db) args.push('--db', req.db);
  if (req.collection) args.push('--collection', req.collection);
  if (req.kind === 'dump') args.push('--out', req.path);
  else args.push(req.path);
  if (req.extraArgs) args.push(...req.extraArgs);

  let proc;
  try {
    proc = spawn(bin, args, { env: process.env });
  } catch (err) {
    notifyAll({
      jobId,
      channel: 'stderr',
      data: `Failed to spawn ${bin}: ${(err as Error).message}\n` +
        `Is ${bin} installed and on your PATH?\n`,
    });
    notifyAll({ jobId, channel: 'exit', data: '', exitCode: 127 });
    return jobId;
  }

  proc.stdout.on('data', (chunk: Buffer) =>
    notifyAll({ jobId, channel: 'stdout', data: chunk.toString('utf8') }),
  );
  proc.stderr.on('data', (chunk: Buffer) =>
    notifyAll({ jobId, channel: 'stderr', data: chunk.toString('utf8') }),
  );
  proc.on('error', (err) =>
    notifyAll({ jobId, channel: 'stderr', data: `${err.message}\n` }),
  );
  proc.on('exit', (code) =>
    notifyAll({ jobId, channel: 'exit', data: '', exitCode: code }),
  );

  return jobId;
}

function notifyAll(event: DumpRestoreEvent): void {
  for (const win of BrowserWindow.getAllWindows()) {
    win.webContents.send(IpcChannels.IoDumpRestoreEvent, event);
  }
}
