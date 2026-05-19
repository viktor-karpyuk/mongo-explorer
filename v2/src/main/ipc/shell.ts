import { ipcMain } from 'electron';
import { IpcChannels } from '../../shared/ipc.js';
import type { AppContext } from '../context.js';
import { sendShell, startShell, stopShell } from '../io/shell.js';

export function registerShellIpc(ctx: AppContext): void {
  ipcMain.handle(IpcChannels.ShellStart, (_e, connectionId: string): string =>
    startShell(ctx, connectionId),
  );
  ipcMain.handle(IpcChannels.ShellSend, (_e, sessionId: string, text: string): boolean =>
    sendShell(sessionId, text),
  );
  ipcMain.handle(IpcChannels.ShellStop, (_e, sessionId: string): void =>
    stopShell(sessionId),
  );
}
