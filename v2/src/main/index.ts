import { app, BrowserWindow, ipcMain, shell } from 'electron';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { IpcChannels } from '../shared/ipc.js';
import { closeContext, initContext } from './context.js';
import { registerConnectionsIpc } from './ipc/connections.js';
import { registerNamespaceIpc } from './ipc/namespaces.js';
import { registerQueryIpc } from './ipc/queries.js';
import { registerAggregateIpc } from './ipc/aggregate.js';
import { registerSchemaIpc } from './ipc/schema.js';
import { registerMutateIpc } from './ipc/mutate.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const isDev = !!process.env['ELECTRON_RENDERER_URL'];

function createWindow(): BrowserWindow {
  const win = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 960,
    minHeight: 600,
    show: false,
    backgroundColor: '#0f1115',
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    webPreferences: {
      preload: join(__dirname, '../preload/index.js'),
      sandbox: true,
      contextIsolation: true,
      nodeIntegration: false,
      spellcheck: false,
    },
  });

  win.once('ready-to-show', () => win.show());
  win.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  if (isDev) {
    win.loadURL(process.env['ELECTRON_RENDERER_URL']!);
  } else {
    win.loadFile(join(__dirname, '../renderer/index.html'));
  }

  return win;
}

function registerIpc(): void {
  ipcMain.handle(IpcChannels.AppVersion, () => app.getVersion());
  ipcMain.handle(IpcChannels.AppPing, () => 'pong');
}

app.whenReady().then(() => {
  const ctx = initContext();
  ctx.registry = registerConnectionsIpc(ctx);
  registerNamespaceIpc(ctx.registry);
  registerQueryIpc(ctx, ctx.registry);
  registerAggregateIpc(ctx, ctx.registry);
  registerSchemaIpc(ctx.registry);
  registerMutateIpc(ctx.registry);
  registerIpc();
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('will-quit', (e) => {
  e.preventDefault();
  closeContext().finally(() => app.exit());
});
