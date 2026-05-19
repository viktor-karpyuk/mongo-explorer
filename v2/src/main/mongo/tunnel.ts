import { readFile } from 'node:fs/promises';
import { createServer, type Server } from 'node:net';
import { Client } from 'ssh2';
import type { SshTunnel } from '../../shared/connection.js';

export interface ActiveTunnel {
  localHost: string;
  localPort: number;
  close: () => Promise<void>;
}

export async function openTunnel(
  ssh: SshTunnel,
  remoteHost: string,
  remotePort: number,
): Promise<ActiveTunnel> {
  const privateKey = await readFile(ssh.keyPath);

  const sshClient = await new Promise<Client>((resolve, reject) => {
    const c = new Client();
    c.once('ready', () => resolve(c));
    c.once('error', reject);
    c.connect({
      host: ssh.host,
      port: ssh.port,
      username: ssh.user,
      privateKey,
      readyTimeout: 10_000,
    });
  });

  const server: Server = createServer((socket) => {
    sshClient.forwardOut(
      socket.remoteAddress ?? '127.0.0.1',
      socket.remotePort ?? 0,
      remoteHost,
      remotePort,
      (err, stream) => {
        if (err) {
          socket.destroy(err);
          return;
        }
        socket.pipe(stream).pipe(socket);
      },
    );
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });

  const addr = server.address();
  if (!addr || typeof addr === 'string') {
    server.close();
    sshClient.end();
    throw new Error('Failed to allocate local tunnel port');
  }

  return {
    localHost: '127.0.0.1',
    localPort: addr.port,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => {
          sshClient.end();
          resolve();
        });
      }),
  };
}

export function rewriteUriToLocal(uri: string, localHost: string, localPort: number): string {
  const parsed = new URL(uri);
  parsed.host = `${localHost}:${localPort}`;
  if (parsed.searchParams.has('directConnection')) {
    parsed.searchParams.set('directConnection', 'true');
  }
  return parsed.toString();
}
