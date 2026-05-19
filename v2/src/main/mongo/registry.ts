import { MongoClient, type Document } from 'mongodb';
import type { ConnectionRecord } from '../../shared/connection.js';
import type { ConnectionState, TestResult } from '../../shared/connectionState.js';
import { openTunnel, rewriteUriToLocal, type ActiveTunnel } from './tunnel.js';

interface ActiveConnection {
  client: MongoClient;
  tunnel?: ActiveTunnel;
}

type StateListener = (id: string, state: ConnectionState) => void;

export class MongoRegistry {
  private readonly clients = new Map<string, ActiveConnection>();
  private readonly states = new Map<string, ConnectionState>();
  private readonly listeners = new Set<StateListener>();

  onStateChange(listener: StateListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  getState(id: string): ConnectionState {
    return this.states.get(id) ?? { kind: 'disconnected' };
  }

  snapshot(): ReadonlyMap<string, ConnectionState> {
    return new Map(this.states);
  }

  async connect(record: ConnectionRecord): Promise<ConnectionState> {
    if (this.clients.has(record.id)) {
      return this.getState(record.id);
    }
    this.setState(record.id, { kind: 'connecting' });

    let tunnel: ActiveTunnel | undefined;
    let client: MongoClient | undefined;

    try {
      let uri = record.uri;
      if (record.ssh) {
        const target = parseFirstHostPort(uri);
        tunnel = await openTunnel(record.ssh, target.host, target.port);
        uri = rewriteUriToLocal(uri, tunnel.localHost, tunnel.localPort);
      }

      client = new MongoClient(uri, {
        serverSelectionTimeoutMS: 8_000,
        connectTimeoutMS: 8_000,
      });
      await client.connect();

      const info = await probeServer(client);
      this.clients.set(record.id, { client, tunnel });
      const state: ConnectionState = {
        kind: 'connected',
        pingMs: info.pingMs,
        serverVersion: info.version,
        topology: info.topology,
        connectedAt: Date.now(),
      };
      this.setState(record.id, state);
      return state;
    } catch (err) {
      await safeCleanup(client, tunnel);
      const state: ConnectionState = {
        kind: 'error',
        message: err instanceof Error ? err.message : String(err),
      };
      this.setState(record.id, state);
      return state;
    }
  }

  async disconnect(id: string): Promise<void> {
    const active = this.clients.get(id);
    if (!active) return;
    this.clients.delete(id);
    await safeCleanup(active.client, active.tunnel);
    this.setState(id, { kind: 'disconnected' });
  }

  async disconnectAll(): Promise<void> {
    await Promise.allSettled([...this.clients.keys()].map((id) => this.disconnect(id)));
  }

  async test(record: ConnectionRecord): Promise<TestResult> {
    let tunnel: ActiveTunnel | undefined;
    let client: MongoClient | undefined;
    try {
      let uri = record.uri;
      if (record.ssh) {
        const target = parseFirstHostPort(uri);
        tunnel = await openTunnel(record.ssh, target.host, target.port);
        uri = rewriteUriToLocal(uri, tunnel.localHost, tunnel.localPort);
      }
      client = new MongoClient(uri, {
        serverSelectionTimeoutMS: 6_000,
        connectTimeoutMS: 6_000,
      });
      await client.connect();
      const info = await probeServer(client);
      return {
        ok: true,
        pingMs: info.pingMs,
        serverVersion: info.version,
        topology: info.topology,
      };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    } finally {
      await safeCleanup(client, tunnel);
    }
  }

  getClient(id: string): MongoClient | undefined {
    return this.clients.get(id)?.client;
  }

  private setState(id: string, state: ConnectionState): void {
    this.states.set(id, state);
    for (const l of this.listeners) l(id, state);
  }
}

interface ServerInfo {
  pingMs: number;
  version: string;
  topology: string;
}

async function probeServer(client: MongoClient): Promise<ServerInfo> {
  const admin = client.db('admin');
  const t0 = performance.now();
  await admin.command({ ping: 1 });
  const pingMs = Math.round(performance.now() - t0);

  const buildInfo = (await admin.command({ buildInfo: 1 })) as Document;
  const isMaster = (await admin.command({ hello: 1 })) as Document;

  const version = String(buildInfo['version'] ?? 'unknown');
  const topology = describeTopology(isMaster);
  return { pingMs, version, topology };
}

function describeTopology(hello: Document): string {
  if (hello['msg'] === 'isdbgrid') return 'sharded';
  if (typeof hello['setName'] === 'string') return `replica-set (${hello['setName']})`;
  if (hello['isWritablePrimary'] === true || hello['ismaster'] === true) return 'standalone';
  return 'unknown';
}

function parseFirstHostPort(uri: string): { host: string; port: number } {
  const parsed = new URL(uri);
  const host = parsed.hostname || '127.0.0.1';
  const port = parsed.port ? Number(parsed.port) : 27017;
  return { host, port };
}

async function safeCleanup(client: MongoClient | undefined, tunnel: ActiveTunnel | undefined): Promise<void> {
  if (client) {
    try {
      await client.close(true);
    } catch {
      /* swallow */
    }
  }
  if (tunnel) {
    try {
      await tunnel.close();
    } catch {
      /* swallow */
    }
  }
}
