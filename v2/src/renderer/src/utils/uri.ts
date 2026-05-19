export interface ParsedUri {
  protocol: string;
  host: string;
  port: number | null;
  database: string | null;
}

export function parseMongoUri(uri: string): ParsedUri | null {
  try {
    const parsed = new URL(uri);
    if (!/^mongodb(\+srv)?:$/.test(parsed.protocol)) return null;
    return {
      protocol: parsed.protocol.replace(':', ''),
      host: parsed.hostname,
      port: parsed.port ? Number(parsed.port) : null,
      database: parsed.pathname.replace(/^\//, '') || null,
    };
  } catch {
    return null;
  }
}

export function formatUriPreview(uri: string): string {
  const p = parseMongoUri(uri);
  if (!p) return uri;
  const port = p.port ? `:${p.port}` : '';
  const db = p.database ? `/${p.database}` : '';
  return `${p.protocol}://${p.host}${port}${db}`;
}
