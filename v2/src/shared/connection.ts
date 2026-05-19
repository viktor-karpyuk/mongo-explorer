export interface SshTunnel {
  host: string;
  port: number;
  user: string;
  keyPath: string;
}

export interface ConnectionInput {
  name: string;
  uri: string;
  ssh?: SshTunnel;
  notes?: string;
}

export interface ConnectionRecord {
  id: string;
  name: string;
  uri: string;
  ssh?: SshTunnel;
  notes?: string;
  createdAt: number;
  updatedAt: number;
  lastUsedAt?: number;
}

export interface ConnectionSummary {
  id: string;
  name: string;
  hasSsh: boolean;
  notes?: string;
  createdAt: number;
  updatedAt: number;
  lastUsedAt?: number;
}
