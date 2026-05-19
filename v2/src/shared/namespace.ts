export interface DatabaseInfo {
  name: string;
  sizeOnDisk?: number;
  empty?: boolean;
}

export type CollectionType = 'collection' | 'view' | 'timeseries';

export interface CollectionInfo {
  name: string;
  type: CollectionType;
  options?: Record<string, unknown>;
}

export interface DbStats {
  db: string;
  collections: number;
  views: number;
  objects: number;
  avgObjSize: number;
  dataSize: number;
  storageSize: number;
  indexes: number;
  indexSize: number;
  totalSize: number;
}

export interface CollStats {
  ns: string;
  count: number;
  size: number;
  avgObjSize: number;
  storageSize: number;
  totalIndexSize: number;
  nindexes: number;
  capped: boolean;
}

export interface CreateCollectionInput {
  name: string;
  capped?: { sizeBytes: number; maxDocs?: number };
  timeseries?: {
    timeField: string;
    metaField?: string;
    granularity?: 'seconds' | 'minutes' | 'hours';
  };
  validator?: Record<string, unknown>;
  validationLevel?: 'off' | 'moderate' | 'strict';
  validationAction?: 'warn' | 'error';
}
