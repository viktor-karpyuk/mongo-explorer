import { create } from 'zustand';
import type {
  CollectionInfo,
  DatabaseInfo,
} from '../../../shared/namespace.js';

interface CachedDb {
  loading: boolean;
  databases: DatabaseInfo[];
  collections: Record<string, CollectionInfo[]>;
  collectionsLoading: Record<string, boolean>;
  expanded: Set<string>;
}

interface NamespacesStore {
  byConnection: Record<string, CachedDb>;

  loadDatabases: (connectionId: string) => Promise<void>;
  loadCollections: (connectionId: string, db: string) => Promise<void>;
  toggleExpanded: (connectionId: string, db: string) => Promise<void>;
  reset: (connectionId: string) => void;
  refresh: (connectionId: string) => Promise<void>;
}

function emptyCache(): CachedDb {
  return {
    loading: false,
    databases: [],
    collections: {},
    collectionsLoading: {},
    expanded: new Set(),
  };
}

export const useNamespacesStore = create<NamespacesStore>((set, get) => ({
  byConnection: {},

  loadDatabases: async (connectionId) => {
    set((s) => {
      const existing = s.byConnection[connectionId] ?? emptyCache();
      return {
        byConnection: { ...s.byConnection, [connectionId]: { ...existing, loading: true } },
      };
    });
    try {
      const databases = await window.mex.ns.listDatabases(connectionId);
      set((s) => {
        const existing = s.byConnection[connectionId] ?? emptyCache();
        return {
          byConnection: {
            ...s.byConnection,
            [connectionId]: { ...existing, loading: false, databases },
          },
        };
      });
    } catch {
      set((s) => {
        const existing = s.byConnection[connectionId] ?? emptyCache();
        return {
          byConnection: {
            ...s.byConnection,
            [connectionId]: { ...existing, loading: false },
          },
        };
      });
    }
  },

  loadCollections: async (connectionId, db) => {
    set((s) => {
      const existing = s.byConnection[connectionId] ?? emptyCache();
      return {
        byConnection: {
          ...s.byConnection,
          [connectionId]: {
            ...existing,
            collectionsLoading: { ...existing.collectionsLoading, [db]: true },
          },
        },
      };
    });
    try {
      const collections = await window.mex.ns.listCollections(connectionId, db);
      set((s) => {
        const existing = s.byConnection[connectionId] ?? emptyCache();
        return {
          byConnection: {
            ...s.byConnection,
            [connectionId]: {
              ...existing,
              collections: { ...existing.collections, [db]: collections },
              collectionsLoading: { ...existing.collectionsLoading, [db]: false },
            },
          },
        };
      });
    } catch {
      set((s) => {
        const existing = s.byConnection[connectionId] ?? emptyCache();
        return {
          byConnection: {
            ...s.byConnection,
            [connectionId]: {
              ...existing,
              collectionsLoading: { ...existing.collectionsLoading, [db]: false },
            },
          },
        };
      });
    }
  },

  toggleExpanded: async (connectionId, db) => {
    const cache = get().byConnection[connectionId] ?? emptyCache();
    const expanded = new Set(cache.expanded);
    if (expanded.has(db)) {
      expanded.delete(db);
    } else {
      expanded.add(db);
      if (!cache.collections[db]) {
        void get().loadCollections(connectionId, db);
      }
    }
    set((s) => ({
      byConnection: {
        ...s.byConnection,
        [connectionId]: { ...cache, expanded },
      },
    }));
  },

  reset: (connectionId) => {
    set((s) => {
      const next = { ...s.byConnection };
      delete next[connectionId];
      return { byConnection: next };
    });
  },

  refresh: async (connectionId) => {
    const cache = get().byConnection[connectionId];
    await get().loadDatabases(connectionId);
    if (cache) {
      await Promise.all(
        [...cache.expanded].map((db) => get().loadCollections(connectionId, db)),
      );
    }
  },
}));
