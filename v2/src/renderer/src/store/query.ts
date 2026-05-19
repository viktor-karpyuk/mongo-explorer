import { create } from 'zustand';
import type { FindRequest, FindResponse } from '../../../shared/query.js';

export interface QueryDraft {
  filter: string;
  projection: string;
  sort: string;
  skip: number;
  limit: number;
  maxTimeMs: number;
}

interface QueryStore {
  draftByNs: Record<string, QueryDraft>;
  resultByNs: Record<string, FindResponse | null>;
  runningByNs: Record<string, boolean>;

  getDraft: (ns: string) => QueryDraft;
  setDraft: (ns: string, patch: Partial<QueryDraft>) => void;
  run: (
    ns: string,
    base: { connectionId: string; db: string; collection: string },
  ) => Promise<void>;
  nextPage: (
    ns: string,
    base: { connectionId: string; db: string; collection: string },
  ) => Promise<void>;
  prevPage: (
    ns: string,
    base: { connectionId: string; db: string; collection: string },
  ) => Promise<void>;
}

const defaultDraft = (): QueryDraft => ({
  filter: '{}',
  projection: '',
  sort: '',
  skip: 0,
  limit: 50,
  maxTimeMs: 30_000,
});

export function nsKey(connectionId: string, db: string, collection: string): string {
  return `${connectionId}::${db}.${collection}`;
}

export const useQueryStore = create<QueryStore>((set, get) => ({
  draftByNs: {},
  resultByNs: {},
  runningByNs: {},

  getDraft: (ns) => get().draftByNs[ns] ?? defaultDraft(),

  setDraft: (ns, patch) => {
    set((s) => ({
      draftByNs: {
        ...s.draftByNs,
        [ns]: { ...(s.draftByNs[ns] ?? defaultDraft()), ...patch },
      },
    }));
  },

  run: async (ns, base) => {
    const draft = get().draftByNs[ns] ?? defaultDraft();
    if (!get().draftByNs[ns]) {
      set((s) => ({ draftByNs: { ...s.draftByNs, [ns]: draft } }));
    }
    set((s) => ({ runningByNs: { ...s.runningByNs, [ns]: true } }));
    const req: FindRequest = {
      connectionId: base.connectionId,
      db: base.db,
      collection: base.collection,
      filter: draft.filter,
      projection: draft.projection,
      sort: draft.sort,
      skip: draft.skip,
      limit: draft.limit,
      maxTimeMs: draft.maxTimeMs,
    };
    const result = await window.mex.query.find(req);
    set((s) => ({
      resultByNs: { ...s.resultByNs, [ns]: result },
      runningByNs: { ...s.runningByNs, [ns]: false },
    }));
  },

  nextPage: async (ns, base) => {
    const draft = get().draftByNs[ns] ?? defaultDraft();
    get().setDraft(ns, { skip: draft.skip + draft.limit });
    await get().run(ns, base);
  },

  prevPage: async (ns, base) => {
    const draft = get().draftByNs[ns] ?? defaultDraft();
    const nextSkip = Math.max(0, draft.skip - draft.limit);
    get().setDraft(ns, { skip: nextSkip });
    await get().run(ns, base);
  },
}));
