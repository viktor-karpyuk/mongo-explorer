import { create } from 'zustand';
import { ulid } from 'ulid';
import type {
  AggregateRequest,
  AggregateResponse,
  AggregationStage,
  AggregationTemplate,
} from '../../../shared/aggregation.js';

interface AggDraft {
  stages: AggregationStage[];
  limit: number;
  maxTimeMs: number;
}

interface AggStore {
  draftByNs: Record<string, AggDraft>;
  resultByNs: Record<string, AggregateResponse | null>;
  runningByNs: Record<string, boolean>;
  /** Index of the stage selected as the "run-to-here" target, or -1 for full. */
  runToByNs: Record<string, number>;

  getDraft: (ns: string) => AggDraft;
  setStages: (ns: string, stages: AggregationStage[]) => void;
  addStage: (ns: string, after?: number) => void;
  removeStage: (ns: string, id: string) => void;
  moveStage: (ns: string, id: string, dir: -1 | 1) => void;
  toggleStage: (ns: string, id: string) => void;
  setStageOperator: (ns: string, id: string, operator: string) => void;
  setStageBody: (ns: string, id: string, body: string) => void;
  loadTemplate: (ns: string, t: AggregationTemplate) => void;
  setLimit: (ns: string, limit: number) => void;
  setMaxTimeMs: (ns: string, ms: number) => void;
  run: (
    ns: string,
    base: { connectionId: string; db: string; collection: string },
    upToIndex?: number,
  ) => Promise<void>;
}

const defaultDraft = (): AggDraft => ({
  stages: [{ id: ulid(), operator: '$match', body: '{}', enabled: true }],
  limit: 50,
  maxTimeMs: 60_000,
});

function patchDraft(state: AggStore, ns: string, mutator: (d: AggDraft) => AggDraft) {
  const current = state.draftByNs[ns] ?? defaultDraft();
  return { draftByNs: { ...state.draftByNs, [ns]: mutator(current) } };
}

export const useAggStore = create<AggStore>((set, get) => ({
  draftByNs: {},
  resultByNs: {},
  runningByNs: {},
  runToByNs: {},

  getDraft: (ns) => get().draftByNs[ns] ?? defaultDraft(),

  setStages: (ns, stages) =>
    set((s) => patchDraft(s, ns, (d) => ({ ...d, stages }))),

  addStage: (ns, after) =>
    set((s) =>
      patchDraft(s, ns, (d) => {
        const next: AggregationStage = {
          id: ulid(),
          operator: '$match',
          body: '{}',
          enabled: true,
        };
        if (after === undefined || after < 0) {
          return { ...d, stages: [...d.stages, next] };
        }
        const stages = [...d.stages];
        stages.splice(after + 1, 0, next);
        return { ...d, stages };
      }),
    ),

  removeStage: (ns, id) =>
    set((s) =>
      patchDraft(s, ns, (d) => ({
        ...d,
        stages: d.stages.filter((st) => st.id !== id),
      })),
    ),

  moveStage: (ns, id, dir) =>
    set((s) =>
      patchDraft(s, ns, (d) => {
        const i = d.stages.findIndex((st) => st.id === id);
        if (i < 0) return d;
        const j = i + dir;
        if (j < 0 || j >= d.stages.length) return d;
        const stages = [...d.stages];
        [stages[i], stages[j]] = [stages[j]!, stages[i]!];
        return { ...d, stages };
      }),
    ),

  toggleStage: (ns, id) =>
    set((s) =>
      patchDraft(s, ns, (d) => ({
        ...d,
        stages: d.stages.map((st) =>
          st.id === id ? { ...st, enabled: !st.enabled } : st,
        ),
      })),
    ),

  setStageOperator: (ns, id, operator) =>
    set((s) =>
      patchDraft(s, ns, (d) => ({
        ...d,
        stages: d.stages.map((st) => (st.id === id ? { ...st, operator } : st)),
      })),
    ),

  setStageBody: (ns, id, body) =>
    set((s) =>
      patchDraft(s, ns, (d) => ({
        ...d,
        stages: d.stages.map((st) => (st.id === id ? { ...st, body } : st)),
      })),
    ),

  loadTemplate: (ns, t) =>
    set((s) =>
      patchDraft(s, ns, (d) => ({
        ...d,
        stages: t.stages.map((stage) => ({
          id: ulid(),
          operator: stage.operator,
          body: stage.body,
          enabled: true,
        })),
      })),
    ),

  setLimit: (ns, limit) =>
    set((s) => patchDraft(s, ns, (d) => ({ ...d, limit }))),

  setMaxTimeMs: (ns, ms) =>
    set((s) => patchDraft(s, ns, (d) => ({ ...d, maxTimeMs: ms }))),

  run: async (ns, base, upToIndex) => {
    const draft = get().draftByNs[ns] ?? defaultDraft();
    if (!get().draftByNs[ns]) {
      set((s) => ({ draftByNs: { ...s.draftByNs, [ns]: draft } }));
    }
    set((s) => ({
      runningByNs: { ...s.runningByNs, [ns]: true },
      runToByNs: { ...s.runToByNs, [ns]: upToIndex ?? -1 },
    }));

    const allStages = draft.stages;
    const sliced = upToIndex === undefined ? allStages : allStages.slice(0, upToIndex + 1);
    const enabled = sliced.filter((s) => s.enabled);

    const req: AggregateRequest = {
      connectionId: base.connectionId,
      db: base.db,
      collection: base.collection,
      pipeline: enabled.map((s) => ({ operator: s.operator, body: s.body })),
      limit: draft.limit,
      maxTimeMs: draft.maxTimeMs,
    };
    const result = await window.mex.query.aggregate(req);
    set((s) => ({
      resultByNs: { ...s.resultByNs, [ns]: result },
      runningByNs: { ...s.runningByNs, [ns]: false },
    }));
  },
}));
