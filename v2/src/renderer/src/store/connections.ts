import { create } from 'zustand';
import type {
  ConnectionInput,
  ConnectionSummary,
} from '../../../shared/connection.js';
import type { ConnectionState } from '../../../shared/connectionState.js';

interface ConnectionsStore {
  list: ConnectionSummary[];
  states: Record<string, ConnectionState>;
  loaded: boolean;

  load: () => Promise<void>;
  create: (input: ConnectionInput) => Promise<ConnectionSummary>;
  update: (id: string, input: ConnectionInput) => Promise<void>;
  remove: (id: string) => Promise<void>;
  duplicate: (id: string) => Promise<void>;
  open: (id: string) => Promise<ConnectionState>;
  close: (id: string) => Promise<void>;

  _setState: (id: string, state: ConnectionState) => void;
}

export const useConnectionsStore = create<ConnectionsStore>((set, get) => ({
  list: [],
  states: {},
  loaded: false,

  load: async () => {
    const [list, states] = await Promise.all([
      window.mex.connections.list(),
      window.mex.connections.states(),
    ]);
    const statesMap: Record<string, ConnectionState> = {};
    for (const s of states) statesMap[s.id] = s.state;
    set({ list, states: statesMap, loaded: true });
  },

  create: async (input) => {
    const created = await window.mex.connections.create(input);
    set((s) => ({ list: [created, ...s.list] }));
    return created;
  },

  update: async (id, input) => {
    const updated = await window.mex.connections.update(id, input);
    if (!updated) return;
    set((s) => ({ list: s.list.map((c) => (c.id === id ? updated : c)) }));
  },

  remove: async (id) => {
    const ok = await window.mex.connections.delete(id);
    if (!ok) return;
    set((s) => {
      const states = { ...s.states };
      delete states[id];
      return { list: s.list.filter((c) => c.id !== id), states };
    });
  },

  duplicate: async (id) => {
    const dup = await window.mex.connections.duplicate(id);
    if (!dup) return;
    set((s) => ({ list: [dup, ...s.list] }));
  },

  open: async (id) => {
    const state = await window.mex.connections.open(id);
    get()._setState(id, state);
    return state;
  },

  close: async (id) => {
    await window.mex.connections.close(id);
    get()._setState(id, { kind: 'disconnected' });
  },

  _setState: (id, state) => {
    set((s) => ({ states: { ...s.states, [id]: state } }));
  },
}));

export function subscribeConnectionState(): () => void {
  return window.mex.connections.onStateChange(({ id, state }) => {
    useConnectionsStore.getState()._setState(id, state);
  });
}
