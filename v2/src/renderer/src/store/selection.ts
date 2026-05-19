import { create } from 'zustand';

export type Selection =
  | { kind: 'welcome' }
  | { kind: 'database'; connectionId: string; db: string }
  | { kind: 'collection'; connectionId: string; db: string; collection: string };

interface SelectionStore {
  current: Selection;
  select: (s: Selection) => void;
}

export const useSelectionStore = create<SelectionStore>((set) => ({
  current: { kind: 'welcome' },
  select: (s) => set({ current: s }),
}));
