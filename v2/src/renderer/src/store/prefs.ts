import { create } from 'zustand';

export type Theme = 'dark' | 'light';

export interface Prefs {
  theme: Theme;
  editorFontSize: number;
  tabWidth: number;
}

interface PrefsStore {
  prefs: Prefs;
  setTheme: (t: Theme) => void;
  setEditorFontSize: (n: number) => void;
  setTabWidth: (n: number) => void;
}

const STORAGE_KEY = 'mex-prefs';

const DEFAULT_PREFS: Prefs = {
  theme: 'dark',
  editorFontSize: 12.5,
  tabWidth: 2,
};

function load(): Prefs {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return DEFAULT_PREFS;
    return { ...DEFAULT_PREFS, ...(JSON.parse(raw) as Partial<Prefs>) };
  } catch {
    return DEFAULT_PREFS;
  }
}

function persist(prefs: Prefs) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    /* ignore quota */
  }
}

function applyToDom(prefs: Prefs) {
  document.documentElement.dataset['theme'] = prefs.theme;
  document.documentElement.style.setProperty(
    '--editor-font-size',
    `${prefs.editorFontSize}px`,
  );
}

export const usePrefsStore = create<PrefsStore>((set) => {
  const initial = load();
  applyToDom(initial);
  return {
    prefs: initial,
    setTheme: (theme) =>
      set((s) => {
        const next = { ...s.prefs, theme };
        persist(next);
        applyToDom(next);
        return { prefs: next };
      }),
    setEditorFontSize: (editorFontSize) =>
      set((s) => {
        const clamped = Math.max(10, Math.min(20, editorFontSize));
        const next = { ...s.prefs, editorFontSize: clamped };
        persist(next);
        applyToDom(next);
        return { prefs: next };
      }),
    setTabWidth: (tabWidth) =>
      set((s) => {
        const next = { ...s.prefs, tabWidth: Math.max(2, Math.min(8, tabWidth)) };
        persist(next);
        applyToDom(next);
        return { prefs: next };
      }),
  };
});
