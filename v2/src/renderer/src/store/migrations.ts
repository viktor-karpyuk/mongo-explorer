import { create } from 'zustand';
import type {
  MigrationJob,
  MigrationProgress,
  MigrationSpec,
} from '../../../shared/migration.js';

interface MigrationsStore {
  jobs: MigrationJob[];
  progress: Record<string, MigrationProgress>;
  loaded: boolean;

  load: () => Promise<void>;
  create: (spec: MigrationSpec) => Promise<MigrationJob>;
  start: (jobId: string) => Promise<void>;
  pause: (jobId: string) => Promise<void>;
  cancel: (jobId: string) => Promise<void>;
  remove: (jobId: string) => Promise<void>;

  _setProgress: (event: MigrationProgress) => void;
}

export const useMigrationsStore = create<MigrationsStore>((set, get) => ({
  jobs: [],
  progress: {},
  loaded: false,

  load: async () => {
    const jobs = await window.mex.migrate.list();
    set({ jobs, loaded: true });
  },

  create: async (spec) => {
    const job = await window.mex.migrate.create(spec);
    set((s) => ({ jobs: [job, ...s.jobs] }));
    return job;
  },

  start: async (jobId) => {
    await window.mex.migrate.start(jobId);
    await get().load();
  },

  pause: async (jobId) => {
    await window.mex.migrate.pause(jobId);
    await get().load();
  },

  cancel: async (jobId) => {
    await window.mex.migrate.cancel(jobId);
    await get().load();
  },

  remove: async (jobId) => {
    await window.mex.migrate.delete(jobId);
    set((s) => ({ jobs: s.jobs.filter((j) => j.id !== jobId) }));
  },

  _setProgress: (event) => {
    set((s) => {
      const jobs = s.jobs.map((j) =>
        j.id === event.jobId
          ? { ...j, status: event.status, error: event.error ?? j.error }
          : j,
      );
      return { jobs, progress: { ...s.progress, [event.jobId]: event } };
    });
  },
}));

export function subscribeMigrationProgress(): () => void {
  return window.mex.migrate.onProgress((event) => {
    useMigrationsStore.getState()._setProgress(event);
  });
}
