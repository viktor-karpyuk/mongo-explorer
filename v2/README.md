# Mongo Explorer v2

Performance-focused rewrite. Electron + Node.js + React + TypeScript.

Status: **0.1.0-alpha** — Phase A scaffold.

## Layout

```
v2/
├── electron.vite.config.ts   # build config
├── tsconfig.{node,web}.json  # split TS projects (main/preload vs renderer)
├── FEATURES.md               # locked feature cut (~85 features)
└── src/
    ├── main/      # Electron main process (Node)
    ├── preload/   # contextBridge layer
    ├── shared/    # types shared between main and renderer
    └── renderer/  # React UI
```

## Develop

```bash
npm install
npm run dev       # launches Electron with HMR
npm run typecheck # both node + web TS projects
npm run build     # production bundle
```

## Architecture decisions

- **Strict process separation**: renderer is sandboxed, `nodeIntegration: false`,
  `contextIsolation: true`. All Node/Mongo I/O lives in main and reaches the
  renderer only via the typed `mex` bridge defined in `src/shared/ipc.ts`.
- **No remote module, no IPC in shared state**. Renderer treats main as a service.
- **Native deps stay in main**: `mongodb`, `better-sqlite3`, `keytar`, `ssh2`
  are imported from main only — `externalizeDepsPlugin` keeps them off the
  renderer bundle.
- **Performance budget**: cold start under 1.5 s, Welcome paint under 200 ms,
  result table virtualized for ≥ 1M rows.

## Phase plan

See `FEATURES.md` for the locked scope. Phases A → O are tracked in the
session task list.
