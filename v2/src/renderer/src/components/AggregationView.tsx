import { useEffect } from 'react';
import {
  STAGE_OPERATORS,
  TEMPLATES,
  type AggregationStage,
} from '../../../shared/aggregation.js';
import { useAggStore } from '../store/aggregation';
import { nsKey } from '../store/query';
import { ResultsPane } from './results/ResultsPane';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
}

export function AggregationView({ connectionId, db, collection }: Props) {
  const ns = nsKey(connectionId, db, collection);
  const draft =
    useAggStore((s) => s.draftByNs[ns]) ?? useAggStore.getState().getDraft(ns);
  const result = useAggStore((s) => s.resultByNs[ns]) ?? null;
  const running = useAggStore((s) => s.runningByNs[ns]) ?? false;

  const addStage = useAggStore((s) => s.addStage);
  const removeStage = useAggStore((s) => s.removeStage);
  const moveStage = useAggStore((s) => s.moveStage);
  const toggleStage = useAggStore((s) => s.toggleStage);
  const setStageOperator = useAggStore((s) => s.setStageOperator);
  const setStageBody = useAggStore((s) => s.setStageBody);
  const loadTemplate = useAggStore((s) => s.loadTemplate);
  const setLimit = useAggStore((s) => s.setLimit);
  const setMaxTimeMs = useAggStore((s) => s.setMaxTimeMs);
  const run = useAggStore((s) => s.run);

  const base = { connectionId, db, collection };

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        void run(ns, base);
      }
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ns, connectionId, db, collection]);

  return (
    <div className="agg-view">
      <div className="agg-view__head">
        <strong>Pipeline · {db}.{collection}</strong>
        <select
          className="agg-template-picker"
          value=""
          onChange={(e) => {
            const t = TEMPLATES.find((t) => t.name === e.target.value);
            if (t) loadTemplate(ns, t);
            e.target.selectedIndex = 0;
          }}
        >
          <option value="">Load template…</option>
          {TEMPLATES.map((t) => (
            <option key={t.name} value={t.name}>
              {t.name} — {t.description}
            </option>
          ))}
        </select>
        <div className="spacer" />
        <label className="inline-field">
          <span>Limit</span>
          <input
            type="number"
            min={1}
            value={draft.limit}
            onChange={(e) => setLimit(ns, Number(e.target.value) || 50)}
          />
        </label>
        <label className="inline-field">
          <span>maxTimeMs</span>
          <input
            type="number"
            min={0}
            value={draft.maxTimeMs}
            onChange={(e) => setMaxTimeMs(ns, Number(e.target.value) || 60_000)}
          />
        </label>
        <button
          className="btn btn--primary"
          onClick={() => void run(ns, base)}
          disabled={running || draft.stages.length === 0}
        >
          {running ? 'Running…' : 'Run pipeline ⌘↵'}
        </button>
      </div>

      <div className="agg-view__stages">
        {draft.stages.map((stage, i) => (
          <StageRow
            key={stage.id}
            index={i}
            stage={stage}
            count={draft.stages.length}
            onMove={(dir) => moveStage(ns, stage.id, dir)}
            onRemove={() => removeStage(ns, stage.id)}
            onToggle={() => toggleStage(ns, stage.id)}
            onOperator={(op) => setStageOperator(ns, stage.id, op)}
            onBody={(b) => setStageBody(ns, stage.id, b)}
            onRunToHere={() => void run(ns, base, i)}
          />
        ))}
        <button className="btn btn--ghost agg-view__add" onClick={() => addStage(ns)}>
          + Add stage
        </button>
      </div>

      <ResultsPane
        result={result}
        running={running}
        skip={0}
        limit={draft.limit}
        onPrev={() => {}}
        onNext={() => {}}
      />
    </div>
  );
}

interface StageRowProps {
  index: number;
  stage: AggregationStage;
  count: number;
  onMove: (dir: -1 | 1) => void;
  onRemove: () => void;
  onToggle: () => void;
  onOperator: (op: string) => void;
  onBody: (b: string) => void;
  onRunToHere: () => void;
}

function StageRow({
  index,
  stage,
  count,
  onMove,
  onRemove,
  onToggle,
  onOperator,
  onBody,
  onRunToHere,
}: StageRowProps) {
  return (
    <div className={`stage${stage.enabled ? '' : ' stage--disabled'}`}>
      <div className="stage__head">
        <span className="stage__index">{index + 1}</span>
        <select
          className="stage__op"
          value={stage.operator}
          onChange={(e) => onOperator(e.target.value)}
        >
          {STAGE_OPERATORS.map((op) => (
            <option key={op} value={op}>
              {op}
            </option>
          ))}
        </select>
        <button className="btn btn--ghost btn--xs" onClick={onToggle}>
          {stage.enabled ? 'Disable' : 'Enable'}
        </button>
        <button
          className="btn btn--ghost btn--xs"
          onClick={() => onMove(-1)}
          disabled={index === 0}
        >
          ↑
        </button>
        <button
          className="btn btn--ghost btn--xs"
          onClick={() => onMove(1)}
          disabled={index >= count - 1}
        >
          ↓
        </button>
        <button className="btn btn--ghost btn--xs" onClick={onRunToHere}>
          Run to here
        </button>
        <div className="spacer" />
        <button
          className="btn btn--ghost btn--xs btn--danger"
          onClick={onRemove}
        >
          ✕
        </button>
      </div>
      <textarea
        className="stage__body"
        value={stage.body}
        onChange={(e) => onBody(e.target.value)}
        spellCheck={false}
        rows={3}
      />
    </div>
  );
}
