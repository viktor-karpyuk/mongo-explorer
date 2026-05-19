import { useState } from 'react';
import { formatValue, isContainer, parseRow, type EjsonValue } from '../../utils/ejson';

interface Props {
  rows: string[];
  onEdit?: (doc: string) => void;
  onDelete?: (doc: string) => void;
}

export function ResultTree({ rows, onEdit, onDelete }: Props) {
  return (
    <div className="result-tree">
      {rows.map((row, i) => {
        let parsed: Record<string, EjsonValue> | null;
        try {
          parsed = parseRow(row);
        } catch {
          parsed = null;
        }
        return (
          <DocBlock
            key={i}
            index={i + 1}
            raw={row}
            value={parsed ?? { _: row as unknown as EjsonValue }}
            onEdit={onEdit}
            onDelete={onDelete}
          />
        );
      })}
    </div>
  );
}

function DocBlock({
  index,
  raw,
  value,
  onEdit,
  onDelete,
}: {
  index: number;
  raw: string;
  value: Record<string, EjsonValue>;
  onEdit?: (doc: string) => void;
  onDelete?: (doc: string) => void;
}) {
  return (
    <div className="result-tree__doc">
      <div className="result-tree__header">
        <span>Document {index}</span>
        <div className="spacer" />
        {onEdit && (
          <button className="btn btn--ghost btn--xs" onClick={() => onEdit(raw)}>
            Edit
          </button>
        )}
        {onDelete && (
          <button className="btn btn--ghost btn--xs btn--danger" onClick={() => onDelete(raw)}>
            Delete
          </button>
        )}
      </div>
      <Node label="" value={value} depth={0} root />
    </div>
  );
}

interface NodeProps {
  label: string;
  value: EjsonValue;
  depth: number;
  root?: boolean;
}

function Node({ label, value, depth, root }: NodeProps) {
  const [open, setOpen] = useState(depth < 1 || !!root);

  if (!isContainer(value) || isLeafContainer(value)) {
    const f = formatValue(value);
    return (
      <div className="result-tree__row" style={{ paddingLeft: depth * 16 + 8 }}>
        {label && <span className="result-tree__key">{label}:</span>}{' '}
        <span className={`cell cell--${f.type}`}>{f.label}</span>
      </div>
    );
  }

  const entries = Array.isArray(value)
    ? value.map((v, i) => [String(i), v] as const)
    : Object.entries(value);

  return (
    <div>
      <div
        className="result-tree__row result-tree__row--expandable"
        style={{ paddingLeft: depth * 16 + 8 }}
        onClick={() => setOpen(!open)}
      >
        <span className="result-tree__caret">{open ? '▾' : '▸'}</span>{' '}
        {label && <span className="result-tree__key">{label}:</span>}{' '}
        <span className="muted">
          {Array.isArray(value) ? `array(${entries.length})` : `{ ${entries.length} }`}
        </span>
      </div>
      {open &&
        entries.map(([k, v]) => (
          <Node key={k} label={k} value={v as EjsonValue} depth={depth + 1} />
        ))}
    </div>
  );
}

function isLeafContainer(v: EjsonValue): boolean {
  // EJSON wrappers are containers in JS but should render as leaves.
  if (Array.isArray(v) || v === null || typeof v !== 'object') return false;
  return (
    '$oid' in v ||
    '$date' in v ||
    '$numberLong' in v ||
    '$numberDecimal' in v ||
    '$binary' in v ||
    '$regularExpression' in v ||
    '$minKey' in v ||
    '$maxKey' in v
  );
}
