import { useMemo, useRef } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { collectColumns, formatValue, parseRow, type EjsonValue } from '../../utils/ejson';

const ROW_HEIGHT = 28;

interface Props {
  rows: string[];
}

export function ResultTable({ rows }: Props) {
  const parsed = useMemo(() => {
    return rows.map((r) => {
      try {
        return parseRow(r);
      } catch {
        return null;
      }
    });
  }, [rows]);

  const columns = useMemo(() => collectColumns(rows), [rows]);

  const containerRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count: parsed.length,
    getScrollElement: () => containerRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 12,
  });

  if (columns.length === 0) {
    return <div className="result-empty">No documents to render.</div>;
  }

  return (
    <div className="result-table-wrap" ref={containerRef}>
      <table className="result-table">
        <thead>
          <tr>
            <th className="result-table__index">#</th>
            {columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody style={{ height: virtualizer.getTotalSize() }}>
          {virtualizer.getVirtualItems().map((vrow) => {
            const doc = parsed[vrow.index];
            return (
              <tr
                key={vrow.key}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  height: ROW_HEIGHT,
                  transform: `translateY(${vrow.start}px)`,
                }}
              >
                <td className="result-table__index">{vrow.index + 1}</td>
                {columns.map((c) => (
                  <td key={c}>
                    <Cell value={doc?.[c] as EjsonValue | undefined} />
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Cell({ value }: { value: EjsonValue | undefined }) {
  if (value === undefined) return <span className="cell--missing">—</span>;
  const f = formatValue(value);
  return <span className={`cell cell--${f.type}`}>{f.label}</span>;
}
