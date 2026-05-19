import { useMemo } from 'react';
import { prettyPrint } from '../../utils/ejson';

export function ResultJson({ rows }: { rows: string[] }) {
  const text = useMemo(() => rows.map(prettyPrint).join('\n\n'), [rows]);
  return <pre className="result-json">{text}</pre>;
}
