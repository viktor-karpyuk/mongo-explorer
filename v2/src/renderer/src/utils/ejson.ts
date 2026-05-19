/**
 * Lightweight EJSON helpers that work on the POJO form returned by
 * JSON.parse() of a canonical EJSON string. The main process serializes
 * documents with `EJSON.stringify(doc, { relaxed: false })`, so e.g.
 * `ObjectId` becomes `{ "$oid": "..." }` and `Date` becomes
 * `{ "$date": "..." }`.
 */

export type EjsonValue =
  | string
  | number
  | boolean
  | null
  | EjsonValue[]
  | { [k: string]: EjsonValue };

export interface FormattedValue {
  /** Display label, e.g. `ObjectId("...")`. */
  label: string;
  /** Type tag for styling, e.g. `objectid`, `date`, `number`. */
  type:
    | 'string'
    | 'number'
    | 'boolean'
    | 'null'
    | 'objectid'
    | 'date'
    | 'long'
    | 'decimal'
    | 'binary'
    | 'regex'
    | 'minkey'
    | 'maxkey'
    | 'array'
    | 'object'
    | 'unknown';
}

export function parseRow(s: string): Record<string, EjsonValue> {
  return JSON.parse(s) as Record<string, EjsonValue>;
}

/**
 * Returns a primitive-style label + type for a leaf value. Object/array
 * containers return `array` / `object` so the caller can decide whether to
 * render a summary or recurse.
 */
export function formatValue(v: EjsonValue): FormattedValue {
  if (v === null) return { label: 'null', type: 'null' };
  if (typeof v === 'string') return { label: v, type: 'string' };
  if (typeof v === 'number') return { label: String(v), type: 'number' };
  if (typeof v === 'boolean') return { label: String(v), type: 'boolean' };

  if (Array.isArray(v)) {
    return { label: `[ ${v.length} ]`, type: 'array' };
  }

  // Object: check known EJSON wrappers
  if ('$oid' in v && typeof v.$oid === 'string') {
    return { label: `ObjectId("${v.$oid}")`, type: 'objectid' };
  }
  if ('$date' in v) {
    const raw = v.$date as EjsonValue;
    const value =
      typeof raw === 'string'
        ? raw
        : raw && typeof raw === 'object' && '$numberLong' in raw
          ? new Date(Number((raw as { $numberLong: string }).$numberLong)).toISOString()
          : '?';
    return { label: `ISODate("${value}")`, type: 'date' };
  }
  if ('$numberLong' in v) {
    return { label: `NumberLong(${(v as { $numberLong: string }).$numberLong})`, type: 'long' };
  }
  if ('$numberDecimal' in v) {
    return {
      label: `NumberDecimal("${(v as { $numberDecimal: string }).$numberDecimal}")`,
      type: 'decimal',
    };
  }
  if ('$binary' in v) {
    const binary = v.$binary as { base64?: string; subType?: string };
    return {
      label: `BinData(${binary.subType ?? '0'}, ${(binary.base64 ?? '').slice(0, 12)}…)`,
      type: 'binary',
    };
  }
  if ('$regularExpression' in v) {
    const r = v.$regularExpression as { pattern: string; options: string };
    return { label: `/${r.pattern}/${r.options ?? ''}`, type: 'regex' };
  }
  if ('$minKey' in v) return { label: 'MinKey()', type: 'minkey' };
  if ('$maxKey' in v) return { label: 'MaxKey()', type: 'maxkey' };

  const keys = Object.keys(v);
  return { label: `{ ${keys.length} field${keys.length === 1 ? '' : 's'} }`, type: 'object' };
}

export function isContainer(v: EjsonValue): v is EjsonValue[] | { [k: string]: EjsonValue } {
  return Array.isArray(v) || (typeof v === 'object' && v !== null);
}

export function prettyPrint(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

/**
 * Collects the union of top-level field names across a sample of rows.
 * Preserves first-seen order for stability.
 */
export function collectColumns(rows: string[]): string[] {
  const seen = new Set<string>();
  for (const row of rows) {
    try {
      const obj = JSON.parse(row) as Record<string, unknown>;
      for (const k of Object.keys(obj)) {
        if (!seen.has(k)) seen.add(k);
      }
    } catch {
      /* skip */
    }
  }
  return [...seen];
}
