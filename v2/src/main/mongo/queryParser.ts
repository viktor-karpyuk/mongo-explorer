import {
  parseFilter as rawParseFilter,
  parseProject as rawParseProject,
  parseSort as rawParseSort,
} from 'mongodb-query-parser';

export type ParsedFilter = Record<string, unknown>;
export type ParsedProjection = Record<string, unknown>;
export type ParsedSort = Record<string, 1 | -1>;

export function parseFilter(input: string | undefined | null): ParsedFilter {
  const trimmed = (input ?? '').trim();
  if (!trimmed) return {};
  const expanded = expandObjectIdShorthand(trimmed);
  const result = rawParseFilter(expanded) as unknown;
  if (result === undefined || result === null) {
    throw new Error('Filter did not parse to a valid query document.');
  }
  return result as ParsedFilter;
}

export function parseProjection(input: string | undefined | null): ParsedProjection {
  const trimmed = (input ?? '').trim();
  if (!trimmed) return {};
  const result = rawParseProject(trimmed) as unknown;
  if (result === undefined || result === null) {
    throw new Error('Projection did not parse to a valid document.');
  }
  return result as ParsedProjection;
}

export function parseSort(input: string | undefined | null): ParsedSort {
  const trimmed = (input ?? '').trim();
  if (!trimmed) return {};
  const result = rawParseSort(trimmed) as unknown;
  if (result === undefined || result === null) {
    throw new Error('Sort did not parse to a valid document.');
  }
  return result as ParsedSort;
}

/**
 * If the filter is a 24-hex string, treat it as a shorthand for
 * `{ _id: ObjectId("...") }`. This matches the convenience seen in
 * Compass / Studio 3T.
 */
function expandObjectIdShorthand(input: string): string {
  if (/^[a-fA-F0-9]{24}$/.test(input)) {
    return `{ _id: ObjectId("${input}") }`;
  }
  return input;
}
