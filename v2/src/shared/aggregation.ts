export interface AggregationStage {
  id: string;
  operator: string;
  body: string;
  enabled: boolean;
}

export interface AggregateRequest {
  connectionId: string;
  db: string;
  collection: string;
  pipeline: { operator: string; body: string }[];
  limit: number;
  maxTimeMs?: number;
}

export interface AggregateResult {
  ok: true;
  rows: string[];
  hasMore: boolean;
  durationMs: number;
}

export interface AggregateError {
  ok: false;
  error: string;
  durationMs: number;
}

export type AggregateResponse = AggregateResult | AggregateError;

export const STAGE_OPERATORS = [
  '$match',
  '$project',
  '$group',
  '$sort',
  '$limit',
  '$skip',
  '$unwind',
  '$lookup',
  '$count',
  '$addFields',
  '$set',
  '$unset',
  '$replaceRoot',
  '$facet',
  '$bucket',
  '$bucketAuto',
  '$sortByCount',
  '$sample',
  '$merge',
  '$out',
  '$redact',
  '$geoNear',
  '$graphLookup',
  '$changeStream',
] as const;

export interface AggregationTemplate {
  name: string;
  description: string;
  stages: { operator: string; body: string }[];
}

export const TEMPLATES: AggregationTemplate[] = [
  {
    name: 'Total count',
    description: 'How many documents match the filter?',
    stages: [{ operator: '$count', body: '"total"' }],
  },
  {
    name: 'Top-K by field',
    description: 'Most frequent values of a field.',
    stages: [
      { operator: '$group', body: '{ _id: "$field", n: { $sum: 1 } }' },
      { operator: '$sort', body: '{ n: -1 }' },
      { operator: '$limit', body: '10' },
    ],
  },
  {
    name: 'Latest N documents',
    description: 'Most recent by createdAt.',
    stages: [
      { operator: '$sort', body: '{ createdAt: -1 }' },
      { operator: '$limit', body: '10' },
    ],
  },
  {
    name: 'Daily date histogram',
    description: 'Count per day for createdAt.',
    stages: [
      {
        operator: '$group',
        body: '{ _id: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } }, n: { $sum: 1 } }',
      },
      { operator: '$sort', body: '{ _id: 1 }' },
    ],
  },
  {
    name: 'Average per category',
    description: 'Avg of "value" grouped by "category".',
    stages: [
      {
        operator: '$group',
        body: '{ _id: "$category", avg: { $avg: "$value" }, n: { $sum: 1 } }',
      },
      { operator: '$sort', body: '{ avg: -1 }' },
    ],
  },
  {
    name: 'Lookup join',
    description: 'Join with another collection by foreign key.',
    stages: [
      {
        operator: '$lookup',
        body: '{ from: "other", localField: "otherId", foreignField: "_id", as: "joined" }',
      },
    ],
  },
  {
    name: 'Distinct count',
    description: 'Cardinality of a field.',
    stages: [
      { operator: '$group', body: '{ _id: "$field" }' },
      { operator: '$count', body: '"distinct"' },
    ],
  },
  {
    name: 'Faceted breakdown',
    description: 'Run several aggregations in parallel.',
    stages: [
      {
        operator: '$facet',
        body: '{ byStatus: [{ $sortByCount: "$status" }], total: [{ $count: "n" }] }',
      },
    ],
  },
  {
    name: 'Project + reshape',
    description: 'Pick fields and rename.',
    stages: [
      {
        operator: '$project',
        body: '{ _id: 0, name: 1, email: 1, fullName: { $concat: ["$first", " ", "$last"] } }',
      },
    ],
  },
  {
    name: 'Unwind array',
    description: 'Flatten an array field into multiple docs.',
    stages: [
      { operator: '$unwind', body: '"$tags"' },
      { operator: '$sortByCount', body: '"$tags"' },
    ],
  },
];
