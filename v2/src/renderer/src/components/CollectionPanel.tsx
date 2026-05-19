import { useState } from 'react';
import { QueryView } from './QueryView';
import { AggregationView } from './AggregationView';
import { CollectionStatsPanel } from './CollectionStatsPanel';
import { SchemaPanel } from './SchemaPanel';
import { IndexesPanel } from './IndexesPanel';
import { ValidatorPanel } from './ValidatorPanel';

type Sub = 'query' | 'aggregate' | 'schema' | 'indexes' | 'validator' | 'stats';

interface Props {
  connectionId: string;
  db: string;
  collection: string;
}

export function CollectionPanel({ connectionId, db, collection }: Props) {
  const [tab, setTab] = useState<Sub>('query');

  return (
    <div className="coll-panel">
      <div className="coll-panel__tabs">
        <button
          className={`tab${tab === 'query' ? ' tab--active' : ''}`}
          onClick={() => setTab('query')}
        >
          Find
        </button>
        <button
          className={`tab${tab === 'aggregate' ? ' tab--active' : ''}`}
          onClick={() => setTab('aggregate')}
        >
          Aggregate
        </button>
        <button
          className={`tab${tab === 'schema' ? ' tab--active' : ''}`}
          onClick={() => setTab('schema')}
        >
          Schema
        </button>
        <button
          className={`tab${tab === 'indexes' ? ' tab--active' : ''}`}
          onClick={() => setTab('indexes')}
        >
          Indexes
        </button>
        <button
          className={`tab${tab === 'validator' ? ' tab--active' : ''}`}
          onClick={() => setTab('validator')}
        >
          Validator
        </button>
        <button
          className={`tab${tab === 'stats' ? ' tab--active' : ''}`}
          onClick={() => setTab('stats')}
        >
          Stats
        </button>
      </div>
      <div className="coll-panel__body">
        {tab === 'query' && (
          <QueryView connectionId={connectionId} db={db} collection={collection} />
        )}
        {tab === 'aggregate' && (
          <AggregationView connectionId={connectionId} db={db} collection={collection} />
        )}
        {tab === 'schema' && (
          <SchemaPanel connectionId={connectionId} db={db} collection={collection} />
        )}
        {tab === 'indexes' && (
          <IndexesPanel connectionId={connectionId} db={db} collection={collection} />
        )}
        {tab === 'validator' && (
          <ValidatorPanel connectionId={connectionId} db={db} collection={collection} />
        )}
        {tab === 'stats' && (
          <CollectionStatsPanel
            connectionId={connectionId}
            db={db}
            collection={collection}
          />
        )}
      </div>
    </div>
  );
}
