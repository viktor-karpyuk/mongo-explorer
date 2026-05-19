import { useState } from 'react';
import { QueryView } from './QueryView';
import { AggregationView } from './AggregationView';
import { CollectionStatsPanel } from './CollectionStatsPanel';

type Sub = 'query' | 'aggregate' | 'stats';

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
