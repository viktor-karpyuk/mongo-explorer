import { useState } from 'react';
import { ClusterPanel } from './ClusterPanel';
import { MonitoringPanel } from './MonitoringPanel';

type Tab = 'cluster' | 'monitoring';

export function ConnectionPanel({ connectionId }: { connectionId: string }) {
  const [tab, setTab] = useState<Tab>('cluster');
  return (
    <div className="coll-panel">
      <div className="coll-panel__tabs">
        <button
          className={`tab${tab === 'cluster' ? ' tab--active' : ''}`}
          onClick={() => setTab('cluster')}
        >
          Cluster
        </button>
        <button
          className={`tab${tab === 'monitoring' ? ' tab--active' : ''}`}
          onClick={() => setTab('monitoring')}
        >
          Monitoring
        </button>
      </div>
      <div className="coll-panel__body">
        {tab === 'cluster' && <ClusterPanel connectionId={connectionId} />}
        {tab === 'monitoring' && <MonitoringPanel connectionId={connectionId} />}
      </div>
    </div>
  );
}
