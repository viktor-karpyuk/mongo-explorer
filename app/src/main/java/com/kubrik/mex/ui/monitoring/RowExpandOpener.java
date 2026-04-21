package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.monitoring.store.ProfileSampleRecord;

/**
 * App-level opener for the row-detail flow (ROW-EXPAND-*). Implementation lives
 * in {@code MainView} and resolves each connection id to its display name before
 * mounting the view as a modal Stage or an app-level Tab.
 */
public interface RowExpandOpener {

    void openSlowQuery(ProfileSampleRecord record, String connectionId, MetricExpander.Mode mode);

    void openIndex(String db, String coll, String index,
                   long sizeBytes, double opsPerSec, String flags,
                   String connectionId, MetricExpander.Mode mode);

    void openNamespace(String db, String coll,
                       double readMsPerSec, double writeMsPerSec, double totalMsPerSec,
                       double readOpsPerSec, double writeOpsPerSec,
                       String connectionId, MetricExpander.Mode mode);

    void openMember(String member, String state, String health, String uptime,
                    String ping, String lag, String lastHeartbeat,
                    String connectionId, MetricExpander.Mode mode);
}
