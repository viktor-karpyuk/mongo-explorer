package com.kubrik.mex.labs.lifecycle;

import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.store.ConnectionStore;

/**
 * v2.8.4 LAB-CONN-1 — Writes the origin=LAB connection row once a
 * Lab reports healthy. The URI targets the Lab's "entry" container
 * (mongos for sharded, replset seed for replica sets, lone mongod
 * for standalone). Replica-set URIs include the replSet name so the
 * driver routes writes correctly without manual user config.
 */
public final class LabAutoConnectionWriter {

    private final ConnectionStore connectionStore;

    public LabAutoConnectionWriter(ConnectionStore connectionStore) {
        this.connectionStore = connectionStore;
    }

    /** Create the connection row. Caller should then
     *  {@code labDeploymentDao.setConnectionId(lab.id(), returned)}
     *  to complete the back-pointer. */
    public String write(LabDeployment lab, LabTemplate template) {
        String uri = composeUri(lab, template);
        String display = template.displayName() + " (Lab " + shortId(lab) + ")";
        return connectionStore.insertLabOrigin(display, uri, lab.id());
    }

    /** Destroy-path cleanup — delete the lab-origin connection so
     *  the tree doesn't render a dead cluster. Delegates to the
     *  store's existing {@code delete(id)} which handles v2.4
     *  cascade (ops_audit / topology / role_cache cleanup). */
    public void deleteLabOriginConnection(String connectionId) {
        connectionStore.delete(connectionId);
    }

    static String composeUri(LabDeployment lab, LabTemplate template) {
        String entry = LabHealthWatcher.chooseReadyContainer(template);
        int port = lab.portMap().portFor(entry);
        String base = "mongodb://127.0.0.1:" + port;
        String replSet = replSetNameFor(template);
        if (replSet != null) {
            // Single-host replset URI — we only have ONE member port
            // published from the host's perspective; the driver will
            // follow ismaster + reach the other replset members at
            // their own published ports because they all bind to
            // 127.0.0.1. Add directConnection=false so the driver
            // honours the replica-set discovery.
            return base + "/?replicaSet=" + replSet
                    + "&directConnection=false";
        }
        return base + "/?directConnection=true";
    }

    /** For replset-bearing templates, the replset name baked into
     *  the init sidecar. Kept as a static lookup to avoid parsing
     *  the compose template string at connection-write time. */
    private static String replSetNameFor(LabTemplate template) {
        return switch (template.id()) {
            case "rs-3", "rs-5" -> "rs1";
            case "sharded-1" -> null;  // go through mongos, not replset
            case "triple-rs" -> "rs1";  // seed the first rs; the user's
                                         // second / third rs needs a
                                         // manual connection add
            default -> null;
        };
    }

    private static String shortId(LabDeployment lab) {
        String p = lab.composeProject();
        return p.length() >= 8 ? p.substring(p.length() - 8) : p;
    }
}
