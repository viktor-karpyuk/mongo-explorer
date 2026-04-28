package com.kubrik.mex.k8s.provision;

import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-D1 — Backup strategy wired at provision time.
 *
 * <p>Prod requires a non-{@link Mode#NONE} value (milestone §7.6).
 * Decision-tree per operator:</p>
 * <ul>
 *   <li><b>PSMDB</b> — {@link Mode#PSMDB_PBM} native PBM integration
 *       (the operator ships it).</li>
 *   <li><b>MCO</b> — {@link Mode#MANAGED_PBM_CRONJOB} (we render a
 *       pbm-agent Deployment + CronJob + storage Secret) or
 *       {@link Mode#BYO_DECLARED} (user asserts an out-of-band
 *       strategy; audit row captures the claim).</li>
 * </ul>
 */
public record BackupSpec(Mode mode) {

    public enum Mode {
        NONE,
        PSMDB_PBM,
        MANAGED_PBM_CRONJOB,
        BYO_DECLARED
    }

    public BackupSpec {
        Objects.requireNonNull(mode, "mode");
    }

    public static BackupSpec none() { return new BackupSpec(Mode.NONE); }

    public boolean isProdAcceptable() {
        return mode != Mode.NONE;
    }
}
