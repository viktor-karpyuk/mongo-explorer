package com.kubrik.mex.security.cis;

import com.kubrik.mex.security.access.UsersRolesFetcher;
import com.kubrik.mex.security.authn.AuthBackendProbe;
import com.kubrik.mex.security.cert.CertRecord;
import com.kubrik.mex.security.encryption.EncryptionStatus;

import java.util.List;

/**
 * v2.6 Q2.6-H1 — immutable bundle of everything the CIS rules need to
 * evaluate. Built by {@code SecurityScanOrchestrator} (Q2.6-H4) out of
 * the individual probes; rules treat it as read-only.
 *
 * <p>Keeping the dependencies explicit here lets each rule be a pure
 * function of context → finding, which is the shape the runner folds
 * over. It also makes the rule pack unit-testable: construct a fixture
 * context, assert the expected verdict.</p>
 *
 * @param users            users + roles snapshot (Q2.6-B1)
 * @param auth             auth-backend probe result (Q2.6-F1)
 * @param encryption       per-node encryption-at-rest (Q2.6-G1)
 * @param certs            TLS cert inventory (Q2.6-E1)
 * @param connectionId     owning connection; rules that produce a
 *                         finding include it in the CisFinding so the
 *                         scan history groups by cluster
 */
public record ComplianceContext(
        String connectionId,
        UsersRolesFetcher.Snapshot users,
        AuthBackendProbe.Snapshot auth,
        List<EncryptionStatus> encryption,
        List<CertRecord> certs
) {
    public ComplianceContext {
        if (connectionId == null || connectionId.isBlank())
            throw new IllegalArgumentException("connectionId");
        users = users == null ? new UsersRolesFetcher.Snapshot(List.of(), List.of()) : users;
        auth = auth == null ? new AuthBackendProbe.Snapshot(List.of(), 0L) : auth;
        encryption = encryption == null ? List.of() : List.copyOf(encryption);
        certs = certs == null ? List.of() : List.copyOf(certs);
    }
}
