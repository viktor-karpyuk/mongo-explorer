package com.kubrik.mex.security.cis.rules;

import com.kubrik.mex.security.cis.CisRule;
import com.kubrik.mex.security.cis.ComplianceContext;
import com.kubrik.mex.security.encryption.EncryptionStatus;

/** CIS MongoDB v1.2 §2.1 (encryption-at-rest). Fails when any observed
 *  node reports encryption disabled. NOT_APPLICABLE when no encryption
 *  data is available — the probe couldn't speak to the node. */
public final class RequireEncryptionAtRest implements CisRule {
    public String id() { return "CIS-2.1"; }
    public String title() { return "Encryption at rest enabled on every node"; }
    public Severity severity() { return Severity.HIGH; }
    public String rationale() {
        return "WiredTiger encryption at rest protects data files from "
                + "host-level theft (stolen drives, forensics, back-door "
                + "snapshots). A single unencrypted replica leaves the "
                + "whole cluster vulnerable via that node.";
    }
    public Evaluation evaluate(ComplianceContext ctx) {
        if (ctx.encryption().isEmpty()) {
            return Evaluation.notApplicable("no encryption probe data");
        }
        long unencrypted = ctx.encryption().stream()
                .filter(s -> !s.enabled())
                .count();
        if (unencrypted == 0) {
            String keystores = ctx.encryption().stream()
                    .map(EncryptionStatus::keystore)
                    .distinct()
                    .map(Enum::name)
                    .reduce((a, b) -> a + "+" + b).orElse("");
            return Evaluation.pass("Every node reports encryption enabled ("
                    + keystores + ")");
        }
        String bad = ctx.encryption().stream()
                .filter(s -> !s.enabled())
                .map(EncryptionStatus::host)
                .reduce((a, b) -> a + ", " + b).orElse("");
        return Evaluation.fail(unencrypted
                + " node(s) report encryption disabled: " + bad);
    }
}
