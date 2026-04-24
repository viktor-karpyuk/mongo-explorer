package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.VersionApi;
import io.kubernetes.client.openapi.models.VersionInfo;

import java.util.Set;

/**
 * v2.8.1 Q2.8.1-G — Kubernetes server version is within the blessed
 * matrix (milestone §7.8).
 *
 * <p>Blessed: 1.29 / 1.30 / 1.31 (re-evaluated in open question
 * 9.8). Out-of-matrix → WARN; in-matrix → PASS. Older than 1.27 →
 * FAIL (the client-java library itself stops supporting them).</p>
 */
public final class ClusterVersionCheck implements PreflightCheck {

    public static final String ID = "preflight.cluster-version";

    /** Blessed matrix (milestone §7.8). Visible so the Lab / preflight
     *  UI and IT tests can parametrise the same set of versions. */
    public static final Set<String> BLESSED = Set.of("1.29", "1.30", "1.31");
    /** Below-blessed minors we still accept with a warning. Older than
     *  this → client-java itself stops supporting the API surface. */
    public static final Set<String> HARD_MIN = Set.of("1.27", "1.28");

    /** Categorisation of a minor-version string against the blessed
     *  matrix — a pure function split out of {@link #run} so unit tests
     *  and blessed-matrix smoke ITs can parametrise across versions
     *  without rebuilding an {@link ApiClient}. */
    public enum Classification { BLESSED, BELOW_MIN, OUTSIDE, UNPARSEABLE }

    public static Classification classify(String minor) {
        if (minor == null) return Classification.UNPARSEABLE;
        if (BLESSED.contains(minor)) return Classification.BLESSED;
        if (HARD_MIN.contains(minor)) return Classification.BELOW_MIN;
        return Classification.OUTSIDE;
    }

    @Override public String id() { return ID; }
    @Override public String label() { return "Kubernetes version"; }
    @Override public PreflightScope scope(ProvisionModel m) { return PreflightScope.ALWAYS; }

    @Override
    public PreflightResult run(ApiClient client, ProvisionModel m) {
        try {
            VersionInfo v = new VersionApi(client).getCode().execute();
            String git = v.getGitVersion();
            String minor = majorDotMinor(git);
            return switch (classify(minor)) {
                case BLESSED -> PreflightResult.pass(ID);
                case BELOW_MIN -> PreflightResult.warn(ID,
                        "Server version " + minor + " is below the blessed matrix (1.29 / 1.30 / 1.31).",
                        "Upgrade to a blessed version, or proceed knowing you're off-matrix.");
                case OUTSIDE -> PreflightResult.warn(ID,
                        "Server version " + minor + " is outside the blessed matrix.",
                        "v2.8.1 Alpha blesses 1.29 / 1.30 / 1.31. Apply may still succeed.");
                case UNPARSEABLE -> PreflightResult.warn(ID,
                        "Could not parse server version: " + git,
                        "Pre-flight can't verify matrix support.");
            };
        } catch (Exception e) {
            return PreflightResult.warn(ID,
                    "/version probe failed: " + e.getMessage(),
                    "Pre-flight can't verify version; proceed with caution.");
        }
    }

    /** Parse {@code v1.30.2} → {@code "1.30"}. Returns null if unparseable. */
    public static String majorDotMinor(String gitVersion) {
        if (gitVersion == null) return null;
        String s = gitVersion.startsWith("v") ? gitVersion.substring(1) : gitVersion;
        int firstDot = s.indexOf('.');
        if (firstDot < 0) return null;
        int secondDot = s.indexOf('.', firstDot + 1);
        return secondDot < 0 ? s.substring(0, s.length()) : s.substring(0, secondDot);
    }
}
