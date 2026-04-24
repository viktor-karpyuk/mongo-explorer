package com.kubrik.mex.k8s.compute.managedpool;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-A — Row view of {@code cloud_credentials}.
 *
 * <p>Holds only metadata + a pointer ({@code keychainRef}) to where
 * the real secret material lives — the OS keychain on macOS /
 * Windows / a libsecret-backed store on Linux. Mongo Explorer itself
 * never serialises the secret into its SQLite.</p>
 *
 * <p>The {@link AuthMode} enum lists every mode across all three
 * clouds; {@link #provider()} gates which subset is valid.</p>
 */
public record CloudCredential(
        long id,
        String displayName,
        CloudProvider provider,
        AuthMode authMode,
        String keychainRef,
        Optional<String> awsAccountId,
        Optional<String> gcpProject,
        Optional<String> azureSubscription,
        Optional<String> defaultRegion,
        long createdAt,
        Optional<Long> lastProbedAt,
        Optional<ProbeStatus> probeStatus
) {
    public CloudCredential {
        Objects.requireNonNull(displayName, "displayName");
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(authMode, "authMode");
        Objects.requireNonNull(keychainRef, "keychainRef");
        awsAccountId = awsAccountId == null ? Optional.empty() : awsAccountId;
        gcpProject = gcpProject == null ? Optional.empty() : gcpProject;
        azureSubscription = azureSubscription == null ? Optional.empty() : azureSubscription;
        defaultRegion = defaultRegion == null ? Optional.empty() : defaultRegion;
        lastProbedAt = lastProbedAt == null ? Optional.empty() : lastProbedAt;
        probeStatus = probeStatus == null ? Optional.empty() : probeStatus;
    }

    public enum AuthMode {
        // AWS
        IRSA, EXTERNAL_ID, SSO_PROFILE, STATIC,
        // GCP
        WORKLOAD_IDENTITY, APPLICATION_DEFAULT, SERVICE_ACCOUNT_KEY,
        // Azure
        MANAGED_IDENTITY, SERVICE_PRINCIPAL
    }

    public enum ProbeStatus { OK, AUTH_FAILED, NETWORK_ERROR, NOT_PROBED }
}
