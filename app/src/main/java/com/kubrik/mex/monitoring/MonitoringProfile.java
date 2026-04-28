package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.model.RollupTier;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Per-connection monitoring settings. Persisted in {@code monitoring_profiles}.
 * See technical-spec §3.3.
 *
 * <p>P-1 materialises the fields that the scheduler needs; retention + top-N cap +
 * pinned collections are carried through but unused until later phases.
 */
public record MonitoringProfile(
        String connectionId,
        boolean enabled,
        Duration instancePollInterval,
        Duration storagePollInterval,
        Duration indexUsagePollInterval,
        String readPreference,
        boolean profilerEnabled,
        int profilerSlowMs,
        Duration profilerAutoDisableAfter,
        int topNCollectionsPerDb,
        List<String> pinnedCollections,
        Map<RollupTier, Duration> retention,
        Instant createdAt,
        Instant updatedAt
) {
    public MonitoringProfile {
        Objects.requireNonNull(connectionId);
        Objects.requireNonNull(instancePollInterval);
        Objects.requireNonNull(storagePollInterval);
        Objects.requireNonNull(indexUsagePollInterval);
        Objects.requireNonNull(readPreference);
        Objects.requireNonNull(profilerAutoDisableAfter);
        Objects.requireNonNull(pinnedCollections);
        Objects.requireNonNull(retention);
        Objects.requireNonNull(createdAt);
        Objects.requireNonNull(updatedAt);
        pinnedCollections = List.copyOf(pinnedCollections);
        retention = Map.copyOf(new EnumMap<>(retention));
    }

    public static MonitoringProfile defaults(String connectionId) {
        Map<RollupTier, Duration> ret = new EnumMap<>(RollupTier.class);
        for (RollupTier t : RollupTier.values()) ret.put(t, t.defaultHorizon());
        Instant now = Instant.now();
        return new MonitoringProfile(
                connectionId,
                true,
                Duration.ofSeconds(1),
                Duration.ofSeconds(60),
                Duration.ofMinutes(5),
                "secondaryPreferred",
                false,
                100,
                Duration.ofMinutes(60),
                50,
                List.of(),
                ret,
                now,
                now
        );
    }
}
