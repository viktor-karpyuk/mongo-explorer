package com.kubrik.mex.cluster.model;

import java.util.List;
import java.util.Map;

/**
 * v2.4 TOPO-11 — per-member state carried inside a {@link TopologySnapshot}.
 * All fields are nullable except {@code host} and {@code state}; the sampler
 * populates what {@code replSetGetStatus} + {@code replSetGetConfig} return
 * and leaves the rest null.
 */
public record Member(
        String host,
        MemberState state,
        Integer priority,
        Integer votes,
        Boolean hidden,
        Boolean arbiterOnly,
        Map<String, String> tags,
        Long optimeMs,
        Long lagMs,
        Long pingMs,
        Long uptimeSecs,
        String syncSourceHost,
        Integer configVersion
) {
    public Member {
        if (host == null) throw new IllegalArgumentException("host");
        if (state == null) state = MemberState.UNKNOWN;
        if (tags == null) tags = Map.of();
    }

    public boolean isPrimary() { return state == MemberState.PRIMARY; }
    public boolean isSecondary() { return state == MemberState.SECONDARY; }
    public boolean isArbiter() { return state == MemberState.ARBITER; }

    /** Convenience: label for a member in UI rendering. */
    public String displayLabel() {
        StringBuilder sb = new StringBuilder(host);
        if (isPrimary()) sb.append(" (primary)");
        else if (isArbiter()) sb.append(" (arbiter)");
        else if (Boolean.TRUE.equals(hidden)) sb.append(" (hidden)");
        return sb.toString();
    }

    /** Minimal member when only a host is known (e.g., discovery partial). */
    public static Member unknownAt(String host) {
        return new Member(host, MemberState.UNKNOWN, null, null, null, null,
                Map.of(), null, null, null, null, null, null);
    }

    /** Utility for tests / serialisation that want a stable ordering. */
    public static final java.util.Comparator<Member> BY_HOST =
            java.util.Comparator.comparing(Member::host);

    /** Used by {@link TopologySnapshot#sha256()} — delegated so the record stays open. */
    static List<Member> sortedCopy(List<Member> in) {
        List<Member> copy = new java.util.ArrayList<>(in);
        copy.sort(BY_HOST);
        return copy;
    }
}
