package com.kubrik.mex.cluster.model;

import com.kubrik.mex.cluster.util.CanonicalJson;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * v2.4 TOPO-10..14 — canonical topology record emitted by {@code
 * ClusterTopologyService}. Immutable; {@link #sha256()} is stable across equal
 * inputs and drives change-detection for UI consumers + DAO de-duplication
 * (see {@code topology_snapshots} in {@code Database.migrate}).
 */
public record TopologySnapshot(
        ClusterKind clusterKind,
        long capturedAt,
        String version,
        List<Member> members,
        List<Shard> shards,
        List<Mongos> mongos,
        List<Member> configServers,
        List<String> warnings
) {
    public TopologySnapshot {
        if (clusterKind == null) throw new IllegalArgumentException("clusterKind");
        if (version == null) version = "";
        members        = members        == null ? List.of() : List.copyOf(members);
        shards         = shards         == null ? List.of() : List.copyOf(shards);
        mongos         = mongos         == null ? List.of() : List.copyOf(mongos);
        configServers  = configServers  == null ? List.of() : List.copyOf(configServers);
        warnings       = warnings       == null ? List.of() : List.copyOf(warnings);
    }

    public int memberCount() {
        return switch (clusterKind) {
            case STANDALONE, REPLSET -> members.size();
            case SHARDED -> shards.stream().mapToInt(s -> s.members().size()).sum();
        };
    }

    public int shardCount() { return shards.size(); }

    /** Parses {@code buildInfo.version} into (major, minor). Missing → (0,0). */
    public int[] majorMinor() {
        String v = version == null ? "" : version;
        String[] parts = v.split("\\.");
        int major = parts.length > 0 ? tryInt(parts[0]) : 0;
        int minor = parts.length > 1 ? tryInt(parts[1]) : 0;
        return new int[] { major, minor };
    }

    private static int tryInt(String s) {
        try { return Integer.parseInt(s.replaceAll("[^0-9].*$", "")); }
        catch (NumberFormatException ignored) { return 0; }
    }

    /**
     * Canonical JSON serialisation used to derive {@link #sha256()}. Keys are
     * sorted, collections are sorted deterministically, {@code capturedAt}
     * participates so two snapshots at the same wall-clock differ only if the
     * rest of the tree differs — which is what we want.
     *
     * <p>Callers persisting snapshots should use this directly (see DAO).</p>
     */
    public String toCanonicalJson() {
        CanonicalJson.ObjectWriter obj = CanonicalJson.object();
        obj.putString("clusterKind", clusterKind.name());
        obj.putLong("capturedAt", capturedAt);
        obj.putString("version", version);

        List<Member> sortedMembers = Member.sortedCopy(members);
        CanonicalJson.ArrayWriter arr = obj.putArray("members");
        for (Member m : sortedMembers) CanonicalJson.writeMember(arr.addObject(), m);
        arr.close();

        CanonicalJson.ArrayWriter shardArr = obj.putArray("shards");
        List<Shard> sortedShards = new java.util.ArrayList<>(shards);
        sortedShards.sort(java.util.Comparator.comparing(Shard::id));
        for (Shard s : sortedShards) {
            CanonicalJson.ObjectWriter so = shardArr.addObject();
            so.putString("id", s.id());
            so.putString("rsHost", s.rsHost());
            so.putBool("draining", s.draining());
            CanonicalJson.ArrayWriter mArr = so.putArray("members");
            for (Member m : Member.sortedCopy(s.members())) {
                CanonicalJson.writeMember(mArr.addObject(), m);
            }
            mArr.close();
            CanonicalJson.writeTags(so.putObject("tags"), s.tags());
            so.close();
        }
        shardArr.close();

        CanonicalJson.ArrayWriter mgs = obj.putArray("mongos");
        List<Mongos> sortedMongos = new java.util.ArrayList<>(mongos);
        sortedMongos.sort(java.util.Comparator.comparing(Mongos::host));
        for (Mongos m : sortedMongos) {
            CanonicalJson.ObjectWriter mo = mgs.addObject();
            mo.putString("host", m.host());
            mo.putString("version", m.version());
            mo.putNullableLong("uptimeSecs", m.uptimeSecs());
            mo.putNullableLong("advisoryStartupDelaySecs", m.advisoryStartupDelaySecs());
            mo.close();
        }
        mgs.close();

        CanonicalJson.ArrayWriter cfg = obj.putArray("configServers");
        for (Member m : Member.sortedCopy(configServers)) {
            CanonicalJson.writeMember(cfg.addObject(), m);
        }
        cfg.close();

        CanonicalJson.ArrayWriter wArr = obj.putArray("warnings");
        for (String w : warnings) wArr.addString(w);
        wArr.close();

        return obj.toJson();
    }

    /** Hex-lowercase SHA-256 over {@link #toCanonicalJson()}. */
    public String sha256() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(toCanonicalJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** Canonical JSON excluding {@code capturedAt} — useful for tests that want
     *  wall-clock-insensitive structural equality. */
    public String structuralCanonicalJson() {
        TopologySnapshot stamped = new TopologySnapshot(clusterKind, 0L, version,
                members, shards, mongos, configServers, warnings);
        return stamped.toCanonicalJson();
    }
}
