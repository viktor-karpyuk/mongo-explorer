package com.kubrik.mex.labs.model;

import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * v2.8.4 Q2.8.4-B — Immutable container-name → host-port mapping.
 * Stored as canonical JSON in {@code lab_deployments.port_map_json}
 * so the auto-connection writer + UI can both find the mongos /
 * replset-seed port by name without pattern-matching.
 */
public record PortMap(Map<String, Integer> ports) {

    public PortMap {
        Objects.requireNonNull(ports, "ports");
        // Map.copyOf does NOT preserve iteration order (it hashes to
        // an ImmutableCollections.MapN with implementation-defined
        // order). Insertion order matters here because the renderer's
        // golden tests + port_map_json canonical form depend on it.
        ports = java.util.Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(ports));
    }

    public static PortMap empty() { return new PortMap(Map.of()); }

    public int portFor(String containerName) {
        Integer p = ports.get(containerName);
        if (p == null) throw new IllegalArgumentException(
                "no port for container " + containerName + "; known: " + ports.keySet());
        return p;
    }

    public boolean has(String containerName) {
        return ports.containsKey(containerName);
    }

    /** Canonical JSON — keys in insertion order so two maps built
     *  the same way serialise byte-identically (the renderer's
     *  golden tests depend on this). */
    public String toJson() {
        // Manual serialization — avoids pulling Jackson for this
        // one-sentence Map<String,Integer> → JSON need.
        StringBuilder sb = new StringBuilder(ports.size() * 24 + 2);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Integer> e : ports.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(escape(e.getKey())).append("\":")
                    .append(e.getValue().intValue());
        }
        return sb.append('}').toString();
    }

    public static PortMap fromJson(String json) {
        if (json == null || json.isBlank()) return empty();
        Document d = Document.parse(json);
        Map<String, Integer> m = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : d.entrySet()) {
            if (e.getValue() instanceof Number n) m.put(e.getKey(), n.intValue());
        }
        return new PortMap(m);
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
