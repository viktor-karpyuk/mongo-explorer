package com.kubrik.mex.k8s.compute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kubrik.mex.k8s.compute.karpenter.KarpenterSpec;
import com.kubrik.mex.k8s.compute.managedpool.CloudProvider;
import com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * v2.8.2 Q2.8.2-A — Canonical JSON codec for {@link ComputeStrategy}.
 *
 * <p>Serialisation target: {@code provisioning_records.compute_strategy_json}
 * (nullable; NULL means "v2.8.0 default scheduler"). The wire format
 * is intentionally explicit and human-readable so audit diff tools
 * and post-mortems can inspect it without code:</p>
 *
 * <pre>{@code
 *   { "type": "node_pool",
 *     "selector": { "workload": "mongodb", "tier": "prod" },
 *     "tolerations": [
 *       { "key": "dedicated", "value": "mongo", "effect": "NoSchedule" }
 *     ],
 *     "spread": "within_pool" }
 * }</pre>
 *
 * <p>Unknown {@code type} values round-trip as {@link
 * ComputeStrategy.None} with a warning logged by the DAO — a forward-
 * compatibility hatch for rows written by later Mongo Explorer
 * versions.</p>
 */
public final class ComputeStrategyJson {

    private static final ObjectMapper M = new ObjectMapper();

    private ComputeStrategyJson() {}

    public static String toJson(ComputeStrategy s) {
        if (s == null || s instanceof ComputeStrategy.None) return null;
        ObjectNode root = M.createObjectNode();
        switch (s) {
            case ComputeStrategy.NodePool np -> {
                root.put("type", "node_pool");
                ObjectNode sel = root.putObject("selector");
                for (LabelPair p : np.selector()) sel.put(p.key(), p.value());
                ArrayNode tol = root.putArray("tolerations");
                for (Toleration t : np.tolerations()) {
                    ObjectNode o = tol.addObject();
                    o.put("key", t.key());
                    if (t.value() != null) o.put("value", t.value());
                    o.put("effect", t.effectYamlValue());
                }
                root.put("spread", np.spreadScope().name().toLowerCase());
            }
            case ComputeStrategy.Karpenter k -> {
                root.put("type", "karpenter");
                k.spec().ifPresent(sp -> writeKarpenterSpec(root, sp));
            }
            case ComputeStrategy.ManagedPool mp -> {
                root.put("type", "managed_pool");
                mp.spec().ifPresent(sp -> writeManagedPoolSpec(root, sp));
            }
            case ComputeStrategy.None n -> { /* filtered earlier */ }
        }
        try { return M.writeValueAsString(root); }
        catch (Exception e) { throw new IllegalStateException(
                "serialise compute strategy: " + e.getMessage(), e); }
    }

    public static ComputeStrategy fromJson(String json) {
        if (json == null || json.isBlank()) return ComputeStrategy.NONE;
        JsonNode root;
        try { root = M.readTree(json); }
        catch (IOException ioe) { throw new IllegalArgumentException(
                "parse compute_strategy_json: " + ioe.getMessage(), ioe); }
        String type = root.path("type").asText("none");
        return switch (type) {
            case "node_pool" -> parseNodePool(root);
            case "karpenter" -> new ComputeStrategy.Karpenter(parseKarpenter(root));
            case "managed_pool" -> new ComputeStrategy.ManagedPool(parseManagedPool(root));
            default -> ComputeStrategy.NONE;
        };
    }

    private static void writeKarpenterSpec(ObjectNode root, KarpenterSpec sp) {
        ObjectNode ncr = root.putObject("node_class_ref");
        ncr.put("api_version", sp.nodeClassRef().apiVersion());
        ncr.put("kind", sp.nodeClassRef().kind());
        ncr.put("name", sp.nodeClassRef().name());
        root.set("capacity_type", arrayOf(sp.capacityTypes()));
        root.set("instance_families", arrayOf(sp.instanceFamilies()));
        root.set("arch", arrayOf(sp.architectures()));
        if (sp.cpuMin() != null || sp.cpuMax() != null) {
            ObjectNode cpu = root.putObject("cpu_range");
            if (sp.cpuMin() != null) cpu.put("min", sp.cpuMin());
            if (sp.cpuMax() != null) cpu.put("max", sp.cpuMax());
        }
        if (sp.memMin() != null || sp.memMax() != null) {
            ObjectNode mem = root.putObject("mem_range");
            if (sp.memMin() != null) mem.put("min", sp.memMin());
            if (sp.memMax() != null) mem.put("max", sp.memMax());
        }
        ObjectNode dis = root.putObject("disruption");
        dis.put("consolidation_when_underutilized", sp.consolidation());
        if (sp.expireAfter() != null) dis.put("expire_after", sp.expireAfter());
        if (sp.limitCpu() != null || sp.limitMemory() != null) {
            ObjectNode lim = root.putObject("limits");
            if (sp.limitCpu() != null) lim.put("cpu", sp.limitCpu());
            if (sp.limitMemory() != null) lim.put("memory", sp.limitMemory());
        }
    }

    private static Optional<KarpenterSpec> parseKarpenter(JsonNode root) {
        JsonNode ncr = root.path("node_class_ref");
        if (!ncr.isObject() || ncr.path("name").isMissingNode()) return Optional.empty();
        KarpenterSpec.NodeClassRef ref = new KarpenterSpec.NodeClassRef(
                ncr.path("api_version").asText("karpenter.k8s.aws/v1"),
                ncr.path("kind").asText("EC2NodeClass"),
                ncr.path("name").asText());
        List<String> cap = stringArray(root.path("capacity_type"));
        List<String> fam = stringArray(root.path("instance_families"));
        List<String> arch = stringArray(root.path("arch"));
        String cpuMin = root.path("cpu_range").path("min").asText(null);
        String cpuMax = root.path("cpu_range").path("max").asText(null);
        String memMin = root.path("mem_range").path("min").asText(null);
        String memMax = root.path("mem_range").path("max").asText(null);
        boolean consolidation = root.path("disruption")
                .path("consolidation_when_underutilized").asBoolean(true);
        String expireAfter = root.path("disruption").path("expire_after").asText(null);
        String limitCpu = root.path("limits").path("cpu").asText(null);
        String limitMem = root.path("limits").path("memory").asText(null);
        return Optional.of(new KarpenterSpec(ref, cap, fam, arch,
                cpuMin, cpuMax, memMin, memMax,
                consolidation, expireAfter, limitCpu, limitMem));
    }

    private static void writeManagedPoolSpec(com.fasterxml.jackson.databind.node.ObjectNode root,
                                               ManagedPoolSpec sp) {
        root.put("provider", sp.provider().wireValue());
        root.put("credential_id", sp.credentialId());
        root.put("region", sp.region());
        root.put("pool_name", sp.poolName());
        root.put("instance_type", sp.instanceType());
        root.put("capacity_type", sp.capacityType().name());
        ObjectNode counts = root.putObject("node_count");
        counts.put("min", sp.minNodes());
        counts.put("desired", sp.desiredNodes());
        counts.put("max", sp.maxNodes());
        root.put("arch", sp.arch());
        root.set("zones", arrayOf(sp.zones()));
        root.set("subnet_ids", arrayOf(sp.subnetIds()));
    }

    private static Optional<ManagedPoolSpec> parseManagedPool(com.fasterxml.jackson.databind.JsonNode root) {
        com.fasterxml.jackson.databind.JsonNode prov = root.path("provider");
        if (prov.isMissingNode() || prov.asText("").isBlank()) return Optional.empty();
        CloudProvider provider;
        try { provider = CloudProvider.fromWire(prov.asText()); }
        catch (IllegalArgumentException iae) { return Optional.empty(); }
        long credId = root.path("credential_id").asLong(0L);
        String region = root.path("region").asText("");
        String poolName = root.path("pool_name").asText("");
        String instanceType = root.path("instance_type").asText("");
        if (region.isEmpty() || poolName.isEmpty() || instanceType.isEmpty()) {
            return Optional.empty();
        }
        ManagedPoolSpec.CapacityType ct;
        try {
            ct = ManagedPoolSpec.CapacityType.valueOf(
                    root.path("capacity_type").asText("ON_DEMAND"));
        } catch (IllegalArgumentException iae) {
            ct = ManagedPoolSpec.CapacityType.ON_DEMAND;
        }
        int min = root.path("node_count").path("min").asInt(1);
        int desired = root.path("node_count").path("desired").asInt(Math.max(1, min));
        int max = root.path("node_count").path("max").asInt(Math.max(desired, min));
        String arch = root.path("arch").asText("amd64");
        List<String> zones = stringArray(root.path("zones"));
        List<String> subnets = stringArray(root.path("subnet_ids"));
        try {
            return Optional.of(new ManagedPoolSpec(provider, credId, region,
                    poolName, instanceType, ct, min, desired, max, arch, zones, subnets));
        } catch (IllegalArgumentException iae) {
            return Optional.empty();
        }
    }

    private static ArrayNode arrayOf(List<String> values) {
        ArrayNode a = M.createArrayNode();
        for (String v : values) a.add(v);
        return a;
    }

    private static List<String> stringArray(JsonNode node) {
        if (!node.isArray()) return List.of();
        List<String> out = new ArrayList<>(node.size());
        for (JsonNode n : node) out.add(n.asText());
        return List.copyOf(out);
    }

    private static ComputeStrategy.NodePool parseNodePool(JsonNode root) {
        List<LabelPair> selector = new ArrayList<>();
        JsonNode sel = root.path("selector");
        if (sel.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = sel.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                selector.add(new LabelPair(e.getKey(), e.getValue().asText()));
            }
        }
        List<Toleration> tolerations = new ArrayList<>();
        JsonNode tol = root.path("tolerations");
        if (tol.isArray()) {
            for (JsonNode t : tol) {
                String key = t.path("key").asText(null);
                if (key == null) continue;
                String value = t.path("value").isMissingNode() ? null : t.path("value").asText();
                String effectRaw = t.path("effect").asText("NoSchedule");
                Toleration.Effect effect = "NoExecute".equals(effectRaw)
                        ? Toleration.Effect.NO_EXECUTE
                        : Toleration.Effect.NO_SCHEDULE;
                tolerations.add(new Toleration(key, value, effect));
            }
        }
        SpreadScope scope = SpreadScope.WITHIN_POOL; // only value shipped in v2.8.2
        return new ComputeStrategy.NodePool(selector, tolerations, scope);
    }
}
