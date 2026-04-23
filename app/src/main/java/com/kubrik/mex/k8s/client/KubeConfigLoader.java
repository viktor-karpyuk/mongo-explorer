package com.kubrik.mex.k8s.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.kubrik.mex.k8s.model.K8sAuthKind;
import com.kubrik.mex.k8s.model.K8sContextSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-A2 — Pure parser for kubeconfig YAML files.
 *
 * <p>Handles the two things the UI needs before we hand off to the
 * official client:</p>
 * <ol>
 *   <li><b>Discovery.</b> Walk {@code $KUBECONFIG} (colon-delimited)
 *       or default to {@code ~/.kube/config}, parsing each into an
 *       in-memory map. We never mutate on-disk files.</li>
 *   <li><b>Classification.</b> For each context, resolve the user
 *       entry and classify the auth strategy ({@link K8sAuthKind}).
 *       This drives the "Add cluster" picker's per-row hint and the
 *       pre-flight PLUGIN_MISSING probe.</li>
 * </ol>
 *
 * <p>The classification is best-effort and never fatal: an unparseable
 * user stanza still yields a row with {@code authKind = UNKNOWN} so
 * the user can see what's there, instead of silently omitting.</p>
 */
public final class KubeConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(KubeConfigLoader.class);

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private KubeConfigLoader() {}

    /** Default path to {@code ~/.kube/config}. */
    public static Path defaultKubeconfigPath() {
        String home = System.getProperty("user.home");
        return Paths.get(home, ".kube", "config");
    }

    /**
     * Resolve the candidate kubeconfig paths, honouring {@code $KUBECONFIG}
     * (colon-separated on Unix, semicolon on Windows) and falling back to
     * {@code ~/.kube/config}. Paths that don't exist are filtered out; the
     * order is preserved so "earlier file wins" merging is possible upstream.
     */
    public static List<Path> discoverPaths() {
        String env = System.getenv("KUBECONFIG");
        if (env != null && !env.isBlank()) {
            String sep = System.getProperty("path.separator", ":");
            List<Path> out = new ArrayList<>();
            for (String part : env.split(java.util.regex.Pattern.quote(sep))) {
                if (part.isBlank()) continue;
                Path p = Paths.get(part.trim());
                if (Files.isRegularFile(p)) out.add(p);
            }
            if (!out.isEmpty()) return Collections.unmodifiableList(out);
        }
        Path def = defaultKubeconfigPath();
        return Files.isRegularFile(def)
                ? List.of(def)
                : List.of();
    }

    /**
     * Enumerate every context the user could pick from across every
     * discovered kubeconfig, with each context annotated by its
     * originating file path.
     */
    public static List<DiscoveredContext> discoverAll() throws IOException {
        List<DiscoveredContext> out = new ArrayList<>();
        for (Path p : discoverPaths()) {
            try {
                for (K8sContextSummary c : listContexts(p)) {
                    out.add(new DiscoveredContext(p, c));
                }
            } catch (IOException ioe) {
                log.warn("skipping unreadable kubeconfig {}: {}", p, ioe.toString());
            }
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Parse a single kubeconfig file and return a summary per context.
     * Classifies each context's user strategy eagerly so the picker
     * can sort/filter without re-parsing.
     */
    public static List<K8sContextSummary> listContexts(Path kubeconfig) throws IOException {
        Map<?, ?> root;
        try {
            root = YAML.readValue(Files.readAllBytes(kubeconfig), Map.class);
        } catch (IOException ioe) {
            throw new IOException("parse " + kubeconfig + ": " + ioe.getMessage(), ioe);
        }
        if (root == null) return List.of();

        Map<String, Map<?, ?>> clusters = indexByName(listOf(root.get("clusters")), "cluster");
        Map<String, Map<?, ?>> users = indexByName(listOf(root.get("users")), "user");
        List<K8sContextSummary> out = new ArrayList<>();
        for (Object raw : listOf(root.get("contexts"))) {
            if (!(raw instanceof Map<?, ?> ctxRow)) continue;
            String ctxName = asString(ctxRow.get("name"));
            Map<?, ?> ctx = asMap(ctxRow.get("context"));
            if (ctxName == null || ctx == null) continue;
            String clusterName = asString(ctx.get("cluster"));
            String userName = asString(ctx.get("user"));
            Map<?, ?> clusterBody = clusterName == null ? null : clusters.get(clusterName);
            Map<?, ?> userBody = userName == null ? null : users.get(userName);
            Optional<String> ns = Optional.ofNullable(asString(ctx.get("namespace")));
            Optional<String> server = clusterBody == null
                    ? Optional.empty()
                    : Optional.ofNullable(asString(clusterBody.get("server")));

            Classification cl = classifyUser(userBody);
            out.add(new K8sContextSummary(
                    ctxName,
                    clusterName == null ? "" : clusterName,
                    userName == null ? "" : userName,
                    ns,
                    server,
                    cl.kind(),
                    cl.detail(),
                    cl.execBinary()));
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Classify a user stanza. {@code user} here is the already-extracted
     * body of the kubeconfig's {@code users[*].user} map — i.e. the
     * dictionary directly containing {@code token} / {@code exec} /
     * {@code auth-provider} / {@code client-certificate-data}.
     *
     * <p>Order matters: exec has precedence over auth-provider because
     * modern clouds emit both (GKE's {@code gke-gcloud-auth-plugin}
     * era kubeconfigs) and the exec block is the one the client
     * actually uses post-1.26.</p>
     */
    static Classification classifyUser(Map<?, ?> user) {
        if (user == null || user.isEmpty()) {
            return new Classification(K8sAuthKind.UNKNOWN, Optional.empty(), Optional.empty());
        }

        Map<?, ?> exec = asMap(user.get("exec"));
        if (exec != null) {
            String bin = asString(exec.get("command"));
            List<String> args = stringListOf(exec.get("args"));
            String detail = bin == null ? "exec" : bin
                    + (args.isEmpty() ? "" : " " + String.join(" ", args.subList(0,
                            Math.min(args.size(), 3))));
            return new Classification(
                    K8sAuthKind.EXEC_PLUGIN,
                    Optional.of(detail),
                    Optional.ofNullable(bin));
        }
        Map<?, ?> authProvider = asMap(user.get("auth-provider"));
        if (authProvider != null) {
            String name = asString(authProvider.get("name"));
            if (name != null && "oidc".equalsIgnoreCase(name)) {
                Map<?, ?> cfg = asMap(authProvider.get("config"));
                String issuer = cfg == null ? null : asString(cfg.get("idp-issuer-url"));
                return new Classification(
                        K8sAuthKind.OIDC,
                        Optional.ofNullable(issuer == null ? "oidc" : "oidc: " + issuer),
                        Optional.empty());
            }
            // Non-OIDC auth-provider entries (gcp, azure) are legacy;
            // the client treats them similarly to exec plugins.
            return new Classification(
                    K8sAuthKind.EXEC_PLUGIN,
                    Optional.of("auth-provider: " + name),
                    Optional.empty());
        }
        if (user.get("token") instanceof String t && !t.isBlank()) {
            return new Classification(K8sAuthKind.TOKEN, Optional.empty(), Optional.empty());
        }
        if (user.containsKey("client-certificate") || user.containsKey("client-certificate-data")) {
            return new Classification(K8sAuthKind.CLIENT_CERT, Optional.empty(), Optional.empty());
        }
        if (user.containsKey("username") && user.containsKey("password")) {
            return new Classification(K8sAuthKind.BASIC_AUTH, Optional.empty(), Optional.empty());
        }
        return new Classification(K8sAuthKind.UNKNOWN, Optional.empty(), Optional.empty());
    }

    /* =========================== helpers =========================== */

    private static Map<String, Map<?, ?>> indexByName(List<?> rows, String bodyKey) {
        Map<String, Map<?, ?>> out = new LinkedHashMap<>();
        for (Object raw : rows) {
            if (!(raw instanceof Map<?, ?> r)) continue;
            String name = asString(r.get("name"));
            Map<?, ?> body = asMap(r.get(bodyKey));
            if (name != null) out.put(name, body == null ? new HashMap<>() : body);
        }
        return out;
    }

    private static List<?> listOf(Object o) {
        if (o instanceof List<?> l) return l;
        return List.of();
    }

    private static List<String> stringListOf(Object o) {
        if (!(o instanceof List<?> l)) return List.of();
        List<String> out = new ArrayList<>();
        for (Object x : l) if (x instanceof String s) out.add(s);
        return out;
    }

    private static Map<?, ?> asMap(Object o) {
        return o instanceof Map<?, ?> m ? m : null;
    }

    private static String asString(Object o) {
        return o instanceof String s ? s : (o == null ? null : o.toString());
    }

    @SuppressWarnings("unused")
    private static String normalise(String in) {
        return in == null ? "" : in.trim().toLowerCase(Locale.ROOT);
    }

    /** Intermediate shape passed back from {@link #classifyUser(Map)}. */
    record Classification(K8sAuthKind kind, Optional<String> detail,
                           Optional<String> execBinary) {}

    /**
     * One row of the picker: the context summary, plus the kubeconfig
     * file it lives in. Multiple discovered paths can contribute the
     * same context name (merging is the caller's problem; we preserve
     * the ambiguity rather than silently deduplicating).
     */
    public record DiscoveredContext(Path sourcePath, K8sContextSummary summary) {}

    /**
     * Collect kubeconfig paths in the same order the Kubernetes CLI
     * does, then hand the whole set back so callers can show the user
     * which file each context lives in.
     */
    public static List<Path> orderedPaths() {
        return new ArrayList<>(discoverPaths());
    }

    // Retained for test dependency ordering; does not mutate state.
    @SuppressWarnings("unused")
    private static List<Path> splitEnv(String env) {
        if (env == null || env.isBlank()) return List.of();
        String sep = System.getProperty("path.separator", ":");
        return Arrays.stream(env.split(java.util.regex.Pattern.quote(sep)))
                .filter(s -> !s.isBlank())
                .map(Paths::get)
                .toList();
    }
}
