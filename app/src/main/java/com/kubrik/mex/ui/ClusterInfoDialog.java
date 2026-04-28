package com.kubrik.mex.ui;

import com.kubrik.mex.core.MongoService;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.Node;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Window;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Snapshot of everything MongoDB will tell us about the cluster, rendered into
 * readable sections. The commands issued are all read-only, admin-scope:
 *
 * <ul>
 *   <li>{@code hello} — topology, primary, hosts, wire version, local time.</li>
 *   <li>{@code buildInfo} — version, git, storage engines, debug flags, OpenSSL.</li>
 *   <li>{@code hostInfo} — hostname, CPU, memory, OS, kernel, NUMA, page size.</li>
 *   <li>{@code serverStatus} — ops / network / cache / asserts / storage / security / … .</li>
 *   <li>{@code replSetGetStatus} — per-member state (PRIMARY / SECONDARY / ARBITER /
 *       RECOVERING / …) with health, uptime, optime, lag, priority, votes, syncSource.</li>
 *   <li>{@code listShards} — shards + host + state (sharded only).</li>
 *   <li>{@code listDatabases} — databases + sizeOnDisk.</li>
 *   <li>{@code connPoolStats} — pool host → in-use / available.</li>
 * </ul>
 *
 * <p>Probing runs on a virtual thread; the dialog shows a spinner first and swaps
 * it for content once the commands return. Each raw {@link Document} is retained
 * so the "View JSON" buttons can surface the unvarnished response.
 */
public final class ClusterInfoDialog {

    private static final DateTimeFormatter LOCAL_TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss zzz").withZone(ZoneId.systemDefault());

    private ClusterInfoDialog() {}

    public static void show(Window owner, MongoService svc, String displayName) {
        Dialog<Void> d = new Dialog<>();
        d.initOwner(owner);
        d.setTitle("Cluster info · " + displayName);

        VBox content = new VBox(12);
        content.setPadding(new Insets(16));
        content.setPrefWidth(760);

        ProgressIndicator spinner = new ProgressIndicator();
        spinner.setPrefSize(28, 28);
        Label loading = new Label("Probing cluster…");
        loading.setStyle("-fx-text-fill: #6b7280;");
        HBox loadingRow = new HBox(10, spinner, loading);
        loadingRow.setAlignment(Pos.CENTER_LEFT);
        content.getChildren().add(loadingRow);

        ScrollPane scroll = new ScrollPane(content);
        scroll.setFitToWidth(true);
        scroll.setPrefSize(780, 620);
        d.getDialogPane().setContent(scroll);
        d.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);

        Thread.startVirtualThread(() -> {
            Snapshot snap = probe(svc);
            Platform.runLater(() -> {
                content.getChildren().clear();
                render(content, snap);
            });
        });

        d.showAndWait();
    }

    // ==================== data =============================================

    private record Snapshot(
            Topology topology,
            String setName,
            String primary,
            List<String> hosts,
            String me,
            String version,
            String gitVersion,
            String storageEngine,
            long uptimeSeconds,
            long connectionsCurrent,
            long connectionsAvailable,
            long connectionsActive,
            String hostname,
            Integer cpuCores,
            Long memSizeMB,
            String osName,
            List<ReplMember> replMembers,
            List<Shard> shards,
            List<DbInfo> databases,
            Document rawHello,
            Document rawBuildInfo,
            Document rawHostInfo,
            Document rawServerStatus,
            Document rawReplSetStatus,
            Document rawListShards,
            Document rawConnPoolStats,
            String error
    ) {}

    private record ReplMember(
            String name,
            String state,
            int health,
            long uptime,
            Long lagSeconds,
            Double priority,
            Integer votes,
            Date optimeDate,
            Date lastHeartbeat,
            String syncSource,
            boolean self
    ) {}

    private record Shard(String id, String host, String state, boolean draining) {}

    private record DbInfo(String name, long sizeOnDisk, boolean empty) {}

    private enum Topology {
        STANDALONE("Standalone"),
        REPLICA_SET("Replica set"),
        SHARDED("Sharded cluster"),
        UNKNOWN("Unknown");
        final String label;
        Topology(String label) { this.label = label; }
    }

    // ==================== probing ==========================================

    private static Snapshot probe(MongoService svc) {
        Document hello;
        try { hello = svc.hello(); }
        catch (Throwable t) { return errorSnap(t.getMessage()); }

        Topology topo = detectTopology(hello);
        String setName   = hello.getString("setName");
        String primary   = hello.getString("primary");
        String me        = hello.getString("me");
        @SuppressWarnings("unchecked")
        List<String> hosts = (List<String>) hello.get("hosts");
        if (hosts == null) hosts = List.of();

        Document build = runSafe(svc, "admin", "{ \"buildInfo\": 1 }");
        String version = build == null ? "?" : String.valueOf(build.get("version"));
        String gitVer  = build == null ? "?" : String.valueOf(build.get("gitVersion"));

        Document status = runSafe(svc, "admin", "{ \"serverStatus\": 1 }");
        String storage = "?";
        long uptime = 0, cCurr = 0, cAvail = 0, cActive = 0;
        if (status != null) {
            Object se = status.get("storageEngine");
            if (se instanceof Document sed) storage = String.valueOf(sed.get("name"));
            uptime = asLong(status.get("uptime"));
            Object conns = status.get("connections");
            if (conns instanceof Document cd) {
                cCurr   = asLong(cd.get("current"));
                cAvail  = asLong(cd.get("available"));
                cActive = asLong(cd.get("active"));
            }
        }

        Document hostInfo = runSafe(svc, "admin", "{ \"hostInfo\": 1 }");
        String hostname = "?";
        Integer cores = null;
        Long memMB = null;
        String os = "?";
        if (hostInfo != null) {
            if (hostInfo.get("system") instanceof Document sys) {
                hostname = String.valueOf(sys.getOrDefault("hostname", "?"));
                if (sys.get("numCores") instanceof Number n) cores = n.intValue();
                if (sys.get("memSizeMB") instanceof Number m) memMB = m.longValue();
            }
            if (hostInfo.get("os") instanceof Document osd) {
                os = String.valueOf(osd.getOrDefault("name", "?"))
                        + " " + String.valueOf(osd.getOrDefault("version", ""));
            }
        }

        List<ReplMember> replMembers = List.of();
        Document replStatus = null;
        if (topo == Topology.REPLICA_SET) {
            replStatus = runSafe(svc, "admin", "{ \"replSetGetStatus\": 1 }");
            if (replStatus != null) replMembers = parseReplMembers(replStatus);
        }

        List<Shard> shards = List.of();
        Document listShards = null;
        if (topo == Topology.SHARDED) {
            listShards = runSafe(svc, "admin", "{ \"listShards\": 1 }");
            if (listShards != null) shards = parseShards(listShards);
        }

        List<DbInfo> dbs = List.of();
        Document listDbs = runSafe(svc, "admin", "{ \"listDatabases\": 1 }");
        if (listDbs != null) dbs = parseDatabases(listDbs);

        Document connPool = runSafe(svc, "admin", "{ \"connPoolStats\": 1 }");

        return new Snapshot(
                topo, setName, primary, hosts, me,
                version, gitVer, storage,
                uptime, cCurr, cAvail, cActive,
                hostname, cores, memMB, os,
                replMembers, shards, dbs,
                hello, build, hostInfo, status, replStatus, listShards, connPool,
                null);
    }

    private static Topology detectTopology(Document hello) {
        if (Boolean.TRUE.equals(hello.getBoolean("isdbgrid"))) return Topology.SHARDED;
        if (hello.containsKey("setName")) return Topology.REPLICA_SET;
        if ("isdbgrid".equals(hello.getString("msg"))) return Topology.SHARDED;
        if (hello.containsKey("ismaster") || hello.containsKey("isWritablePrimary")) return Topology.STANDALONE;
        return Topology.UNKNOWN;
    }

    private static List<ReplMember> parseReplMembers(Document rs) {
        List<ReplMember> out = new ArrayList<>();
        Date primaryOptime = null;
        @SuppressWarnings("unchecked")
        List<Document> members = (List<Document>) rs.get("members");
        if (members == null) return out;
        for (Document m : members) {
            if ("PRIMARY".equals(m.getString("stateStr"))) {
                primaryOptime = m.getDate("optimeDate");
                break;
            }
        }
        for (Document m : members) {
            Long lag = null;
            if (primaryOptime != null && !"PRIMARY".equals(m.getString("stateStr"))) {
                Date optime = m.getDate("optimeDate");
                if (optime != null) lag = Math.max(0, (primaryOptime.getTime() - optime.getTime()) / 1000);
            }
            Double priority = (m.get("priority") instanceof Number n) ? n.doubleValue() : null;
            Integer votes = (m.get("votes") instanceof Number n) ? n.intValue() : null;
            String syncSrc = m.getString("syncSourceHost");
            if (syncSrc == null) syncSrc = m.getString("syncingTo");
            out.add(new ReplMember(
                    m.getString("name"),
                    m.getString("stateStr"),
                    asLong(m.get("health")) > 0 ? 1 : 0,
                    asLong(m.get("uptime")),
                    lag,
                    priority,
                    votes,
                    m.getDate("optimeDate"),
                    m.getDate("lastHeartbeatRecv"),
                    syncSrc,
                    Boolean.TRUE.equals(m.getBoolean("self"))));
        }
        return out;
    }

    private static List<Shard> parseShards(Document ls) {
        List<Shard> out = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Document> arr = (List<Document>) ls.get("shards");
        if (arr == null) return out;
        for (Document s : arr) {
            String state = s.getString("state");
            boolean draining = Boolean.TRUE.equals(s.getBoolean("draining"));
            out.add(new Shard(
                    s.getString("_id"),
                    s.getString("host"),
                    state == null ? (draining ? "draining" : "active") : state,
                    draining));
        }
        return out;
    }

    private static List<DbInfo> parseDatabases(Document lsdb) {
        List<DbInfo> out = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Document> dbs = (List<Document>) lsdb.get("databases");
        if (dbs == null) return out;
        for (Document d : dbs) {
            out.add(new DbInfo(
                    d.getString("name"),
                    asLong(d.get("sizeOnDisk")),
                    Boolean.TRUE.equals(d.getBoolean("empty"))));
        }
        out.sort(Comparator.comparingLong((DbInfo x) -> x.sizeOnDisk).reversed());
        return out;
    }

    private static Document runSafe(MongoService svc, String db, String json) {
        try { return svc.runCommand(db, json); }
        catch (Throwable t) { return null; }
    }

    private static long asLong(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }

    private static Snapshot errorSnap(String msg) {
        return new Snapshot(Topology.UNKNOWN, null, null, List.of(), null,
                "?", "?", "?", 0, 0, 0, 0,
                "?", null, null, "?",
                List.of(), List.of(), List.of(),
                null, null, null, null, null, null, null,
                msg);
    }

    // ==================== rendering ========================================

    private static void render(VBox root, Snapshot s) {
        if (s.error != null) {
            Label err = new Label("Could not read cluster info: " + s.error);
            err.setStyle("-fx-text-fill: #b91c1c;");
            root.getChildren().add(err);
            return;
        }

        // ---------- Overview ----------
        root.getChildren().add(sectionHeader("Overview"));
        GridPane g = grid();
        int r = 0;
        kv(g, r++, "Topology",       s.topology.label);
        if (s.setName != null)  kv(g, r++, "Replica set",    s.setName);
        if (s.primary != null)  kv(g, r++, "Primary",        s.primary);
        if (s.me != null)       kv(g, r++, "Connected to",   s.me);
        if (!s.hosts.isEmpty()) kv(g, r++, "Known members",  String.join(", ", s.hosts));
        if (s.rawHello != null) {
            Date localTime = s.rawHello.getDate("localTime");
            if (localTime != null) kv(g, r++, "Server local time", LOCAL_TIME_FMT.format(localTime.toInstant()));
            Integer minWire = intOf(s.rawHello.get("minWireVersion"));
            Integer maxWire = intOf(s.rawHello.get("maxWireVersion"));
            if (minWire != null && maxWire != null) {
                kv(g, r++, "Wire version", minWire + " – " + maxWire);
            }
        }
        root.getChildren().add(g);

        // ---------- Server ----------
        root.getChildren().add(sectionHeader("Server"));
        GridPane g2 = grid();
        r = 0;
        kv(g2, r++, "Version",        s.version + (s.gitVersion != null && !s.gitVersion.isBlank()
                ? "  (" + s.gitVersion + ")" : ""));
        kv(g2, r++, "Storage engine", s.storageEngine);
        kv(g2, r++, "Uptime",         humanDuration(Duration.ofSeconds(s.uptimeSeconds)));
        if (s.rawServerStatus != null) {
            Object proc = s.rawServerStatus.get("process");
            if (proc != null) kv(g2, r++, "Process", String.valueOf(proc));
            Object pid = s.rawServerStatus.get("pid");
            if (pid != null) kv(g2, r++, "PID", String.valueOf(pid));
        }
        kv(g2, r++, "Connections",    s.connectionsCurrent + " current · " + s.connectionsActive
                + " active · " + s.connectionsAvailable + " available");
        root.getChildren().add(g2);

        // ---------- Host ----------
        root.getChildren().add(sectionHeader("Host"));
        GridPane g3 = grid();
        r = 0;
        kv(g3, r++, "Hostname", s.hostname);
        if (s.cpuCores != null) kv(g3, r++, "CPU",    s.cpuCores + " cores");
        if (s.memSizeMB != null) kv(g3, r++, "Memory", humanMemory(s.memSizeMB));
        kv(g3, r++, "OS", s.osName);
        root.getChildren().add(g3);

        // ---------- Replica set members (rich) ----------
        if (!s.replMembers.isEmpty()) {
            root.getChildren().add(sectionHeader("Replica set members (" + s.replMembers.size() + ")"));
            root.getChildren().add(renderReplMembers(s.replMembers));
            if (s.rawReplSetStatus != null) root.getChildren().add(replSetMetaPanel(s.rawReplSetStatus));
        }

        // ---------- Sharding ----------
        if (!s.shards.isEmpty()) {
            root.getChildren().add(sectionHeader("Shards (" + s.shards.size() + ")"));
            root.getChildren().add(renderShards(s.shards));
        }

        // ---------- Databases ----------
        if (!s.databases.isEmpty()) {
            TitledPane tp = new TitledPane(
                    "Databases (" + s.databases.size() + ")",
                    renderDatabases(s.databases));
            tp.setExpanded(false);
            root.getChildren().add(tp);
        }

        // ---------- Server status summary + drilldown ----------
        if (s.rawServerStatus != null) {
            root.getChildren().add(renderServerStatus(s.rawServerStatus));
        }

        // ---------- More details ----------
        TitledPane more = new TitledPane("More details", moreDetailsBody(s));
        more.setExpanded(false);
        root.getChildren().add(more);

        // ---------- Raw JSON viewers ----------
        root.getChildren().add(rawJsonButtons(s));
    }

    /** Group members by role (PRIMARY / SECONDARY / ARBITER / other) and render each in its own grid. */
    private static VBox renderReplMembers(List<ReplMember> members) {
        VBox out = new VBox(10);
        Map<String, List<ReplMember>> byState = new LinkedHashMap<>();
        byState.put("PRIMARY",    new ArrayList<>());
        byState.put("SECONDARY",  new ArrayList<>());
        byState.put("ARBITER",    new ArrayList<>());
        byState.put("Other",      new ArrayList<>());
        for (ReplMember m : members) {
            String bucket = switch (m.state == null ? "" : m.state) {
                case "PRIMARY", "SECONDARY", "ARBITER" -> m.state;
                default -> "Other";
            };
            byState.get(bucket).add(m);
        }
        for (var e : byState.entrySet()) {
            if (e.getValue().isEmpty()) continue;
            Label h = new Label(e.getKey() + " (" + e.getValue().size() + ")");
            h.setStyle(stateColour(e.getKey()) + " -fx-font-weight: bold;");
            VBox group = new VBox(4, h);
            group.setPadding(new Insets(4, 0, 0, 0));
            for (ReplMember m : e.getValue()) group.getChildren().add(memberCard(m));
            out.getChildren().add(group);
        }
        return out;
    }

    private static VBox memberCard(ReplMember m) {
        GridPane g = grid();
        int r = 0;
        String head = m.name + (m.self ? "  (this node)" : "");
        Label title = new Label(head);
        title.setStyle("-fx-font-weight: bold;");
        Label state = new Label("● " + m.state);
        state.setStyle(stateColour(m.state));
        HBox headerRow = new HBox(10, title, state);
        headerRow.setAlignment(Pos.CENTER_LEFT);

        kv(g, r++, "Health",   m.health == 1 ? "OK (1)" : "DOWN (0)");
        kv(g, r++, "Uptime",   humanDuration(Duration.ofSeconds(m.uptime)));
        if (m.lagSeconds != null) kv(g, r++, "Replication lag", m.lagSeconds + " s");
        if (m.priority != null)   kv(g, r++, "Priority", String.format(Locale.ROOT, "%.1f", m.priority));
        if (m.votes != null)      kv(g, r++, "Votes",    String.valueOf(m.votes));
        if (m.syncSource != null && !m.syncSource.isBlank()) kv(g, r++, "Sync source", m.syncSource);
        if (m.optimeDate != null) kv(g, r++, "Optime", LOCAL_TIME_FMT.format(m.optimeDate.toInstant()));
        if (m.lastHeartbeat != null) {
            long sinceSec = Math.max(0, (System.currentTimeMillis() - m.lastHeartbeat.getTime()) / 1000);
            kv(g, r++, "Last heartbeat", sinceSec + " s ago");
        }

        VBox box = new VBox(4, headerRow, g);
        box.setPadding(new Insets(8));
        box.setStyle(
                "-fx-background-color: #f9fafb;"
                + "-fx-border-color: #e5e7eb;"
                + "-fx-border-width: 1;"
                + "-fx-background-radius: 4;"
                + "-fx-border-radius: 4;");
        return box;
    }

    private static String stateColour(String state) {
        return "-fx-text-fill: " + switch (state == null ? "" : state) {
            case "PRIMARY"    -> "#16a34a";   // green
            case "SECONDARY"  -> "#2563eb";   // blue
            case "ARBITER"    -> "#7c3aed";   // purple
            case "RECOVERING", "STARTUP", "STARTUP2" -> "#d97706";  // amber
            case "DOWN", "REMOVED", "ROLLBACK" -> "#b91c1c";  // red
            default           -> "#6b7280";   // grey
        } + ";";
    }

    private static VBox replSetMetaPanel(Document rs) {
        GridPane g = grid();
        int r = 0;
        Object term = rs.get("term");
        if (term != null)  kv(g, r++, "Term", String.valueOf(term));
        Object majority = rs.get("majorityVoteCount");
        if (majority != null) kv(g, r++, "Majority vote count", String.valueOf(majority));
        Object writeMajority = rs.get("writeMajorityCount");
        if (writeMajority != null) kv(g, r++, "Write majority count", String.valueOf(writeMajority));
        Object hbMs = rs.get("heartbeatIntervalMillis");
        if (hbMs != null) kv(g, r++, "Heartbeat interval", hbMs + " ms");
        if (rs.get("electionCandidateMetrics") instanceof Document ecm) {
            Date last = ecm.getDate("lastElectionDate");
            if (last != null) kv(g, r++, "Last election", LOCAL_TIME_FMT.format(last.toInstant()));
        }
        VBox v = new VBox(g);
        v.setPadding(new Insets(4, 0, 0, 0));
        return v;
    }

    private static VBox renderShards(List<Shard> shards) {
        GridPane g = grid();
        int r = 0;
        kv(g, r++, "Shard id", "Host · state", true);
        for (Shard s : shards) {
            kv(g, r++, s.id, s.host + " · " + s.state);
        }
        VBox v = new VBox(g);
        v.setPadding(new Insets(6, 0, 0, 0));
        return v;
    }

    private static VBox renderDatabases(List<DbInfo> dbs) {
        GridPane g = grid();
        int r = 0;
        kv(g, r++, "Database", "Size on disk", true);
        for (DbInfo d : dbs) {
            kv(g, r++, d.name + (d.empty ? "  (empty)" : ""), humanBytes(d.sizeOnDisk));
        }
        VBox v = new VBox(g);
        v.setPadding(new Insets(6, 0, 0, 0));
        return v;
    }

    private static TitledPane renderServerStatus(Document status) {
        GridPane g = grid();
        int r = 0;
        Document op = subDoc(status, "opcounters");
        if (!op.isEmpty()) {
            kv(g, r++, "Inserts / Queries / Updates / Deletes",
                    asLong(op.get("insert")) + " / " + asLong(op.get("query"))
                    + " / " + asLong(op.get("update")) + " / " + asLong(op.get("delete")));
            kv(g, r++, "Commands / GetMores",
                    asLong(op.get("command")) + " / " + asLong(op.get("getmore")));
        }
        Document opRepl = subDoc(status, "opcountersRepl");
        if (!opRepl.isEmpty()) {
            kv(g, r++, "Repl insert / update / delete",
                    asLong(opRepl.get("insert")) + " / " + asLong(opRepl.get("update"))
                    + " / " + asLong(opRepl.get("delete")));
        }
        Document net = subDoc(status, "network");
        if (!net.isEmpty()) {
            kv(g, r++, "Network in / out (cumulative)",
                    humanBytes(asLong(net.get("bytesIn"))) + " / " + humanBytes(asLong(net.get("bytesOut"))));
            kv(g, r++, "Requests (cumulative)", String.valueOf(asLong(net.get("numRequests"))));
        }
        Document wtCache = subDoc(subDoc(status, "wiredTiger"), "cache");
        if (!wtCache.isEmpty()) {
            long inCache = asLong(wtCache.get("bytes currently in the cache"));
            long max     = asLong(wtCache.get("maximum bytes configured"));
            long dirty   = asLong(wtCache.get("tracked dirty bytes in the cache"));
            kv(g, r++, "WiredTiger cache",
                    humanBytes(inCache) + " of " + humanBytes(max)
                            + (max > 0 ? String.format(Locale.ROOT, " (%.1f%% full)", inCache * 100.0 / max) : ""));
            if (max > 0) {
                kv(g, r++, "WiredTiger dirty ratio",
                        String.format(Locale.ROOT, "%.1f%%", dirty * 100.0 / max));
            }
        }
        Document mem = subDoc(status, "mem");
        if (!mem.isEmpty()) {
            kv(g, r++, "Resident / virtual memory",
                    asLong(mem.get("resident")) + " MB / " + asLong(mem.get("virtual")) + " MB");
        }
        Document glq = subDoc(subDoc(status, "globalLock"), "currentQueue");
        if (!glq.isEmpty()) {
            kv(g, r++, "Global lock queue (r/w/total)",
                    asLong(glq.get("readers")) + " / " + asLong(glq.get("writers"))
                    + " / " + asLong(glq.get("total")));
        }
        Document cursor = subDoc(subDoc(status, "metrics"), "cursor");
        Document cursorOpen = subDoc(cursor, "open");
        if (!cursorOpen.isEmpty()) {
            kv(g, r++, "Cursors open (total / pinned / noTimeout)",
                    asLong(cursorOpen.get("total")) + " / " + asLong(cursorOpen.get("pinned"))
                    + " / " + asLong(cursorOpen.get("noTimeout")));
        }
        Document asserts = subDoc(status, "asserts");
        if (!asserts.isEmpty()) {
            kv(g, r++, "Asserts (regular / warning / user)",
                    asLong(asserts.get("regular")) + " / " + asLong(asserts.get("warning"))
                    + " / " + asLong(asserts.get("user")));
        }

        VBox box = new VBox(6, g);
        box.setPadding(new Insets(4, 0, 0, 0));

        TitledPane tp = new TitledPane("Server status (summary)", box);
        tp.setExpanded(true);
        return tp;
    }

    private static VBox moreDetailsBody(Snapshot s) {
        VBox box = new VBox(10);
        box.setPadding(new Insets(6, 0, 0, 0));

        if (s.rawHostInfo != null) {
            Document sys = subDoc(s.rawHostInfo, "system");
            Document osd = subDoc(s.rawHostInfo, "os");
            Document extra = subDoc(s.rawHostInfo, "extra");
            GridPane g = grid();
            int r = 0;
            kv(g, r++, "System", "", true);
            if (sys.get("cpuAddrSize")    != null) kv(g, r++, "CPU address size",   String.valueOf(sys.get("cpuAddrSize")) + "-bit");
            if (sys.get("numPhysicalCores") != null) kv(g, r++, "Physical cores",    String.valueOf(sys.get("numPhysicalCores")));
            if (sys.get("numCpuSockets")  != null) kv(g, r++, "CPU sockets",         String.valueOf(sys.get("numCpuSockets")));
            if (sys.get("cpuArch")        != null) kv(g, r++, "CPU arch",            String.valueOf(sys.get("cpuArch")));
            if (sys.get("numaEnabled")    != null) kv(g, r++, "NUMA enabled",        String.valueOf(sys.get("numaEnabled")));
            if (sys.get("memLimitMB")     != null) kv(g, r++, "Memory limit",         humanMemory(asLong(sys.get("memLimitMB"))));
            if (!osd.isEmpty()) {
                kv(g, r++, "Operating system", "", true);
                if (osd.get("type")    != null) kv(g, r++, "OS type",    String.valueOf(osd.get("type")));
                if (osd.get("name")    != null) kv(g, r++, "OS name",    String.valueOf(osd.get("name")));
                if (osd.get("version") != null) kv(g, r++, "OS version", String.valueOf(osd.get("version")));
            }
            if (!extra.isEmpty()) {
                kv(g, r++, "Extra", "", true);
                if (extra.get("kernelVersion")   != null) kv(g, r++, "Kernel",       String.valueOf(extra.get("kernelVersion")));
                if (extra.get("libcVersion")     != null) kv(g, r++, "libc",         String.valueOf(extra.get("libcVersion")));
                if (extra.get("versionString")   != null) kv(g, r++, "Version str",  String.valueOf(extra.get("versionString")));
                if (extra.get("pageSize")        != null) kv(g, r++, "Page size",    String.valueOf(extra.get("pageSize")) + " bytes");
                if (extra.get("numPages")        != null) kv(g, r++, "Num pages",    String.valueOf(extra.get("numPages")));
                if (extra.get("maxOpenFiles")    != null) kv(g, r++, "Max open files", String.valueOf(extra.get("maxOpenFiles")));
                if (extra.get("cpuFrequencyMHz") != null) kv(g, r++, "CPU frequency", extra.get("cpuFrequencyMHz") + " MHz");
            }
            box.getChildren().add(withHeader("Host information", g));
        }

        if (s.rawBuildInfo != null) {
            Document b = s.rawBuildInfo;
            GridPane g = grid();
            int r = 0;
            if (b.get("debug")          != null) kv(g, r++, "Debug build",   String.valueOf(b.get("debug")));
            if (b.get("bits")           != null) kv(g, r++, "Word size",     b.get("bits") + "-bit");
            if (b.get("allocator")      != null) kv(g, r++, "Allocator",     String.valueOf(b.get("allocator")));
            if (b.get("javascriptEngine") != null) kv(g, r++, "JS engine",   String.valueOf(b.get("javascriptEngine")));
            if (b.get("sysInfo")        != null) kv(g, r++, "System info",   String.valueOf(b.get("sysInfo")));
            if (b.get("openssl") instanceof Document ossl && !ossl.isEmpty()) {
                kv(g, r++, "OpenSSL", String.valueOf(ossl.getOrDefault("running", ossl.getOrDefault("compiled", "?"))));
            }
            @SuppressWarnings("unchecked")
            List<String> mods = (List<String>) b.get("modules");
            if (mods != null && !mods.isEmpty()) kv(g, r++, "Modules", String.join(", ", mods));
            if (r > 0) box.getChildren().add(withHeader("Build information", g));
        }

        if (s.rawHello != null) {
            Document h = s.rawHello;
            GridPane g = grid();
            int r = 0;
            if (h.get("maxBsonObjectSize")  != null) kv(g, r++, "Max BSON object size",  humanBytes(asLong(h.get("maxBsonObjectSize"))));
            if (h.get("maxMessageSizeBytes") != null) kv(g, r++, "Max message size",     humanBytes(asLong(h.get("maxMessageSizeBytes"))));
            if (h.get("maxWriteBatchSize")  != null) kv(g, r++, "Max write batch size",  String.valueOf(h.get("maxWriteBatchSize")));
            if (h.get("logicalSessionTimeoutMinutes") != null)
                kv(g, r++, "Logical session timeout", h.get("logicalSessionTimeoutMinutes") + " min");
            if (h.get("readOnly")           != null) kv(g, r++, "Read-only",            String.valueOf(h.get("readOnly")));
            @SuppressWarnings("unchecked")
            List<String> sasl = (List<String>) h.get("saslSupportedMechs");
            if (sasl != null && !sasl.isEmpty()) kv(g, r++, "SASL mechanisms", String.join(", ", sasl));
            @SuppressWarnings("unchecked")
            List<Document> compression = (List<Document>) h.get("compression");
            if (compression != null && !compression.isEmpty()) kv(g, r++, "Compression", compression.toString());
            Object connId = h.get("connectionId");
            if (connId != null) kv(g, r++, "Driver connection id", String.valueOf(connId));
            if (r > 0) box.getChildren().add(withHeader("Driver / wire protocol", g));
        }

        if (s.rawConnPoolStats != null) {
            Document cps = s.rawConnPoolStats;
            GridPane g = grid();
            int r = 0;
            if (cps.get("totalInUse")       != null) kv(g, r++, "Total in use",     String.valueOf(cps.get("totalInUse")));
            if (cps.get("totalAvailable")   != null) kv(g, r++, "Total available",  String.valueOf(cps.get("totalAvailable")));
            if (cps.get("totalCreated")     != null) kv(g, r++, "Total created",    String.valueOf(cps.get("totalCreated")));
            if (cps.get("numClientConnections") != null)
                kv(g, r++, "Client connections",    String.valueOf(cps.get("numClientConnections")));
            if (cps.get("numAScopedConnections") != null)
                kv(g, r++, "Scoped connections",    String.valueOf(cps.get("numAScopedConnections")));
            if (r > 0) box.getChildren().add(withHeader("Connection pool", g));
        }

        if (s.rawServerStatus != null) {
            Document sec = subDoc(s.rawServerStatus, "security");
            if (!sec.isEmpty()) {
                GridPane g = grid();
                int r = 0;
                if (sec.get("authentication") instanceof Document auth) {
                    kv(g, r++, "Authentication mechanisms", auth.toString());
                }
                if (sec.get("SSLServerSubjectName") != null) kv(g, r++, "SSL server subject", String.valueOf(sec.get("SSLServerSubjectName")));
                if (sec.get("SSLServerHasCertificateAuthority") != null)
                    kv(g, r++, "SSL CA configured", String.valueOf(sec.get("SSLServerHasCertificateAuthority")));
                if (r > 0) box.getChildren().add(withHeader("Security", g));
            }

            Document txn = subDoc(s.rawServerStatus, "transactions");
            if (!txn.isEmpty()) {
                GridPane g = grid();
                int r = 0;
                if (txn.get("currentOpen")     != null) kv(g, r++, "Currently open",   String.valueOf(txn.get("currentOpen")));
                if (txn.get("currentActive")   != null) kv(g, r++, "Currently active", String.valueOf(txn.get("currentActive")));
                if (txn.get("currentInactive") != null) kv(g, r++, "Currently inactive", String.valueOf(txn.get("currentInactive")));
                if (txn.get("totalCommitted")  != null) kv(g, r++, "Total committed",  String.valueOf(txn.get("totalCommitted")));
                if (txn.get("totalAborted")    != null) kv(g, r++, "Total aborted",    String.valueOf(txn.get("totalAborted")));
                if (r > 0) box.getChildren().add(withHeader("Transactions", g));
            }

            Document repl = subDoc(s.rawServerStatus, "repl");
            if (!repl.isEmpty()) {
                GridPane g = grid();
                int r = 0;
                if (repl.get("setName")     != null) kv(g, r++, "setName",     String.valueOf(repl.get("setName")));
                if (repl.get("setVersion")  != null) kv(g, r++, "setVersion",  String.valueOf(repl.get("setVersion")));
                if (repl.get("rbid")        != null) kv(g, r++, "Rollback id", String.valueOf(repl.get("rbid")));
                if (repl.get("primary")     != null) kv(g, r++, "Primary",     String.valueOf(repl.get("primary")));
                if (repl.get("isWritablePrimary") != null) kv(g, r++, "isWritablePrimary", String.valueOf(repl.get("isWritablePrimary")));
                if (r > 0) box.getChildren().add(withHeader("Replication", g));
            }
        }

        if (box.getChildren().isEmpty()) {
            Label none = new Label("No additional details available.");
            none.setStyle("-fx-text-fill: #6b7280;");
            box.getChildren().add(none);
        }
        return box;
    }

    private static HBox rawJsonButtons(Snapshot s) {
        HBox row = new HBox(8);
        row.setPadding(new Insets(8, 0, 0, 0));
        Label lbl = new Label("View JSON:");
        lbl.setStyle("-fx-text-fill: #6b7280;");
        row.getChildren().add(lbl);
        addJsonButton(row, "hello", s.rawHello);
        addJsonButton(row, "buildInfo", s.rawBuildInfo);
        addJsonButton(row, "hostInfo", s.rawHostInfo);
        addJsonButton(row, "serverStatus", s.rawServerStatus);
        addJsonButton(row, "replSetGetStatus", s.rawReplSetStatus);
        addJsonButton(row, "listShards", s.rawListShards);
        addJsonButton(row, "connPoolStats", s.rawConnPoolStats);
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        row.getChildren().add(0, spacer);
        return row;
    }

    private static void addJsonButton(HBox row, String label, Document doc) {
        if (doc == null) return;
        Button b = new Button(label);
        b.setOnAction(e -> showJson(
                b.getScene() == null ? null : b.getScene().getWindow(), label, doc));
        row.getChildren().add(b);
    }

    private static void showJson(Window owner, String title, Document doc) {
        Dialog<Void> d = new Dialog<>();
        d.initOwner(owner);
        d.setTitle("Raw JSON · " + title);
        TextArea ta = new TextArea(doc.toJson(MongoService.JSON_RELAXED));
        ta.setEditable(false);
        ta.setStyle("-fx-font-family: 'Menlo','Monaco',monospace;");
        ta.setPrefSize(760, 560);
        d.getDialogPane().setContent(ta);
        d.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        d.showAndWait();
    }

    // ==================== helpers ==========================================

    private static Document subDoc(Document d, String field) {
        if (d == null) return new Document();
        Object v = d.get(field);
        return v instanceof Document sub ? sub : new Document();
    }

    private static Integer intOf(Object v) {
        return v instanceof Number n ? n.intValue() : null;
    }

    private static VBox withHeader(String text, Node body) {
        Label h = new Label(text);
        h.setStyle("-fx-font-weight: bold;");
        VBox v = new VBox(4, h, body);
        return v;
    }

    private static Label sectionHeader(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-weight: bold; -fx-font-size: 14px; -fx-padding: 8 0 2 0;");
        return l;
    }

    private static GridPane grid() {
        GridPane g = new GridPane();
        g.setHgap(16);
        g.setVgap(4);
        return g;
    }

    private static void kv(GridPane g, int row, String key, String value) {
        kv(g, row, key, value, false);
    }

    private static void kv(GridPane g, int row, String key, String value, boolean bold) {
        Label k = new Label(key);
        k.setStyle("-fx-text-fill: #6b7280;" + (bold ? "-fx-font-weight: bold;" : ""));
        Label v = new Label(value == null ? "—" : value);
        if (bold) v.setStyle("-fx-font-weight: bold;");
        g.add(k, 0, row);
        g.add(v, 1, row);
    }

    private static String humanDuration(Duration d) {
        long s = d.getSeconds();
        long days = s / 86400;
        long hours = (s % 86400) / 3600;
        long mins = (s % 3600) / 60;
        if (days > 0) return String.format(Locale.ROOT, "%dd %dh", days, hours);
        if (hours > 0) return String.format(Locale.ROOT, "%dh %dm", hours, mins);
        return mins + "m " + (s % 60) + "s";
    }

    private static String humanMemory(long mb) {
        if (mb >= 1024) return String.format(Locale.ROOT, "%.1f GB", mb / 1024.0);
        return mb + " MB";
    }

    private static String humanBytes(long n) {
        double v = n;
        String[] u = { "B", "KB", "MB", "GB", "TB" };
        int i = 0;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return String.format(Locale.ROOT, "%.1f %s", v, u[i]);
    }
}
