package com.kubrik.mex.maint.reconfig;

import com.kubrik.mex.maint.model.ReconfigSpec;
import com.kubrik.mex.maint.model.ReconfigSpec.Member;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * v2.7 Q2.7-D — Translates a typed {@link ReconfigSpec.Request} into
 * the BSON document {@code replSetReconfig} expects, and rehydrates a
 * {@code replSetGetConfig} reply back into our typed {@link Member}
 * list. Kept off the runner so both sides are unit-testable without a
 * MongoDB driver mock.
 */
public final class ReconfigSerializer {

    private final ReconfigPreflight preflight;

    public ReconfigSerializer() {
        this(new ReconfigPreflight());
    }

    ReconfigSerializer(ReconfigPreflight preflight) {
        this.preflight = preflight;
    }

    /** Build the {@code newConfig} body for {@code replSetReconfig}.
     *  The {@code version} bump is the caller's responsibility —
     *  {@link #bumpedVersion} does it consistently so every call-site
     *  gets the same +1 arithmetic. */
    public Document toReconfigBody(ReconfigSpec.Request req) {
        List<Member> proposed = preflight.applyChange(
                req.currentMembers(), req.change());
        Document cfg = new Document()
                .append("_id", req.replicaSetName())
                .append("version", bumpedVersion(req.currentConfigVersion()))
                .append("members", membersToBson(proposed));
        return cfg;
    }

    /** The exact arithmetic the MongoDB driver would do server-side.
     *  Exposed so the runner can assert parity pre-dispatch. */
    public int bumpedVersion(int current) { return current + 1; }

    public List<Document> membersToBson(List<Member> members) {
        List<Document> out = new ArrayList<>(members.size());
        for (Member m : members) {
            Document d = new Document()
                    .append("_id", m.id())
                    .append("host", m.host())
                    .append("priority", m.priority())
                    .append("votes", m.votes());
            // Only include flags when they diverge from defaults — the
            // server stores them either way, but a minimal body is
            // easier to eyeball in audit JSON.
            if (m.hidden()) d.append("hidden", true);
            if (m.arbiterOnly()) d.append("arbiterOnly", true);
            if (!m.buildIndexes()) d.append("buildIndexes", false);
            if (m.slaveDelay() > 0) d.append("slaveDelay", m.slaveDelay());
            out.add(d);
        }
        return out;
    }

    /** Inverse of {@link #toReconfigBody} — parse a {@code
     *  replSetGetConfig} reply into the typed shape the preflight
     *  wants. Tolerates the optional fields; defaults them like the
     *  server does. */
    @SuppressWarnings("unchecked")
    public Optional<ReconfigSpec.Request> fromConfigReply(Document configReply,
                                                         ReconfigSpec.Change change) {
        if (configReply == null) return Optional.empty();
        Document cfg = configReply.get("config", Document.class);
        if (cfg == null) return Optional.empty();
        String setName = cfg.getString("_id");
        Object versionRaw = cfg.get("version");
        if (setName == null || !(versionRaw instanceof Number)) return Optional.empty();
        int version = ((Number) versionRaw).intValue();
        List<Document> rawMembers = (List<Document>) cfg.get("members", List.class);
        if (rawMembers == null) return Optional.empty();
        List<Member> members = new ArrayList<>(rawMembers.size());
        for (Document m : rawMembers) {
            // Live replSetGetConfig returns numeric fields as a mix of
            // Int32 / Int64 / Double depending on how the member was
            // created — getInteger() ClassCasts on anything but Int32.
            // Coerce via Number.intValue() so the parser survives every
            // supported server version.
            members.add(new Member(
                    intOf(m.get("_id"), 0),
                    m.getString("host"),
                    intOf(m.get("priority"), 1),
                    intOf(m.get("votes"), 1),
                    m.getBoolean("hidden", false),
                    m.getBoolean("arbiterOnly", false),
                    m.getBoolean("buildIndexes", true),
                    doubleOf(m.get("slaveDelay"))));
        }
        return Optional.of(new ReconfigSpec.Request(setName, version, members, change));
    }

    private static double doubleOf(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Number n) return n.doubleValue();
        return 0.0;
    }

    private static int intOf(Object v, int fallback) {
        if (v instanceof Number n) return n.intValue();
        return fallback;
    }
}
