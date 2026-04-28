package com.kubrik.mex.migration.engine;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.log.JobLogger;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** SCOPE-12 — copy non-built-in users from each source database to the corresponding target
 *  database after all collections finish.
 *
 *  <p><b>Password hashes are NOT preserved.</b> The public {@code createUser} command rejects
 *  raw credentials / hash injection — preserving hashes requires direct manipulation of
 *  {@code admin.system.users}, which isn't a supported operation. Copied users are created
 *  with a one-shot placeholder password; the preflight warning tells the user to re-set
 *  passwords after the job. Roles, mechanisms, and the username/db pair are preserved.
 *
 *  <p>Per-user failures are counted in {@link Metrics#addUserFailed()} and logged as warnings;
 *  the stage never fails the job (§4.3). */
public final class UsersCopier {

    private static final Logger log = LoggerFactory.getLogger(UsersCopier.class);

    /** Built-in roles / users we never copy — they exist on every MongoDB instance. */
    private static final Set<String> BUILTIN_USERNAMES = Set.of("__system");

    private final MongoService src;
    private final MongoService tgt;
    private final Metrics metrics;
    private final JobLogger jlog;

    public UsersCopier(MongoService src, MongoService tgt, Metrics metrics, JobLogger jlog) {
        this.src = src;
        this.tgt = tgt;
        this.metrics = metrics;
        this.jlog = jlog;
    }

    /** Copy every non-built-in user from {@code sourceDb} to {@code targetDb}. Safe to call
     *  once per destination DB. Returns the number of users successfully created. */
    public int copy(String sourceDb, String targetDb) {
        List<Document> users = listUsers(sourceDb);
        if (users.isEmpty()) {
            jlog.info("users_stage_skip", "reason", "no users", "db", sourceDb);
            return 0;
        }
        int created = 0;
        for (Document user : users) {
            String name = user.getString("user");
            if (name == null || BUILTIN_USERNAMES.contains(name)) continue;
            try {
                createOnTarget(targetDb, user);
                created++;
            } catch (Exception e) {
                metrics.addUserFailed();
                jlog.warn("user_copy_failed",
                        "user", name,
                        "sourceDb", sourceDb,
                        "targetDb", targetDb,
                        "error", e.getMessage());
                log.warn("user {}/{} copy failed: {}", sourceDb, name, e.getMessage());
            }
        }
        jlog.info("users_stage_done", "db", sourceDb, "created", created, "total", users.size());
        return created;
    }

    @SuppressWarnings("unchecked")
    private List<Document> listUsers(String db) {
        try {
            Document resp = src.database(db).runCommand(new Document("usersInfo", 1));
            Object raw = resp.get("users");
            return raw instanceof List ? (List<Document>) raw : List.of();
        } catch (Exception e) {
            metrics.addUserFailed();
            jlog.warn("users_info_failed", "db", db, "error", e.getMessage());
            log.warn("usersInfo on {} failed: {}", db, e.getMessage());
            return List.of();
        }
    }

    private static final String PLACEHOLDER_PASSWORD = "__mex_placeholder_reset_me__";

    private void createOnTarget(String targetDb, Document user) {
        Document cmd = new Document("createUser", user.getString("user"))
                .append("pwd", PLACEHOLDER_PASSWORD)
                .append("roles", roleListFor(user));
        Object mechanisms = user.get("mechanisms");
        if (mechanisms instanceof List<?> l && !l.isEmpty()) cmd.append("mechanisms", l);
        tgt.database(targetDb).runCommand(cmd);
    }

    @SuppressWarnings("unchecked")
    private List<Document> roleListFor(Document user) {
        Object raw = user.get("roles");
        if (!(raw instanceof List)) return List.of();
        List<Document> out = new ArrayList<>();
        for (Object r : (List<Object>) raw) {
            if (!(r instanceof Document rd)) continue;
            String roleName = rd.getString("role");
            if (roleName == null || BUILTIN_USERNAMES.contains(roleName)) continue;
            Document roleDoc = new Document("role", roleName).append("db", rd.getString("db"));
            out.add(roleDoc);
        }
        return out;
    }
}
