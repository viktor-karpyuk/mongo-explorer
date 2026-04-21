package com.kubrik.mex.monitoring;

import com.kubrik.mex.core.MongoService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives server-side slow-query profiling state via {@code profile} commands.
 * See technical-spec §6.3 — enabling sampling means setting profiling level 1
 * on every user database with a {@code slowms} threshold; disabling restores
 * level 0. System databases ({@code admin}, {@code local}, {@code config}) are
 * skipped — the mongod rejects profiling on them anyway and logging noise helps
 * no one.
 */
public final class ProfilingController {

    private static final Logger log = LoggerFactory.getLogger(ProfilingController.class);

    private static final java.util.Set<String> SYSTEM_DBS = java.util.Set.of("admin", "local", "config");

    private ProfilingController() {}

    /** Set profiling level {@code 1} with the given {@code slowMs} on every non-system database. */
    public static Result enable(MongoService mongo, int slowMs) {
        return apply(mongo, 1, slowMs);
    }

    /** Set profiling level {@code 0} on every non-system database; restores baseline. */
    public static Result disable(MongoService mongo) {
        return apply(mongo, 0, 0);
    }

    private static Result apply(MongoService mongo, int level, int slowMs) {
        List<String> changed = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        if (mongo == null) return new Result(changed, failed);
        List<String> dbNames;
        try {
            dbNames = mongo.listDatabaseNames();
        } catch (Throwable t) {
            log.debug("profile {} listDatabaseNames failed: {}", level == 1 ? "enable" : "disable", t.toString());
            return new Result(changed, failed);
        }
        for (String dbName : dbNames) {
            if (SYSTEM_DBS.contains(dbName)) continue;
            try {
                Document cmd = new Document("profile", level);
                if (level == 1) cmd.append("slowms", slowMs);
                mongo.database(dbName).runCommand(cmd);
                changed.add(dbName);
            } catch (Throwable t) {
                log.debug("profile level={} slowms={} failed on {}: {}", level, slowMs, dbName, t.toString());
                failed.add(dbName);
            }
        }
        return new Result(changed, failed);
    }

    /** Summary of a profile-command sweep: databases touched vs databases the server rejected. */
    public record Result(List<String> changedDbs, List<String> failedDbs) {
        public int changedCount() { return changedDbs.size(); }
        public int failedCount()  { return failedDbs.size(); }
    }
}
