package com.kubrik.mex.security.audit;

import java.util.Map;
import java.util.Objects;

/**
 * v2.6 Q2.6-C2 — one parsed entry from MongoDB's native audit log.
 *
 * <p>MongoDB's {@code auditLog.destination = file} writes one JSON
 * object per line with fields like:</p>
 * <pre>
 * {"atype":"authenticate","ts":{"$date":"2026-04-21T10:00:00Z"},
 *  "local":{"ip":"127.0.0.1","port":27017},
 *  "remote":{"ip":"10.0.0.5","port":55233},
 *  "users":[{"user":"dba","db":"admin"}],
 *  "roles":[{"role":"root","db":"admin"}],
 *  "param":{"user":"dba","db":"admin","mechanism":"SCRAM-SHA-256"},
 *  "result":0}
 * </pre>
 *
 * <p>The fields vary slightly across server versions 4.x → 7.x; this
 * record keeps the union so the FTS index can span every shape without
 * losing information.</p>
 */
public record AuditEvent(
        String atype,
        long tsMs,
        String who,
        String whoDb,
        String fromHost,
        int result,
        Map<String, Object> param,
        String rawJson
) {
    public AuditEvent {
        Objects.requireNonNull(atype, "atype");
        if (who == null) who = "";
        if (whoDb == null) whoDb = "";
        if (fromHost == null) fromHost = "";
        if (rawJson == null) rawJson = "";
        param = param == null ? Map.of() : Map.copyOf(param);
    }
}
