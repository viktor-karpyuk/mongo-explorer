package com.kubrik.mex.migration.log;

import com.kubrik.mex.migration.JobId;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

/** Thin SLF4J wrapper that emits structured fields via logstash-logback-encoder markers.
 *  <p>
 *  Every line is tagged with the job ID (via MDC so it appears in the pattern layout) and
 *  carries {@code event=…} plus any extra key/value pairs. The accompanying human-readable
 *  message reuses the event name so operators can grep either way.
 *  <p>
 *  See docs/mvp-technical-spec.md §12.1. */
public final class JobLogger {

    public static final String MDC_JOB_ID = "jobId";

    private final Logger log;
    private final JobId jobId;

    public JobLogger(Class<?> owner, JobId jobId) {
        this.log = LoggerFactory.getLogger(owner);
        this.jobId = jobId;
    }

    public void info(String event, Object... kv) { emit(Level.INFO, event, buildFields(kv)); }
    public void warn(String event, Object... kv) { emit(Level.WARN, event, buildFields(kv)); }
    public void error(String event, Throwable cause, Object... kv) {
        Map<String, Object> fields = buildFields(kv);
        if (cause != null) fields.put("cause", cause.getClass().getSimpleName() + ": " + cause.getMessage());
        emit(Level.ERROR, event, fields);
    }

    public void debug(String event, Object... kv) {
        if (!log.isDebugEnabled()) return;
        emit(Level.DEBUG, event, buildFields(kv));
    }

    private void emit(Level level, String event, Map<String, Object> fields) {
        fields.put("event", event);
        fields.put("jobId", jobId.value());
        String prev = MDC.get(MDC_JOB_ID);
        MDC.put(MDC_JOB_ID, jobId.value());
        try {
            switch (level) {
                case DEBUG -> log.debug(Markers.appendEntries(fields), event);
                case INFO  -> log.info (Markers.appendEntries(fields), event);
                case WARN  -> log.warn (Markers.appendEntries(fields), event);
                case ERROR -> log.error(Markers.appendEntries(fields), event);
            }
        } finally {
            if (prev == null) MDC.remove(MDC_JOB_ID);
            else MDC.put(MDC_JOB_ID, prev);
        }
    }

    /** Build an ordered map from varargs pairs. Odd length → last key with null value.
     *  Duplicates keep the last value. */
    private static Map<String, Object> buildFields(Object[] kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (kv == null) return m;
        for (int i = 0; i < kv.length; i += 2) {
            Object k = kv[i];
            Object v = i + 1 < kv.length ? kv[i + 1] : null;
            if (k != null) m.put(k.toString(), v);
        }
        return m;
    }

    private enum Level { DEBUG, INFO, WARN, ERROR }
}
