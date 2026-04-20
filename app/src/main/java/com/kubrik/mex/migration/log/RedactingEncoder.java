package com.kubrik.mex.migration.log;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.nio.charset.StandardCharsets;

/** Logback encoder that pipes every formatted log line through {@link Redactor} before it
 *  reaches disk or stdout. Declared in {@code logback.xml} — no code wiring needed.
 *  <p>
 *  Redaction is applied once per record. The {@link Redactor} is thread-safe and holds only
 *  immutable compiled patterns, so a single shared instance is reused. */
public final class RedactingEncoder extends PatternLayoutEncoder {

    private static final Redactor REDACTOR = Redactor.defaultInstance();

    @Override
    public byte[] encode(ILoggingEvent event) {
        String line = getLayout().doLayout(event);
        return REDACTOR.redact(line).getBytes(StandardCharsets.UTF_8);
    }
}
