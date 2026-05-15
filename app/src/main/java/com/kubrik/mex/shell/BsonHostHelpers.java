package com.kubrik.mex.shell;

import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;

/**
 * Host bindings for the BSON literal helpers users expect from
 * mongosh: {@code ObjectId(...)}, {@code ISODate(...)}, {@code
 * NumberLong(...)}, {@code NumberDecimal(...)}, {@code UUID(...)},
 * {@code BinData(...)}, {@code Timestamp(...)}, {@code MinKey()},
 * {@code MaxKey()}.
 *
 * <p>Each helper is a {@link ProxyExecutable} so the JS engine can
 * invoke it both as {@code ObjectId("…")} and (for compat with
 * mongosh) as {@code new ObjectId("…")} — Graal honors {@code execute}
 * for the latter when no constructor is defined.
 */
final class BsonHostHelpers {

    private BsonHostHelpers() {}

    static ProxyExecutable objectId() {
        return args -> {
            if (args.length == 0) return new ObjectId();
            String s = args[0].asString();
            return new ObjectId(s);
        };
    }

    static ProxyExecutable isoDate() {
        return args -> {
            if (args.length == 0) return new Date();
            Value v = args[0];
            if (v.isString()) {
                String s = v.asString();
                // Accept both "2026-05-14" (date) and full ISO-8601.
                Instant parsed;
                if (s.length() == 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
                    parsed = Instant.parse(s + "T00:00:00Z");
                } else {
                    parsed = DateTimeFormatter.ISO_DATE_TIME
                            .parse(s, Instant::from);
                }
                return Date.from(parsed);
            }
            if (v.isNumber()) {
                return new Date(v.asLong());
            }
            throw new IllegalArgumentException("ISODate expects a string or millis");
        };
    }

    static ProxyExecutable numberLong() {
        return args -> {
            if (args.length == 0) return 0L;
            Value v = args[0];
            return v.isString() ? Long.parseLong(v.asString()) : v.asLong();
        };
    }

    static ProxyExecutable numberInt() {
        return args -> {
            if (args.length == 0) return 0;
            Value v = args[0];
            return v.isString() ? Integer.parseInt(v.asString()) : v.asInt();
        };
    }

    static ProxyExecutable numberDecimal() {
        return args -> {
            if (args.length == 0) return Decimal128.POSITIVE_ZERO;
            Value v = args[0];
            return v.isString() ? Decimal128.parse(v.asString())
                    : Decimal128.parse(String.valueOf(v.asDouble()));
        };
    }

    static ProxyExecutable uuid() {
        return args -> {
            if (args.length == 0) return UUID.randomUUID();
            return UUID.fromString(args[0].asString());
        };
    }

    /** {@code BinData(subtype, base64)} — mongosh shape. */
    static ProxyExecutable binData() {
        return args -> {
            if (args.length < 2) {
                throw new IllegalArgumentException("BinData(subtype, base64String)");
            }
            int subtype = args[0].asInt();
            byte[] bytes = Base64.getDecoder().decode(args[1].asString());
            return new Binary((byte) subtype, bytes);
        };
    }

    /** {@code Timestamp(seconds, increment)} — replication BSON timestamp. */
    static ProxyExecutable timestamp() {
        return args -> {
            int seconds   = args.length > 0 ? args[0].asInt() : (int)(System.currentTimeMillis()/1000);
            int increment = args.length > 1 ? args[1].asInt() : 0;
            return new BsonTimestamp(seconds, increment);
        };
    }

    static ProxyExecutable minKey() { return args -> new MinKey(); }
    static ProxyExecutable maxKey() { return args -> new MaxKey(); }

    /** mongosh-style {@code RegExp(pattern, flags)} → BSON regex. */
    static ProxyExecutable regExp() {
        return args -> {
            String pat   = args.length > 0 ? args[0].asString() : "";
            String flags = args.length > 1 ? args[1].asString() : "";
            return new BsonRegularExpression(pat, flags);
        };
    }
}
