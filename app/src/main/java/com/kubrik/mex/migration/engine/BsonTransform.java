package com.kubrik.mex.migration.engine;

import com.kubrik.mex.migration.spec.CastOp;
import com.kubrik.mex.migration.spec.TransformSpec;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Compiled, immutable per-document transform. Created once per collection from a
 *  {@link TransformSpec} and reused for every batch.
 *  <p>
 *  Supports field-level rename, drop, and a curated set of casts (MVP scope per the
 *  functional spec §10.4). Filter and projection live on the cursor, not here.
 */
public final class BsonTransform {

    public static final BsonTransform IDENTITY = new BsonTransform(Map.of(), Set.of(), Map.of());

    private final Map<String, String> rename;
    private final Set<String> drop;
    private final Map<String, CastOp> casts;   // keyed by field name

    private BsonTransform(Map<String, String> rename, Set<String> drop, Map<String, CastOp> casts) {
        this.rename = rename;
        this.drop = drop;
        this.casts = casts;
    }

    public static BsonTransform compile(TransformSpec spec) {
        if (spec == null || spec.isEmpty()) return IDENTITY;
        if (spec.rename().isEmpty() && spec.drop().isEmpty() && spec.cast().isEmpty()) return IDENTITY;
        Map<String, CastOp> castMap = new HashMap<>(spec.cast().size());
        for (CastOp op : spec.cast()) castMap.put(op.field(), op);
        return new BsonTransform(
                Map.copyOf(spec.rename()),
                Set.copyOf(spec.drop()),
                Map.copyOf(castMap));
    }

    public boolean isIdentity() { return this == IDENTITY; }

    /** Apply the transform to a document. Returns a new {@link RawBsonDocument}; the input is
     *  left untouched. Throws {@link PerDocumentException} on cast failure. */
    public RawBsonDocument apply(RawBsonDocument input) {
        if (isIdentity()) return input;
        BsonDocument out = new BsonDocument();
        BsonValue id = null;
        for (Map.Entry<String, BsonValue> e : input.entrySet()) {
            String field = e.getKey();
            if ("_id".equals(field)) id = e.getValue();
            if (drop.contains(field)) continue;
            String newField = rename.getOrDefault(field, field);
            BsonValue value = e.getValue();
            CastOp cast = casts.get(field);
            if (cast != null) value = applyCast(id, cast, value);
            out.put(newField, value);
        }
        return RawBsonDocument.parse(out.toJson());
    }

    private static BsonValue applyCast(BsonValue docId, CastOp op, BsonValue value) {
        if (value == null || value.isNull()) return value;
        try {
            return switch (op.to().toLowerCase()) {
                case "string"   -> new BsonString(coerceString(value));
                case "int"      -> new BsonInt32(coerceInt(value));
                case "long"     -> new BsonInt64(coerceLong(value));
                case "double"   -> new BsonDouble(coerceDouble(value));
                case "date"     -> coerceDate(value);
                case "objectid" -> coerceObjectId(value);
                default -> throw new PerDocumentException(docId, op.field(),
                        "unsupported target type `" + op.to() + "`");
            };
        } catch (PerDocumentException e) {
            throw e;
        } catch (Exception e) {
            throw new PerDocumentException(docId, op.field(),
                    "cast " + op.from() + " → " + op.to() + " failed: " + e.getMessage());
        }
    }

    private static String coerceString(BsonValue v) {
        if (v.isString()) return v.asString().getValue();
        if (v.isInt32())  return Integer.toString(v.asInt32().getValue());
        if (v.isInt64())  return Long.toString(v.asInt64().getValue());
        if (v.isDouble()) return Double.toString(v.asDouble().getValue());
        if (v.isObjectId()) return v.asObjectId().getValue().toHexString();
        if (v.isBoolean()) return Boolean.toString(v.asBoolean().getValue());
        return v.toString();
    }

    private static int coerceInt(BsonValue v) {
        if (v.isInt32())  return v.asInt32().getValue();
        if (v.isInt64())  return Math.toIntExact(v.asInt64().getValue());
        if (v.isDouble()) return (int) v.asDouble().getValue();
        if (v.isString()) return Integer.parseInt(v.asString().getValue().trim());
        throw new IllegalArgumentException("cannot coerce " + v.getBsonType() + " to int");
    }

    private static long coerceLong(BsonValue v) {
        if (v.isInt32())  return v.asInt32().getValue();
        if (v.isInt64())  return v.asInt64().getValue();
        if (v.isDouble()) return (long) v.asDouble().getValue();
        if (v.isString()) return Long.parseLong(v.asString().getValue().trim());
        throw new IllegalArgumentException("cannot coerce " + v.getBsonType() + " to long");
    }

    private static double coerceDouble(BsonValue v) {
        if (v.isInt32())  return v.asInt32().getValue();
        if (v.isInt64())  return v.asInt64().getValue();
        if (v.isDouble()) return v.asDouble().getValue();
        if (v.isString()) return Double.parseDouble(v.asString().getValue().trim());
        throw new IllegalArgumentException("cannot coerce " + v.getBsonType() + " to double");
    }

    private static BsonValue coerceDate(BsonValue v) {
        if (v.isDateTime()) return v;
        if (v.isString()) {
            try {
                return new BsonDateTime(Instant.parse(v.asString().getValue()).toEpochMilli());
            } catch (DateTimeParseException ex) {
                throw new IllegalArgumentException("expected ISO-8601 date, got `" + v.asString().getValue() + "`");
            }
        }
        if (v.isInt64())  return new BsonDateTime(v.asInt64().getValue());
        if (v.isInt32())  return new BsonDateTime(v.asInt32().getValue());
        throw new IllegalArgumentException("cannot coerce " + v.getBsonType() + " to date");
    }

    private static BsonValue coerceObjectId(BsonValue v) {
        if (v.isObjectId()) return v;
        if (v.isString()) {
            String s = v.asString().getValue();
            if (!ObjectId.isValid(s)) {
                throw new IllegalArgumentException("expected 24-hex-char ObjectId, got `" + s + "`");
            }
            return new BsonObjectId(new ObjectId(s));
        }
        throw new IllegalArgumentException("cannot coerce " + v.getBsonType() + " to ObjectId");
    }

    // Parallel HashSet import kept private so callers don't leak implementation detail.
    private static <T> Set<T> immutableSet(java.util.Collection<T> input) {
        return Set.copyOf(new HashSet<>(input));
    }
}
