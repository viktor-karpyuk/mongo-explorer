package com.kubrik.mex.shell;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * mongosh-style cursor: supports the chainable shape
 * {@code db.coll.find({}).sort({a:-1}).skip(10).limit(20).toArray()}.
 *
 * <p>Backed by either a {@link FindIterable} or an {@link
 * AggregateIterable}; chainable mutators apply to the underlying
 * iterable when supported and otherwise no-op (with no error — matches
 * mongosh's lenient behavior on aggregate cursors).
 */
final class CursorProxy implements ProxyObject {

    private MongoIterable<Document> source;
    private final Map<String, Object> members = new LinkedHashMap<>();

    CursorProxy(MongoIterable<Document> source) {
        this.source = source;
        bind();
    }

    private void bind() {
        members.put("sort", (ProxyExecutable) a -> {
            Document spec = JsBsonConverter.toDocument(a[0]);
            if (source instanceof FindIterable<Document> f)      source = f.sort(spec);
            else if (source instanceof AggregateIterable<Document>) {
                /* aggregate cursors have no sort modifier; ignore — pipeline owns sort */
            }
            return this;
        });
        members.put("skip", (ProxyExecutable) a -> {
            int n = a[0].asInt();
            if (source instanceof FindIterable<Document> f) source = f.skip(n);
            return this;
        });
        members.put("limit", (ProxyExecutable) a -> {
            int n = a[0].asInt();
            if (source instanceof FindIterable<Document> f)      source = f.limit(n);
            else if (source instanceof AggregateIterable<Document>) { /* no-op */ }
            return this;
        });
        members.put("projection", (ProxyExecutable) a -> {
            Document p = JsBsonConverter.toDocument(a[0]);
            if (source instanceof FindIterable<Document> f) source = f.projection(p);
            return this;
        });
        members.put("batchSize", (ProxyExecutable) a -> {
            int n = a[0].asInt();
            if (source instanceof FindIterable<Document> f)      source = f.batchSize(n);
            else if (source instanceof AggregateIterable<Document> ai) source = ai.batchSize(n);
            return this;
        });
        members.put("toArray", (ProxyExecutable) a -> {
            List<Document> out = new ArrayList<>();
            source.into(out);
            return out;
        });
        members.put("forEach", (ProxyExecutable) a -> {
            Value fn = a[0];
            try (MongoCursor<Document> cur = source.iterator()) {
                while (cur.hasNext()) fn.executeVoid(cur.next());
            }
            return null;
        });
        members.put("hasNext", (ProxyExecutable) a -> source.iterator().hasNext());
        members.put("next",    (ProxyExecutable) a -> source.iterator().next());
        members.put("count",   (ProxyExecutable) a -> {
            int c = 0;
            try (MongoCursor<Document> cur = source.iterator()) {
                while (cur.hasNext()) { cur.next(); c++; }
            }
            return c;
        });
    }

    @Override public Object getMember(String key) { return members.get(key); }
    @Override public Object getMemberKeys()       { return members.keySet().toArray(new String[0]); }
    @Override public boolean hasMember(String key){ return members.containsKey(key); }
    @Override public void putMember(String k, Value v) {
        throw new UnsupportedOperationException("cursor members are read-only");
    }
}
