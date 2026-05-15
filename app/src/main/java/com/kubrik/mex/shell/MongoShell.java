package com.kubrik.mex.shell;

import com.kubrik.mex.core.MongoService;
import com.mongodb.client.MongoClient;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * mongosh-shaped JavaScript shell for an open {@link MongoService}.
 * One {@code MongoShell} owns one Graal {@link Context}; reuse the
 * same instance across statements so {@code use("db")} and
 * user-defined variables persist between Run clicks.
 *
 * <p>Thread-safety: a Graal Context is single-threaded by default —
 * {@link #eval(String)} synchronizes so callers can submit from any
 * virtual thread (the UI runs each statement on a fresh virtual
 * thread). {@link #cancel()} signals the running statement to abort
 * via {@link Context#interrupt(java.time.Duration)}.
 */
public final class MongoShell implements AutoCloseable {

    private final MongoService service;
    private final Context ctx;
    private final DatabaseProxy db;
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private final Object lock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MongoShell(MongoService service, String defaultDbName) {
        this.service = service;
        this.ctx = Context.newBuilder("js")
                .allowHostAccess(HostAccess.ALL)
                .allowHostClassLookup(name -> false) // no arbitrary Java class access from JS
                .out(new PrintStream(out, true))
                .err(new PrintStream(err, true))
                .option("engine.WarnInterpreterOnly", "false")
                .build();
        MongoClient client = service.client();
        this.db = new DatabaseProxy(client, defaultDbName == null ? "test" : defaultDbName);
        installBindings();
    }

    private void installBindings() {
        Value bindings = ctx.getBindings("js");
        bindings.putMember("db", db);

        // BSON literals
        bindings.putMember("ObjectId",      BsonHostHelpers.objectId());
        bindings.putMember("ISODate",       BsonHostHelpers.isoDate());
        bindings.putMember("Date",          BsonHostHelpers.isoDate()); // mongosh alias for the BSON Date
        bindings.putMember("NumberLong",    BsonHostHelpers.numberLong());
        bindings.putMember("NumberInt",     BsonHostHelpers.numberInt());
        bindings.putMember("NumberDecimal", BsonHostHelpers.numberDecimal());
        bindings.putMember("UUID",          BsonHostHelpers.uuid());
        bindings.putMember("BinData",       BsonHostHelpers.binData());
        bindings.putMember("Timestamp",     BsonHostHelpers.timestamp());
        bindings.putMember("MinKey",        BsonHostHelpers.minKey());
        bindings.putMember("MaxKey",        BsonHostHelpers.maxKey());
        bindings.putMember("RegExp",        BsonHostHelpers.regExp());

        // Helpers
        bindings.putMember("use", (ProxyExecutable) a -> {
            String name = a[0].asString();
            db.switchTo(name);
            return "switched to db " + name;
        });
        bindings.putMember("printjson", (ProxyExecutable) a -> {
            for (Value v : a) {
                out.writeBytes(JsBsonConverter.prettyPrint(JsBsonConverter.toJava(v)).getBytes());
                out.write('\n');
            }
            return null;
        });
        bindings.putMember("print", (ProxyExecutable) a -> {
            for (int i = 0; i < a.length; i++) {
                if (i > 0) out.write(' ');
                out.writeBytes(String.valueOf(JsBsonConverter.toJava(a[i])).getBytes());
            }
            out.write('\n');
            return null;
        });
    }

    /** Result of one {@link #eval} call — last-statement value rendered
     *  the same way mongosh prints it, plus anything the script
     *  produced via {@code print} / {@code printjson}. */
    public record Result(String stdout, String value, String stderr, boolean error) {}

    public Result eval(String script) {
        if (closed.get()) {
            return new Result("", "", "shell is closed", true);
        }
        synchronized (lock) {
            out.reset();
            err.reset();
            try {
                Value v = ctx.eval("js", script);
                String rendered = renderValue(v);
                return new Result(out.toString(), rendered, err.toString(), false);
            } catch (PolyglotException pe) {
                String msg = pe.isHostException() && pe.asHostException() != null
                        ? pe.asHostException().getClass().getSimpleName()
                            + ": " + pe.asHostException().getMessage()
                        : pe.getMessage();
                if (pe.isInterrupted() || pe.isCancelled()) {
                    msg = "execution cancelled";
                }
                return new Result(out.toString(), "", msg, true);
            } catch (Exception e) {
                return new Result(out.toString(), "", e.getClass().getSimpleName() + ": " + e.getMessage(), true);
            }
        }
    }

    /** Signal the in-flight {@link #eval} to abort. Safe to call from
     *  any thread. */
    public void cancel() {
        try {
            ctx.interrupt(java.time.Duration.ofMillis(500));
        } catch (Exception ignored) {}
    }

    private static String renderValue(Value v) {
        if (v == null || v.isNull()) return "null";
        // Suppress noisy "host proxy" output for cursors / collections —
        // mongosh shows nothing for "db.users" alone, so we mirror that.
        if (v.isProxyObject()) return "";
        Object java = JsBsonConverter.toJava(v);
        if (java == null) return "null";
        if (java instanceof CharSequence s) return "\"" + s + "\"";
        if (java instanceof Number || java instanceof Boolean) return java.toString();
        return JsBsonConverter.prettyPrint(java);
    }

    public String currentDbName() { return db.currentDbName(); }

    public MongoService service() { return service; }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try { ctx.close(true); } catch (Exception ignored) {}
    }
}
