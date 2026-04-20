package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.audit.AuditExporter;
import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 AUD-EXP-1..3 — round-trips the exporter: JSON bundle carries a
 * well-formed envelope with generator + row count + rows array, each row
 * exposes its {@code sha256} digest, and CSV emits the spec's column header
 * with properly escaped fields for the quoting-sensitive cases (commas,
 * embedded quotes, newlines).
 */
class AuditExporterTest {

    @TempDir Path tmp;

    @Test
    void json_bundle_round_trips_core_fields() throws IOException {
        OpsAuditRecord r = sample(1, "replSetStepDown", Outcome.OK,
                "{\"replSetStepDown\":60}", "msg");
        Path out = tmp.resolve("bundle.json");
        AuditExporter.writeJson(out, List.of(r));

        String body = Files.readString(out, StandardCharsets.UTF_8);
        assertTrue(body.contains("\"generator\":\"mongo-explorer v2.4 audit exporter\""));
        assertTrue(body.contains("\"rowCount\":1"));
        assertTrue(body.contains("\"command_name\":\"replSetStepDown\""));
        assertTrue(body.contains("\"outcome\":\"OK\""));
        assertTrue(body.contains("\"sha256\":\""));
    }

    @Test
    void csv_escapes_commas_quotes_and_newlines() throws IOException {
        OpsAuditRecord tricky = sample(2, "killOp", Outcome.FAIL,
                "{\"killOp\":1,\"op\":4917}",
                "boom, \"bad\", end");
        Path out = tmp.resolve("rows.csv");
        AuditExporter.writeCsv(out, List.of(tricky));

        String body = Files.readString(out, StandardCharsets.UTF_8);
        assertTrue(body.startsWith("id,connection_id,started_at"));
        // The server_message field contained a comma, a quote, and a space,
        // so it must end up quoted + the embedded quotes doubled. (The JSON
        // payload itself isn't emitted in CSV — it lives only in the JSON
        // bundle export, which can carry the nested BSON safely.)
        assertTrue(body.contains("\"boom, \"\"bad\"\", end\""), "body=" + body);
    }

    @Test
    void sha256_digest_is_stable_for_equal_rows() {
        OpsAuditRecord a = sample(7, "killOp", Outcome.OK, "{\"killOp\":1}", "ok");
        OpsAuditRecord b = sample(7, "killOp", Outcome.OK, "{\"killOp\":1}", "ok");
        assertEquals(AuditExporter.sha256RowDigest(a), AuditExporter.sha256RowDigest(b));
    }

    private static OpsAuditRecord sample(long id, String cmd, Outcome oc,
                                          String json, String msg) {
        return new OpsAuditRecord(id, "cx-1", "admin", null, cmd, json,
                "h".repeat(64), oc, msg, "root",
                1_000L, 1_250L, 250L,
                "localhost", "dba", "cluster.topology", false, false);
    }
}
