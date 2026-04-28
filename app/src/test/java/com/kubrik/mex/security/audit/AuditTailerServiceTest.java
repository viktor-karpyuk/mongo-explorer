package com.kubrik.mex.security.audit;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-C follow-up — path-resolution pinning. The full service
 * wires to EventBus + ConnectionManager; those live-dependency paths
 * are exercised by the app smoke-test. Here we cover
 * {@link AuditTailerService#resolveAuditPath}, which is pure-logic
 * and covers every classification branch the probe cares about.
 */
class AuditTailerServiceTest {

    @TempDir Path tmp;

    @Test
    void returns_path_when_auditLog_destination_is_file_and_path_is_readable() throws Exception {
        Path auditFile = tmp.resolve("audit.json");
        Files.writeString(auditFile, "");
        Document reply = replyWith(new Document()
                .append("destination", "file")
                .append("path", auditFile.toString()));

        Path resolved = AuditTailerService.resolveAuditPath(reply);

        assertNotNull(resolved);
        assertEquals(auditFile, resolved);
    }

    @Test
    void returns_null_when_destination_is_console() throws Exception {
        Path auditFile = tmp.resolve("audit.json");
        Files.writeString(auditFile, "");
        Document reply = replyWith(new Document()
                .append("destination", "console")
                .append("path", auditFile.toString()));

        assertNull(AuditTailerService.resolveAuditPath(reply));
    }

    @Test
    void returns_null_when_destination_is_syslog() {
        Document reply = replyWith(new Document()
                .append("destination", "syslog"));

        assertNull(AuditTailerService.resolveAuditPath(reply));
    }

    @Test
    void returns_null_when_path_is_unreadable_on_this_host() {
        // Containerised / remote case — the server reports a path
        // inside /data that doesn't exist on the local FS.
        Document reply = replyWith(new Document()
                .append("destination", "file")
                .append("path", "/does/not/exist/on/this/host/audit.json"));

        assertNull(AuditTailerService.resolveAuditPath(reply));
    }

    @Test
    void returns_null_when_auditLog_section_is_absent() {
        Document reply = new Document("parsed", new Document());

        assertNull(AuditTailerService.resolveAuditPath(reply));
    }

    @Test
    void returns_null_when_parsed_section_is_absent() {
        Document reply = new Document();

        assertNull(AuditTailerService.resolveAuditPath(reply));
    }

    @Test
    void returns_null_on_null_reply() {
        assertNull(AuditTailerService.resolveAuditPath(null));
    }

    @Test
    void destination_defaults_to_file_when_omitted() throws Exception {
        // Some MongoDB config variants omit the destination field when
        // it's set to the default 'file'. The probe accepts that.
        Path auditFile = tmp.resolve("audit.json");
        Files.writeString(auditFile, "");
        Document reply = replyWith(new Document("path", auditFile.toString()));

        Path resolved = AuditTailerService.resolveAuditPath(reply);

        assertEquals(auditFile, resolved);
    }

    @Test
    void returns_null_when_path_is_blank() {
        Document reply = replyWith(new Document()
                .append("destination", "file")
                .append("path", "  "));

        assertNull(AuditTailerService.resolveAuditPath(reply));
    }

    /* ============================== helpers ============================== */

    private static Document replyWith(Document auditLog) {
        return new Document("parsed", new Document("auditLog", auditLog));
    }
}
