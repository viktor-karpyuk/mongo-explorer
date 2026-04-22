package com.kubrik.mex.maint.reconfig;

import com.kubrik.mex.maint.model.ReconfigSpec;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ReconfigSerializerTest {

    private final ReconfigSerializer ser = new ReconfigSerializer();

    @Test
    void writes_minimal_body_with_defaults_omitted() {
        ReconfigSpec.Member m0 = new ReconfigSpec.Member(0, "h1:27017", 1, 1,
                false, false, true, 0.0);
        ReconfigSpec.Request req = new ReconfigSpec.Request("rs0", 3,
                List.of(m0), new ReconfigSpec.ChangePriority(0, 2));
        Document body = ser.toReconfigBody(req);
        assertEquals("rs0", body.getString("_id"));
        assertEquals(4, body.getInteger("version"));
        List<?> members = body.getList("members", Document.class);
        assertEquals(1, members.size());
        Document member = (Document) members.get(0);
        assertEquals(2, member.getInteger("priority"));
        // Defaults omitted keep the audit JSON clean.
        assertFalse(member.containsKey("hidden"));
        assertFalse(member.containsKey("arbiterOnly"));
        assertFalse(member.containsKey("buildIndexes"));
    }

    @Test
    void round_trips_config_reply() {
        Document reply = new Document("config", new Document()
                .append("_id", "rs0")
                .append("version", 9)
                .append("members", List.of(
                        new Document()
                                .append("_id", 0)
                                .append("host", "h1:27017")
                                .append("priority", 1)
                                .append("votes", 1),
                        new Document()
                                .append("_id", 1)
                                .append("host", "h2:27017")
                                .append("priority", 0)
                                .append("votes", 0)
                                .append("hidden", true))));
        Optional<ReconfigSpec.Request> parsed = ser.fromConfigReply(reply,
                new ReconfigSpec.RemoveMember(1));
        assertTrue(parsed.isPresent());
        assertEquals("rs0", parsed.get().replicaSetName());
        assertEquals(9, parsed.get().currentConfigVersion());
        assertEquals(2, parsed.get().currentMembers().size());
        assertTrue(parsed.get().currentMembers().get(1).hidden());
    }

    @Test
    void fromConfigReply_handles_null_gracefully() {
        assertTrue(ser.fromConfigReply(null,
                new ReconfigSpec.RemoveMember(0)).isEmpty());
        assertTrue(ser.fromConfigReply(new Document(),
                new ReconfigSpec.RemoveMember(0)).isEmpty());
    }

    @Test
    void bumpedVersion_is_plus_one() {
        assertEquals(8, ser.bumpedVersion(7));
    }
}
