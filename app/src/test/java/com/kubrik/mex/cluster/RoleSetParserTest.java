package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.RoleSet;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RoleSetParserTest {

    @Test
    void emptyReplyYieldsEmpty() {
        assertSame(RoleSet.EMPTY, RoleSet.parse(null));
        assertEquals(RoleSet.EMPTY.roles(), RoleSet.parse(new Document()).roles());
    }

    @Test
    void readUserOnlyAllowsNothingDestructive() {
        RoleSet roles = RoleSet.parse(reply("read", "prod"));
        assertTrue(roles.hasRole("read"));
        assertFalse(roles.allows(new Command.StepDown("a:1", 60, 10)));
        assertFalse(roles.allows(new Command.KillOp("a:1", 1L)));
    }

    @Test
    void clusterManagerAllowsStepDown() {
        RoleSet roles = RoleSet.parse(reply("clusterManager", "admin"));
        assertTrue(roles.allows(new Command.StepDown("a:1", 60, 10)));
        assertTrue(roles.allows(new Command.BalancerStart("c")));
        assertFalse(roles.allows(new Command.KillOp("a:1", 1L)),
                "killOp requires killAnyCursor, not clusterManager");
    }

    @Test
    void rootAllowsEverythingDestructive() {
        RoleSet roles = RoleSet.parse(reply("root", "admin"));
        for (Command c : List.of(
                new Command.StepDown("a:1", 60, 10),
                new Command.Freeze("a:1", 60),
                new Command.KillOp("a:1", 1L),
                new Command.MoveChunk("db.coll", Map.of("k", 1), Map.of("k", 2), "shard1", true, "majority"),
                new Command.BalancerStart("c"),
                new Command.BalancerStop("c"),
                new Command.BalancerWindow("c", "00:00", "06:00"),
                new Command.AddTagRange("db.coll", Map.of("k", 1), Map.of("k", 2), "z"),
                new Command.RemoveTagRange("db.coll", Map.of("k", 1), Map.of("k", 2)))) {
            assertTrue(roles.allows(c), "root should allow " + c.name());
        }
    }

    @Test
    void multipleRolesUnion() {
        Document reply = new Document("authInfo", new Document(
                "authenticatedUserRoles", List.of(
                        new Document("role", "read").append("db", "app"),
                        new Document("role", "clusterManager").append("db", "admin"))));
        RoleSet roles = RoleSet.parse(reply);
        assertTrue(roles.hasRole("read"));
        assertTrue(roles.hasRole("clusterManager"));
    }

    @Test
    void unknownShapeYieldsEmpty() {
        RoleSet roles = RoleSet.parse(new Document("authInfo", "not-a-document"));
        assertTrue(roles.roles().isEmpty());
    }

    private static Document reply(String role, String db) {
        return new Document("authInfo", new Document(
                "authenticatedUserRoles", List.of(
                        new Document("role", role).append("db", db))));
    }
}
