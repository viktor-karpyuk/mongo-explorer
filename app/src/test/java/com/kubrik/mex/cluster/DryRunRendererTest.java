package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 SAFE-OPS-1..4 — dry-run renderer produces byte-stable canonical JSON
 * and a stable SHA-256 per command.
 */
class DryRunRendererTest {

    @Test
    void stepDownHasCanonicalShape() {
        DryRunResult r = DryRunRenderer.render(new Command.StepDown("prod-rs-01:27018", 60, 10));
        assertEquals("replSetStepDown", r.commandName());
        assertEquals("{\"replSetStepDown\":60,\"secondaryCatchUpPeriodSecs\":10}", r.commandJson());
        assertEquals(64, r.previewHash().length());
        assertTrue(r.predictedEffect().contains("prod-rs-01:27018"));
    }

    @Test
    void freezeZeroSecsRendersUnfreezeCopy() {
        DryRunResult r = DryRunRenderer.render(new Command.Freeze("prod-rs-03:27018", 0));
        assertEquals("{\"replSetFreeze\":0}", r.commandJson());
        assertTrue(r.summary().toLowerCase().contains("unfreeze"));
    }

    @Test
    void killOpEmitsOpidField() {
        DryRunResult r = DryRunRenderer.render(new Command.KillOp("prod-rs-01:27018", 4917L));
        assertEquals("{\"killOp\":1,\"op\":4917}", r.commandJson());
    }

    @Test
    void moveChunkIncludesWriteConcern() {
        DryRunResult r = DryRunRenderer.render(new Command.MoveChunk(
                "orders.orders",
                Map.of("region", "us", "_id", 3100),
                Map.of("region", "us", "_id", 3250),
                "shard1", true, "majority"));
        assertTrue(r.commandJson().contains("\"to\":\"shard1\""));
        assertTrue(r.commandJson().contains("\"writeConcern\":{\"w\":\"majority\"}"));
        assertTrue(r.commandJson().contains("\"_waitForDelete\":true"));
    }

    @Test
    void balancerStartStopDifferentHashes() {
        DryRunResult start = DryRunRenderer.render(new Command.BalancerStart("prod-east"));
        DryRunResult stop  = DryRunRenderer.render(new Command.BalancerStop("prod-east"));
        assertNotEquals(start.previewHash(), stop.previewHash());
    }

    @Test
    void balancerWindowRendersHhmmStrings() {
        DryRunResult r = DryRunRenderer.render(
                new Command.BalancerWindow("prod-east", "00:00", "06:00"));
        assertTrue(r.commandJson().contains("\"start\":\"00:00\""));
        assertTrue(r.commandJson().contains("\"stop\":\"06:00\""));
    }

    @Test
    void hashDeterministicAcrossRuns() {
        Command cmd = new Command.StepDown("a:27018", 60, 10);
        DryRunResult a = DryRunRenderer.render(cmd);
        DryRunResult b = DryRunRenderer.render(cmd);
        assertEquals(a.previewHash(), b.previewHash());
        assertEquals(a.commandJson(), b.commandJson());
    }

    @Test
    void stepDownRejectsBadArgs() {
        assertThrows(IllegalArgumentException.class, () -> new Command.StepDown("", 60, 10));
        assertThrows(IllegalArgumentException.class, () -> new Command.StepDown("a:1", 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new Command.StepDown("a:1", 60, 100));
    }

    @Test
    void balancerWindowRejectsNonHhmm() {
        assertThrows(IllegalArgumentException.class,
                () -> new Command.BalancerWindow("c", "24:00", "06:00"));
        assertThrows(IllegalArgumentException.class,
                () -> new Command.BalancerWindow("c", "00:60", "06:00"));
    }

    @Test
    void requiredRolesCoverEveryCommand() {
        Command[] kinds = {
                new Command.StepDown("a:1", 60, 10),
                new Command.Freeze("a:1", 60),
                new Command.KillOp("a:1", 1L),
                new Command.MoveChunk("db.coll", Map.of("k", 1), Map.of("k", 2), "shard1", true, "majority"),
                new Command.BalancerStart("c"),
                new Command.BalancerStop("c"),
                new Command.BalancerWindow("c", "00:00", "06:00"),
                new Command.AddTagRange("db.coll", Map.of("k", 1), Map.of("k", 2), "zone"),
                new Command.RemoveTagRange("db.coll", Map.of("k", 1), Map.of("k", 2))
        };
        for (Command c : kinds) {
            assertFalse(c.requiredRoles().isEmpty(), "required roles missing for " + c.name());
        }
        assertTrue(new Command.KillSwitchToggle(true).requiredRoles().isEmpty(),
                "kill-switch toggle is roleless");
    }
}
