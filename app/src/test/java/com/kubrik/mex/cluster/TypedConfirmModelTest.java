package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class TypedConfirmModelTest {

    private static TypedConfirmModel freshModel(String expected) {
        DryRunResult preview = DryRunRenderer.render(new Command.StepDown(expected, 60, 10));
        return new TypedConfirmModel(expected, preview);
    }

    @Test
    void matchRequiresTrimEquality() {
        TypedConfirmModel m = freshModel("prod-rs-01:27018");
        m.setInput("prod-rs-01:27018");
        assertTrue(m.matches());
        m.setInput(" prod-rs-01:27018 ");
        assertTrue(m.matches(), "trailing whitespace trimmed on both sides");
        m.setInput("prod-rs-02:27018");
        assertFalse(m.matches());
    }

    @Test
    void confirmOnlyWithMatch() {
        TypedConfirmModel m = freshModel("prod-rs-01:27018");
        m.setInput("wrong");
        m.confirm();
        assertNull(m.outcome(), "confirm() must not record CONFIRMED without a trim-match");
        m.setInput("prod-rs-01:27018");
        m.confirm();
        assertEquals(TypedConfirmModel.Outcome.CONFIRMED, m.outcome());
    }

    @Test
    void cancelAlwaysRecordsCancelled() {
        TypedConfirmModel m = freshModel("prod-rs-01:27018");
        m.setInput("prod-rs-01:27018");
        m.cancel();
        assertEquals(TypedConfirmModel.Outcome.CANCELLED, m.outcome());
    }

    @Test
    void pasteFlagSticky() {
        TypedConfirmModel m = freshModel("4917");
        assertFalse(m.paste());
        m.markPaste();
        m.setInput("4917");
        assertTrue(m.paste(), "paste flag persists through subsequent setInput");
    }

    @Test
    void matchListenerFires() {
        TypedConfirmModel m = freshModel("prod-east");
        AtomicBoolean last = new AtomicBoolean(false);
        m.onMatchChanged(last::set);
        m.setInput("prod-east");
        assertTrue(last.get());
        m.setInput("no");
        assertFalse(last.get());
    }
}
