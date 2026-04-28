package com.kubrik.mex.cluster;

import com.kubrik.mex.cluster.safety.KillSwitch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class KillSwitchTest {

    @Test
    void defaultsOff() {
        KillSwitch k = new KillSwitch();
        assertFalse(k.isEngaged());
        assertDoesNotThrow(k::requireDisengaged);
    }

    @Test
    void engagePublishesThenRequireFails() {
        KillSwitch k = new KillSwitch();
        AtomicBoolean seen = new AtomicBoolean(false);
        k.onChange(state -> { if (state) seen.set(true); });
        k.engage();
        assertTrue(k.isEngaged());
        assertTrue(seen.get(), "listener should have fired with engaged=true");
        assertThrows(KillSwitch.KillSwitchEngagedException.class, k::requireDisengaged);
    }

    @Test
    void engageIdempotent() {
        KillSwitch k = new KillSwitch();
        AtomicInteger engagedFires = new AtomicInteger(0);
        k.onChange(state -> { if (state) engagedFires.incrementAndGet(); });
        // initial fire counts as 1 even though we haven't engaged — reset after subscribe
        engagedFires.set(0);
        k.engage();
        k.engage();
        k.engage();
        assertEquals(1, engagedFires.get(), "engage must not re-fire listeners when already engaged");
    }

    @Test
    void disengageAllowsDispatch() {
        KillSwitch k = new KillSwitch();
        k.engage();
        k.disengage();
        assertFalse(k.isEngaged());
        assertDoesNotThrow(k::requireDisengaged);
    }

    @Test
    void listenerCloseDetaches() throws Exception {
        KillSwitch k = new KillSwitch();
        AtomicInteger fires = new AtomicInteger(0);
        AutoCloseable sub = k.onChange(state -> fires.incrementAndGet());
        int baseline = fires.get();
        k.engage();
        sub.close();
        k.disengage();
        assertEquals(baseline + 1, fires.get(), "no further events after close()");
    }
}
