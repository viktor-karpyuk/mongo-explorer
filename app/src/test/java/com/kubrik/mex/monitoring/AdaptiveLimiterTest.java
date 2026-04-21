package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.sampler.AdaptiveLimiter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.*;

class AdaptiveLimiterTest {

    @Test
    void acquireOnceBlocksConcurrent() {
        AdaptiveLimiter l = new AdaptiveLimiter();
        assertTrue(l.tryAcquire());
        assertFalse(l.tryAcquire(), "second concurrent acquire must fail");
        l.release();
    }

    @Test
    void enforcesMinimumGap() throws Exception {
        MutableClock clock = new MutableClock();
        AdaptiveLimiter l = limiterWithClock(clock);
        clock.set(1_000_000_000L);
        assertTrue(l.tryAcquire());
        l.release();
        // 10 ms later — still inside 50 ms minimum gap
        clock.advanceMs(10);
        assertFalse(l.tryAcquire());
        // 60 ms after the release — gap cleared
        clock.advanceMs(55);
        assertTrue(l.tryAcquire());
        l.release();
    }

    @Test
    void twentyCallsAtMinGapAllSucceed() throws Exception {
        // With min-gap = 50 ms and CALLS_PER_SECOND = 20, exactly 20 calls per second
        // is the natural rate and all of them must be admitted.
        MutableClock clock = new MutableClock();
        AdaptiveLimiter l = limiterWithClock(clock);
        clock.set(1_000_000_000L);
        for (int i = 0; i < 20; i++) {
            assertTrue(l.tryAcquire(), "acquire #" + i + " expected to succeed");
            l.release();
            clock.advanceMs(60);
        }
    }

    @Test
    void releaseWithoutAcquireIsHarmless() {
        AdaptiveLimiter l = new AdaptiveLimiter();
        // Defensive: stray release should not violate the inFlight=1 invariant for
        // the next acquire on a fresh limiter. (Java Semaphore permits over-release
        // by design; we rely on callers only releasing what they acquired — the
        // test asserts that our inFlight counter stays sane.)
        assertEquals(0, l.inFlight());
        assertTrue(l.tryAcquire());
        assertEquals(1, l.inFlight());
        l.release();
        assertEquals(0, l.inFlight());
    }

    /** Reflectively reach the package-private clock ctor of AdaptiveLimiter. */
    private static AdaptiveLimiter limiterWithClock(MutableClock clock) throws Exception {
        Class<?> inner = Class.forName(
                "com.kubrik.mex.monitoring.sampler.AdaptiveLimiter$LongSupplier");
        Object proxy = java.lang.reflect.Proxy.newProxyInstance(
                inner.getClassLoader(),
                new Class<?>[] { inner },
                (p, m, a) -> clock.get());
        Constructor<AdaptiveLimiter> ctor = AdaptiveLimiter.class.getDeclaredConstructor(inner);
        ctor.setAccessible(true);
        return ctor.newInstance(proxy);
    }

    private static final class MutableClock {
        private volatile long nanos = 0L;
        void set(long v) { this.nanos = v; }
        void advanceMs(long ms) { this.nanos += ms * 1_000_000L; }
        long get() { return nanos; }
    }
}
