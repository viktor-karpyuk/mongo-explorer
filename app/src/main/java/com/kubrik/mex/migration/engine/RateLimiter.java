package com.kubrik.mex.migration.engine;

/** Thin wrapper around Guava's {@code RateLimiter} so the engine never imports Guava directly.
 *  Zero = unlimited. Used by the Reader to enforce the docs-per-second cap (PERF-5). */
public final class RateLimiter {

    private final com.google.common.util.concurrent.RateLimiter delegate;

    private RateLimiter(com.google.common.util.concurrent.RateLimiter delegate) {
        this.delegate = delegate;
    }

    public static RateLimiter unlimited() { return new RateLimiter(null); }

    public static RateLimiter perSecond(long permitsPerSecond) {
        if (permitsPerSecond <= 0) return unlimited();
        return new RateLimiter(com.google.common.util.concurrent.RateLimiter.create(permitsPerSecond));
    }

    public void acquire(int permits) {
        if (delegate != null) delegate.acquire(permits);
    }
}
