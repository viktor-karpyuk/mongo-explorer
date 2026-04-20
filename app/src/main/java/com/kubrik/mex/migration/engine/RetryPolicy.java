package com.kubrik.mex.migration.engine;

import com.kubrik.mex.migration.spec.ConflictMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/** Simple exponential-backoff retry with jitter, classified by {@link ErrorClassifier}.
 *  Resilience4j was listed as a dependency but the logic we need is tiny enough that rolling
 *  our own avoids a runtime surprise and keeps the retry decision co-located with the
 *  classifier. */
public final class RetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(RetryPolicy.class);

    private static final long BASE_MS = 500L;
    private static final long CAP_MS  = 30_000L;

    private final int maxAttempts;
    private final ConflictMode mode;

    public RetryPolicy(int maxAttempts, ConflictMode mode) {
        this.maxAttempts = Math.max(1, maxAttempts);
        this.mode = mode;
    }

    public <T> T execute(String key, Callable<T> op) throws Exception {
        int attempt = 0;
        long sleepMs = BASE_MS;
        while (true) {
            attempt++;
            try {
                return op.call();
            } catch (Throwable t) {
                ErrorClassifier.ErrorKind kind = ErrorClassifier.classify(t, mode);
                if (kind != ErrorClassifier.ErrorKind.TRANSIENT || attempt >= maxAttempts) {
                    if (t instanceof Exception e) throw e;
                    throw new RuntimeException(t);
                }
                log.warn("retry key={} attempt={} cause={}", key, attempt, t.getClass().getSimpleName());
                long jitter = ThreadLocalRandom.current().nextLong(sleepMs / 5 + 1); // 20 % jitter
                Thread.sleep(sleepMs + jitter);
                sleepMs = Math.min(sleepMs * 2, CAP_MS);
            }
        }
    }

    public void executeVoid(String key, RunnableWithEx op) throws Exception {
        execute(key, () -> { op.run(); return null; });
    }

    @FunctionalInterface
    public interface RunnableWithEx { void run() throws Exception; }
}
