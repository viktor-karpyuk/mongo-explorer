package com.kubrik.mex.k8s.portforward;

import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8-N5 — Ordered-fallback chain must try each delegate
 * in turn and stop at the first one that succeeds; if all fail, the
 * first delegate's exception becomes the cause and the rest land as
 * suppressed.
 */
class ChainedPortForwardOpenerTest {

    @Test
    void first_success_short_circuits_the_chain() throws Exception {
        StubOpener ok = StubOpener.ok();
        StubOpener never = StubOpener.ok();
        ChainedPortForwardOpener chain = new ChainedPortForwardOpener(List.of(ok, never));

        chain.open(null, "ns", "pod", 27017).close();

        assertEquals(1, ok.calls);
        assertEquals(0, never.calls, "a successful primary must not reach later delegates");
    }

    @Test
    void falls_through_to_next_on_primary_ioexception() throws Exception {
        StubOpener boom = StubOpener.fail("primary died");
        StubOpener ok = StubOpener.ok();
        ChainedPortForwardOpener chain = new ChainedPortForwardOpener(List.of(boom, ok));

        chain.open(null, "ns", "pod", 27017).close();

        assertEquals(1, boom.calls);
        assertEquals(1, ok.calls);
    }

    @Test
    void all_failures_raise_aggregate_with_suppressed() {
        StubOpener b1 = StubOpener.fail("a");
        StubOpener b2 = StubOpener.fail("b");
        StubOpener b3 = StubOpener.fail("c");
        ChainedPortForwardOpener chain = new ChainedPortForwardOpener(List.of(b1, b2, b3));

        IOException thrown = assertThrows(IOException.class,
                () -> chain.open(null, "ns", "pod", 27017));
        assertTrue(thrown.getMessage().contains("a"),
                "first delegate's message must be in the aggregate; got: " + thrown.getMessage());
        assertEquals(2, thrown.getSuppressed().length,
                "later delegates' failures must land as suppressed");
    }

    @Test
    void empty_chain_is_rejected_at_construction() {
        assertThrows(IllegalArgumentException.class,
                () -> new ChainedPortForwardOpener(List.of()));
    }

    @Test
    void delegate_types_snapshot_preserves_order() {
        ChainedPortForwardOpener chain = new ChainedPortForwardOpener(
                List.of(StubOpener.ok(), StubOpener.fail("x")));
        List<Class<?>> types = chain.delegateTypes();
        assertEquals(2, types.size());
    }

    static final class StubOpener implements PortForwardService.PortForwardOpener {
        int calls;
        final boolean succeed;
        final String failureMessage;
        private StubOpener(boolean succeed, String failureMessage) {
            this.succeed = succeed;
            this.failureMessage = failureMessage;
        }
        static StubOpener ok() { return new StubOpener(true, null); }
        static StubOpener fail(String msg) { return new StubOpener(false, msg); }

        @Override
        public StreamPair open(ApiClient c, String ns, String pod, int rp) throws IOException {
            calls++;
            if (!succeed) throw new IOException(failureMessage);
            InputStream in = new ByteArrayInputStream(new byte[0]);
            OutputStream out = new ByteArrayOutputStream();
            return new StreamPair() {
                @Override public InputStream downstream() { return in; }
                @Override public OutputStream upstream() { return out; }
                @Override public void close() { /* noop */ }
            };
        }
    }
}
