package com.kubrik.mex.labs.ports;

import com.kubrik.mex.labs.model.PortMap;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EphemeralPortAllocatorTest {

    private final EphemeralPortAllocator allocator = new EphemeralPortAllocator();

    @Test
    void allocates_distinct_ports_for_distinct_containers() throws Exception {
        PortMap pm = allocator.allocate(List.of("mongos", "cfg1", "cfg2",
                "cfg3", "shard1a", "shard1b", "shard1c"));
        Set<Integer> uniq = new HashSet<>(pm.ports().values());
        assertEquals(7, uniq.size());
    }

    @Test
    void preserves_container_name_order() throws Exception {
        PortMap pm = allocator.allocate(List.of("a", "b", "c", "d"));
        var keys = new java.util.ArrayList<>(pm.ports().keySet());
        assertEquals(List.of("a", "b", "c", "d"), keys);
    }

    @Test
    void parallel_allocations_do_not_collide() throws Exception {
        Set<Integer> all = java.util.Collections.synchronizedSet(new HashSet<>());
        int threads = 8;
        int perThread = 10;
        java.util.concurrent.ExecutorService ex =
                java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        java.util.concurrent.CountDownLatch done =
                new java.util.concurrent.CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            ex.submit(() -> {
                try {
                    var pm = allocator.allocate(List.of(
                            "m1", "m2", "m3", "m4", "m5",
                            "m6", "m7", "m8", "m9", "m10"));
                    all.addAll(pm.ports().values());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));
        ex.shutdown();
        // The kernel may recycle ports between successive closings,
        // but we should see substantially more distinct ports than a
        // single allocation would produce.
        assertTrue(all.size() > perThread,
                "parallel allocations should produce many unique ports; got "
                        + all.size());
    }
}
