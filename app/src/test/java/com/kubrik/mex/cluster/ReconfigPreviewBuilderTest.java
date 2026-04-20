package com.kubrik.mex.cluster;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.4 RS-8 — verifies the preview-JSON builder inside
 * {@code ReconfigPreviewDialog}: the targeted member is mutated in place,
 * other members are unchanged, and the config {@code version} increments by
 * one (mirroring what {@code rs.reconfig} would compute server-side).
 *
 * <p>Reflection is used to reach the package-private static builder without
 * spinning a JavaFX runtime.</p>
 */
class ReconfigPreviewBuilderTest {

    @SuppressWarnings("unchecked")
    @Test
    void mutates_targeted_member_and_bumps_version() throws Exception {
        Document current = new Document("_id", "prod-rs")
                .append("version", 7)
                .append("members", List.of(
                        new Document("_id", 0).append("host", "h1:27017").append("priority", 1),
                        new Document("_id", 1).append("host", "h2:27017").append("priority", 1),
                        new Document("_id", 2).append("host", "h3:27017").append("priority", 1)));

        Method m = Class.forName("com.kubrik.mex.ui.cluster.ReconfigPreviewDialog")
                .getDeclaredMethod("buildReconfigPreview", Document.class, String.class,
                        int.class, int.class, boolean.class);
        m.setAccessible(true);
        String json = (String) m.invoke(null, current, "h2:27017", 5, 1, true);
        assertTrue(json.contains("\"replSetReconfig\""));
        assertTrue(json.contains("\"version\": 8"), "version should increment");
        assertTrue(json.contains("\"priority\": 5"));
        assertTrue(json.contains("\"hidden\": true"));
        // Sanity: the other two members remain priority 1.
        long priOnes = json.lines().filter(l -> l.trim().equals("\"priority\": 1,")
                || l.trim().equals("\"priority\": 1")).count();
        assertEquals(2, priOnes, "only the targeted member changes");
    }
}
