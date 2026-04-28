package com.kubrik.mex.maint.index;

import com.kubrik.mex.maint.model.IndexBuildSpec;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class IndexBuildSpecTest {

    @Test
    void minimal_spec_emits_only_key_and_name() {
        IndexBuildSpec spec = new IndexBuildSpec("app", "users",
                new Document("userId", 1), "userId_1",
                false, false,
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        Document d = spec.toIndexSpecDocument();
        assertEquals(new Document("userId", 1), d.get("key"));
        assertEquals("userId_1", d.getString("name"));
        assertFalse(d.containsKey("unique"));
        assertFalse(d.containsKey("sparse"));
        assertFalse(d.containsKey("expireAfterSeconds"));
    }

    @Test
    void full_spec_includes_every_option() {
        IndexBuildSpec spec = new IndexBuildSpec("app", "users",
                new Document("email", 1), "email_unique",
                true, true,
                Optional.of(3600L),
                Optional.of(new Document("active", true)),
                Optional.of(new Document("locale", "en_US")),
                Optional.empty(),
                Optional.of("wiredTiger"));
        Document d = spec.toIndexSpecDocument();
        assertTrue(d.getBoolean("unique"));
        assertTrue(d.getBoolean("sparse"));
        assertEquals(3600L, d.getLong("expireAfterSeconds"));
        assertEquals(new Document("active", true),
                d.get("partialFilterExpression"));
        assertEquals(new Document("locale", "en_US"), d.get("collation"));
        // storageEngine wraps the engine name in its own sub-document
        // per the createIndexes wire contract.
        assertEquals(new Document("wiredTiger", new Document()),
                d.get("storageEngine"));
    }

    @Test
    void blank_name_rejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new IndexBuildSpec("app", "users",
                        new Document("a", 1), "",
                        false, false,
                        Optional.empty(), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty()));
    }
}
