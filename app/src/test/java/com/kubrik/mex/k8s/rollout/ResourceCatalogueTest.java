package com.kubrik.mex.k8s.rollout;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResourceCatalogueTest {

    @Test
    void records_in_insertion_order() {
        ResourceCatalogue c = new ResourceCatalogue();
        c.record(new ResourceCatalogue.Ref("v1", "Secret", "mongo", "users"));
        c.record(new ResourceCatalogue.Ref(
                "mongodbcommunity.mongodb.com/v1", "MongoDBCommunity", "mongo", "rs"));
        List<ResourceCatalogue.Ref> snap = c.snapshot();
        assertEquals("Secret", snap.get(0).kind());
        assertEquals("MongoDBCommunity", snap.get(1).kind());
    }

    @Test
    void reversed_flips_order_for_cleanup() {
        ResourceCatalogue c = new ResourceCatalogue();
        c.record(new ResourceCatalogue.Ref("v1", "Secret", "mongo", "users"));
        c.record(new ResourceCatalogue.Ref(
                "mongodbcommunity.mongodb.com/v1", "MongoDBCommunity", "mongo", "rs"));
        List<ResourceCatalogue.Ref> rev = c.reversed();
        assertEquals("MongoDBCommunity", rev.get(0).kind());
        assertEquals("Secret", rev.get(1).kind());
    }

    @Test
    void snapshot_is_independent_of_future_inserts() {
        ResourceCatalogue c = new ResourceCatalogue();
        c.record(new ResourceCatalogue.Ref("v1", "Secret", "ns", "a"));
        List<ResourceCatalogue.Ref> before = c.snapshot();
        c.record(new ResourceCatalogue.Ref("v1", "Secret", "ns", "b"));
        assertEquals(1, before.size(), "snapshot taken earlier should not reflect later inserts");
        assertEquals(2, c.size());
    }
}
