package com.kubrik.mex.migration.engine;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for the rename-map pipeline rewrite used by {@link ViewCreator}. Each case
 *  feeds a synthetic aggregation stage through {@link ViewCreator#rewriteForTest} and
 *  asserts the translation without spinning up a live MongoService. */
class ViewCreatorTest {

    @Test
    void lookup_from_is_rewritten() {
        Document stage = new Document("$lookup", new Document()
                .append("from", "orders")
                .append("as", "orderDocs")
                .append("localField", "userId")
                .append("foreignField", "_id"));

        Document rewritten = ViewCreator.rewriteForTest(stage,
                Map.of("orders", "orders_v2"));
        Document lookup = (Document) rewritten.get("$lookup");
        assertEquals("orders_v2", lookup.getString("from"));
        assertEquals("orderDocs", lookup.getString("as"));
    }

    @Test
    void graphLookup_from_is_rewritten() {
        Document stage = new Document("$graphLookup", new Document()
                .append("from", "employees")
                .append("startWith", "$reportsTo")
                .append("connectFromField", "reportsTo")
                .append("connectToField", "name"));

        Document rewritten = ViewCreator.rewriteForTest(stage,
                Map.of("employees", "employees_prod"));
        Document gl = (Document) rewritten.get("$graphLookup");
        assertEquals("employees_prod", gl.getString("from"));
    }

    @Test
    void unionWith_coll_is_rewritten() {
        Document stage = new Document("$unionWith", new Document("coll", "archive"));
        Document rewritten = ViewCreator.rewriteForTest(stage,
                Map.of("archive", "archive_v2"));
        Document uw = (Document) rewritten.get("$unionWith");
        assertEquals("archive_v2", uw.getString("coll"));
    }

    @Test
    void nested_pipeline_inside_lookup_is_rewritten_recursively() {
        Document stage = new Document("$lookup", new Document()
                .append("from", "orders")
                .append("pipeline", List.of(
                        new Document("$match", new Document("status", "paid")),
                        new Document("$lookup", new Document()
                                .append("from", "users")
                                .append("as", "u"))))
                .append("as", "enriched"));

        Document rewritten = ViewCreator.rewriteForTest(stage,
                Map.of("orders", "orders_v2", "users", "users_v2"));
        Document lookup = (Document) rewritten.get("$lookup");
        assertEquals("orders_v2", lookup.getString("from"));

        @SuppressWarnings("unchecked")
        List<Document> nested = (List<Document>) lookup.get("pipeline");
        Document innerLookup = (Document) nested.get(1).get("$lookup");
        assertEquals("users_v2", innerLookup.getString("from"));
    }

    @Test
    void unrenamed_collections_are_left_untouched() {
        Document stage = new Document("$lookup", new Document()
                .append("from", "inventory")
                .append("as", "inv"));
        Document rewritten = ViewCreator.rewriteForTest(stage,
                Map.of("orders", "orders_v2"));
        Document lookup = (Document) rewritten.get("$lookup");
        assertEquals("inventory", lookup.getString("from"),
                "collections not in the rename map should pass through verbatim");
    }

    @Test
    void empty_rename_map_is_identity() {
        Document stage = new Document("$match", new Document("active", true));
        Document rewritten = ViewCreator.rewriteForTest(stage, Map.of());
        assertEquals(stage, rewritten);
    }

    @Test
    void non_collection_fields_named_from_are_not_rewritten_when_not_strings() {
        // Defensive: a numeric "from" (e.g. a custom pipeline stage) shouldn't be corrupted
        // by the rewriter — only string-valued `from` / `coll` on known stages are candidates.
        Document stage = new Document("$custom", new Document("from", 42));
        Document rewritten = ViewCreator.rewriteForTest(stage,
                Map.of("orders", "orders_v2"));
        Document custom = (Document) rewritten.get("$custom");
        assertEquals(42, custom.get("from"));
    }
}
