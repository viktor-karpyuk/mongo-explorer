package com.kubrik.mex.maint.schema;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StarterTemplatesTest {

    @Test
    void four_templates_listed_in_insertion_order() {
        var names = StarterTemplates.all().stream()
                .map(StarterTemplates.Template::name).toList();
        assertEquals(java.util.List.of(
                "empty", "doc-with-required-id",
                "doc-with-enum-status", "doc-with-typed-timestamps"), names);
    }

    @Test
    void every_template_is_valid_json() {
        for (var t : StarterTemplates.all()) {
            // Document.parse chokes on malformed JSON — happy path is
            // enough coverage; tampered copies are the user's problem.
            Document d = Document.parse(t.json());
            assertTrue(d.containsKey("$jsonSchema"),
                    t.name() + " must produce a $jsonSchema object");
        }
    }

    @Test
    void byName_rejects_unknown_template() {
        assertThrows(IllegalArgumentException.class,
                () -> StarterTemplates.byName("no-such"));
    }
}
