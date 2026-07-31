package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private fun field(path: String, type: String = "string", presence: Double = 1.0) =
    SchemaField(path, presence, mapOf(type to 1.0), 1)

private fun index(name: String, field: String, dir: String = "1") =
    IndexInfo(name, listOf(IndexKey(field, dir)), false, false, false, null, null)

class DiffCollectionsTest {
    @Test
    fun `classifies presence across both sides`() {
        val diff = diffCollections(
            left = mapOf("app.users" to 10L, "app.orders" to 5L),
            right = mapOf("app.users" to 10L, "app.audit" to 3L),
        )
        val byNs = diff.associateBy { "${it.db}.${it.coll}" }
        assertEquals(DiffSide.both, byNs["app.users"]!!.side)
        assertEquals(DiffSide.onlyLeft, byNs["app.orders"]!!.side)
        assertEquals(DiffSide.onlyRight, byNs["app.audit"]!!.side)
    }

    @Test
    fun `flags a count mismatch on a shared collection`() {
        val diff = diffCollections(mapOf("app.users" to 10L), mapOf("app.users" to 12L))
        assertFalse(diff.single().countMatches)
    }

    @Test
    fun `equal counts match`() {
        val diff = diffCollections(mapOf("app.users" to 10L), mapOf("app.users" to 10L))
        assertTrue(diff.single().countMatches)
    }

    @Test
    fun `output is sorted by namespace`() {
        val diff = diffCollections(mapOf("b.z" to 1L, "a.a" to 1L), emptyMap())
        assertEquals(listOf("a.a", "b.z"), diff.map { "${it.db}.${it.coll}" })
    }
}

class DiffFieldsTest {
    @Test
    fun `detects a dominant-type mismatch`() {
        val diff = diffFields(listOf(field("age", "number")), listOf(field("age", "string")))
        assertTrue(diff.single().typeMismatch)
    }

    @Test
    fun `matching type is not a mismatch`() {
        val diff = diffFields(listOf(field("age", "number")), listOf(field("age", "number")))
        assertFalse(diff.single().typeMismatch)
    }

    @Test
    fun `missing fields are classified by side`() {
        val diff = diffFields(listOf(field("a"), field("b")), listOf(field("a"))).associateBy { it.path }
        assertEquals(DiffSide.both, diff["a"]!!.side)
        assertEquals(DiffSide.onlyLeft, diff["b"]!!.side)
    }

    @Test
    fun `differences sort ahead of in-sync fields`() {
        val diff = diffFields(
            left = listOf(field("aaa_common"), field("zzz_only_left")),
            right = listOf(field("aaa_common")),
        )
        // zzz_only_left is a difference and must appear before the in-sync aaa_common.
        assertEquals("zzz_only_left", diff.first().path)
    }
}

class DiffIndexesTest {
    @Test
    fun `classifies index presence`() {
        val diff = diffIndexes(
            left = listOf(index("_id_", "_id"), index("email_1", "email")),
            right = listOf(index("_id_", "_id")),
        ).associateBy { it.name }
        assertEquals(DiffSide.both, diff["_id_"]!!.side)
        assertEquals(DiffSide.onlyLeft, diff["email_1"]!!.side)
    }

    @Test
    fun `only-one-side indexes sort first`() {
        val diff = diffIndexes(
            left = listOf(index("_id_", "_id"), index("z_only", "z")),
            right = listOf(index("_id_", "_id")),
        )
        assertEquals("z_only", diff.first().name)
    }
}

class NamespaceComparisonTest {
    @Test
    fun `in-sync when everything matches`() {
        val c = NamespaceComparison(
            fields = diffFields(listOf(field("a")), listOf(field("a"))),
            indexes = diffIndexes(listOf(index("_id_", "_id")), listOf(index("_id_", "_id"))),
            leftCount = 5,
            rightCount = 5,
        )
        assertTrue(c.countsMatch && c.fieldsInSync && c.indexesInSync)
    }

    @Test
    fun `a type mismatch breaks field sync`() {
        val c = NamespaceComparison(
            fields = diffFields(listOf(field("a", "number")), listOf(field("a", "string"))),
            indexes = emptyList(),
            leftCount = 5,
            rightCount = 5,
        )
        assertFalse(c.fieldsInSync)
    }
}
