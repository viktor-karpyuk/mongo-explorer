package io.mex.mongo

import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ParseStageBodyTest {
    @Test
    fun `object bodies parse as documents`() {
        val v = parseStageBody("""{ "status": "active" }""")
        assertTrue(v is Document)
        assertEquals("active", (v as Document)["status"])
    }

    @Test
    fun `scalar bodies parse as bare values`() {
        // 4 of the 10 shipped templates use scalar-bodied stages — these used to throw.
        assertEquals("total", parseStageBody("\"total\""))
        assertEquals(10, parseStageBody("10"))
        assertEquals("\$tags", parseStageBody("\"\$tags\""))
        assertEquals("\$status", parseStageBody("\"\$status\""))
    }

    @Test
    fun `shell syntax expands inside scalar bodies`() {
        val v = parseStageBody("NumberLong(42)")
        assertEquals(42L, v)
    }
}
