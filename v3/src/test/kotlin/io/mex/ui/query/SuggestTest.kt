package io.mex.ui.query

import io.mex.mongo.SchemaField
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun field(path: String, presence: Double = 1.0, type: String = "string", values: List<String> = emptyList()) =
    SchemaField(path, presence, mapOf(type to 1.0), values.size, values)

private val FIELDS = listOf(
    field("status", presence = 1.0, values = listOf("active", "archived", "pending")),
    field("statusChangedAt", presence = 0.4, type = "date"),
    field("name", presence = 0.9),
    field("address.city", presence = 0.7),
    field("createdAt", presence = 0.8, type = "date"),
)

class TokenAtTest {
    @Test
    fun `reads the identifier ending at the caret`() {
        val t = tokenAt("{ stat", 6)
        assertEquals("stat", t.text)
        assertEquals(2, t.start)
    }

    @Test
    fun `includes dotted paths as one token`() {
        assertEquals("address.ci", tokenAt("{ address.ci", 12).text)
    }

    @Test
    fun `is empty between punctuation`() {
        assertEquals("", tokenAt("{ ", 2).text)
    }

    @Test
    fun `clamps a caret past the end`() {
        assertEquals("abc", tokenAt("abc", 99).text)
    }
}

class SuggestFieldsTest {
    @Test
    fun `suggests matching field paths`() {
        val s = suggestionsFor("{ stat", 6, FIELDS, FieldContext.filter)
        assertEquals(listOf("status", "statusChangedAt"), s.map { it.label })
    }

    @Test
    fun `ranks prefix matches above substring matches`() {
        val s = suggestionsFor("{ name", 6, FIELDS, FieldContext.filter)
        assertEquals("name", s.first().label)
    }

    @Test
    fun `orders by presence when several match`() {
        val s = suggestionsFor("{ ", 2, FIELDS, FieldContext.filter)
        // status (100%) is present in more sampled documents than name (90%).
        assertTrue(s.indexOfFirst { it.label == "status" } < s.indexOfFirst { it.label == "name" })
    }

    @Test
    fun `shows type and presence as the hint`() {
        val s = suggestionsFor("{ created", 9, FIELDS, FieldContext.filter)
        assertEquals("date · 80%", s.first().detail)
    }

    @Test
    fun `context decides what is inserted`() {
        fun insertFor(c: FieldContext) = suggestionsFor("{ status", 8, FIELDS, c).first().insert
        assertEquals("\"status\": ", insertFor(FieldContext.filter))
        assertEquals("\"status\": 1", insertFor(FieldContext.projection))
        assertEquals("\"status\": -1", insertFor(FieldContext.sort))
    }

    @Test
    fun `returns nothing without a schema`() {
        assertTrue(suggestionsFor("{ stat", 6, emptyList(), FieldContext.filter).isEmpty())
    }
}

class SuggestOperatorsTest {
    @Test
    fun `completes operators after a dollar`() {
        val s = suggestionsFor("{ status: { \$g", 14, FIELDS, FieldContext.filter)
        assertEquals(listOf("\$gt", "\$gte"), s.map { it.label })
        assertTrue(s.all { it.kind == SuggestKind.operator })
    }

    @Test
    fun `a bare dollar offers the whole operator set`() {
        val s = suggestionsFor("{ \$", 3, FIELDS, FieldContext.filter)
        assertTrue(s.isNotEmpty())
        assertTrue(s.all { it.kind == SuggestKind.operator })
    }
}

class SuggestValuesTest {
    @Test
    fun `suggests sampled values in value position`() {
        val text = "{ \"status\": "
        val s = suggestionsFor(text, text.length, FIELDS, FieldContext.filter)
        assertEquals(listOf("active", "archived", "pending"), s.map { it.label })
        assertTrue(s.all { it.kind == SuggestKind.value })
    }

    @Test
    fun `quotes string values on insert`() {
        val text = "{ \"status\": act"
        val s = suggestionsFor(text, text.length, FIELDS, FieldContext.filter)
        assertEquals("\"active\"", s.first().insert)
    }

    @Test
    fun `offers nothing for a field with no sampled values`() {
        val text = "{ \"name\": "
        assertTrue(suggestionsFor(text, text.length, FIELDS, FieldContext.filter).isEmpty())
    }

    @Test
    fun `detects the field that owns the value position`() {
        val text = "{ \"status\": "
        assertTrue(valuePosition(text, text.length))
        assertEquals("status", fieldBeforeValue(text, text.length))
    }
}

class ApplySuggestionTest {
    @Test
    fun `replaces the token under the caret`() {
        val s = suggestionsFor("{ stat", 6, FIELDS, FieldContext.filter).first()
        val (text, caret) = applySuggestion("{ stat", 6, s)
        assertEquals("{ \"status\": ", text)
        assertEquals(text.length, caret)
    }

    @Test
    fun `does not double up existing quotes`() {
        val s = suggestionsFor("{ \"stat\"", 7, FIELDS, FieldContext.filter).first()
        val (text, _) = applySuggestion("{ \"stat\"", 7, s)
        assertEquals("{ \"status\": ", text)
    }

    @Test
    fun `keeps text that follows the token`() {
        val s = suggestionsFor("{ stat, x: 1 }", 6, FIELDS, FieldContext.filter).first()
        val (text, _) = applySuggestion("{ stat, x: 1 }", 6, s)
        assertEquals("{ \"status\": , x: 1 }", text)
    }
}
