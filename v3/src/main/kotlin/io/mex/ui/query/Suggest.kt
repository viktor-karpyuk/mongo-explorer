package io.mex.ui.query

import io.mex.mongo.SchemaField

/** What the caret is sitting on, and therefore what should be offered. */
enum class SuggestKind { field, operator, value }

data class Suggestion(
    val insert: String,
    val label: String,
    val detail: String,
    val kind: SuggestKind,
)

/** The span of text the caret is editing, replaced wholesale when a suggestion is accepted. */
data class TokenSpan(val start: Int, val end: Int, val text: String)

/** How the accepted suggestion is written back — filters want `"f": `, sort wants `"f": -1`. */
enum class FieldContext(val fieldSuffix: String) {
    filter(": "),
    projection(": 1"),
    sort(": -1"),
}

/** Query operators worth completing after a `$`. */
val QUERY_OPERATORS = listOf(
    "\$eq" to "equals",
    "\$ne" to "not equal",
    "\$gt" to "greater than",
    "\$gte" to "greater than or equal",
    "\$lt" to "less than",
    "\$lte" to "less than or equal",
    "\$in" to "matches any value in an array",
    "\$nin" to "matches none of the values",
    "\$exists" to "field is present",
    "\$type" to "field has BSON type",
    "\$regex" to "matches a regular expression",
    "\$size" to "array has length",
    "\$all" to "array contains all values",
    "\$elemMatch" to "array element matches a sub-query",
    "\$and" to "all conditions match",
    "\$or" to "any condition matches",
    "\$nor" to "no condition matches",
    "\$not" to "inverts a condition",
    "\$expr" to "uses aggregation expressions",
    "\$mod" to "modulo of a numeric field",
)

private fun isTokenChar(c: Char) = c.isLetterOrDigit() || c == '_' || c == '.' || c == '$'

/**
 * The identifier-ish run of characters ending at [caret]. Surrounding quotes are excluded
 * from the span so accepting a suggestion doesn't produce `""status""`.
 */
fun tokenAt(text: String, caret: Int): TokenSpan {
    val safeCaret = caret.coerceIn(0, text.length)
    var start = safeCaret
    while (start > 0 && isTokenChar(text[start - 1])) start--
    var end = safeCaret
    while (end < text.length && isTokenChar(text[end])) end++
    return TokenSpan(start, end, text.substring(start, end))
}

/**
 * True when the caret sits in value position — i.e. the nearest non-space character
 * before the token is a `:`. Used to switch from field names to sampled values.
 */
internal fun valuePosition(text: String, tokenStart: Int): Boolean {
    var i = tokenStart - 1
    while (i >= 0 && text[i].isWhitespace()) i--
    if (i < 0) return false
    if (text[i] != ':') return false
    return true
}

/** The field name immediately left of the `:` that put the caret in value position. */
internal fun fieldBeforeValue(text: String, tokenStart: Int): String? {
    var i = tokenStart - 1
    while (i >= 0 && text[i].isWhitespace()) i--
    if (i < 0 || text[i] != ':') return null
    i--
    while (i >= 0 && text[i].isWhitespace()) i--
    if (i >= 0 && (text[i] == '"' || text[i] == '\'')) i--
    var end = i + 1
    while (i >= 0 && isTokenChar(text[i])) i--
    val name = text.substring(i + 1, end)
    return name.ifBlank { null }
}

/**
 * Ranks candidates for the token under the caret.
 *
 * Field paths come from the sampled schema, so the user does not have to remember exact
 * spelling; a typo used to just produce an empty result set with no explanation.
 */
fun suggestionsFor(
    text: String,
    caret: Int,
    fields: List<SchemaField>,
    context: FieldContext,
    limit: Int = 12,
): List<Suggestion> {
    val token = tokenAt(text, caret)
    val q = token.text

    // `$` anywhere in the token means the user is reaching for an operator.
    if (q.startsWith("$")) {
        val needle = q.removePrefix("$").lowercase()
        return QUERY_OPERATORS
            .filter { needle.isEmpty() || it.first.removePrefix("$").lowercase().startsWith(needle) }
            .take(limit)
            .map { (op, desc) -> Suggestion(insert = "\"$op\": ", label = op, detail = desc, kind = SuggestKind.operator) }
    }

    if (valuePosition(text, token.start)) {
        val field = fieldBeforeValue(text, token.start) ?: return emptyList()
        val schema = fields.firstOrNull { it.path == field } ?: return emptyList()
        if (schema.sampleValues.isEmpty()) return emptyList()
        val needle = q.trim('"').lowercase()
        val quoted = schema.dominantType == "string"
        return schema.sampleValues
            .filter { needle.isEmpty() || it.lowercase().contains(needle) }
            .take(limit)
            .map {
                Suggestion(
                    insert = if (quoted) "\"$it\"" else it,
                    label = it,
                    detail = schema.dominantType,
                    kind = SuggestKind.value,
                )
            }
    }

    if (fields.isEmpty()) return emptyList()
    val needle = q.trim('"').lowercase()
    return fields
        .asSequence()
        .filter { needle.isEmpty() || it.path.lowercase().contains(needle) }
        // Prefix matches first, then the fields present in most documents.
        .sortedWith(
            compareByDescending<SchemaField> { it.path.lowercase().startsWith(needle) }
                .thenByDescending { it.presence }
                .thenBy { it.path.length },
        )
        .take(limit)
        .map {
            Suggestion(
                insert = "\"${it.path}\"${context.fieldSuffix}",
                label = it.path,
                detail = "${it.dominantType} · ${(it.presence * 100).toInt()}%",
                kind = SuggestKind.field,
            )
        }
        .toList()
}

/** Applies [suggestion] over the token under [caret], returning the new text and caret. */
fun applySuggestion(text: String, caret: Int, suggestion: Suggestion): Pair<String, Int> {
    val token = tokenAt(text, caret)
    // Swallow a quote on either side of the token so we don't double them up.
    var start = token.start
    var end = token.end
    if (start > 0 && (text[start - 1] == '"' || text[start - 1] == '\'')) start--
    if (end < text.length && (text[end] == '"' || text[end] == '\'')) end++
    val updated = text.substring(0, start) + suggestion.insert + text.substring(end)
    return updated to (start + suggestion.insert.length)
}
